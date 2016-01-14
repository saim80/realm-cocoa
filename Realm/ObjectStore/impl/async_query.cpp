////////////////////////////////////////////////////////////////////////////
//
// Copyright 2015 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#include "async_query.hpp"

#include "realm_coordinator.hpp"
#include "results.hpp"

using namespace realm;
using namespace realm::_impl;

AsyncQuery::AsyncQuery(Results& target)
: m_target_results(&target)
, m_realm(target.get_realm().shared_from_this())
, m_sort(target.get_sort())
, m_sg_version(Realm::Internal::get_shared_group(*m_realm).get_version_of_current_transaction())
{
    Query q = target.get_query();
    m_query_handover = Realm::Internal::get_shared_group(*m_realm).export_for_handover(q, MutableSourcePayload::Move);
}

AsyncQuery::~AsyncQuery()
{
    // unregister() may have been called from a different thread than we're being
    // destroyed on, so we need to synchronize access to the interesting fields
    // modified there
    std::lock_guard<std::mutex> lock(m_target_mutex);
    m_realm = nullptr;
}

size_t AsyncQuery::next_token()
{
    size_t token = 0;
    for (auto& callback : m_callbacks) {
        if (token <= callback.token) {
            token = callback.token + 1;
        }
    }
    return token;
}

size_t AsyncQuery::add_callback(std::function<void (std::exception_ptr)> callback)
{
    return add_callback({}, [=](std::vector<AsyncQueryChange>, std::exception_ptr error) {
        callback(error);
    });
}

size_t AsyncQuery::add_callback(std::vector<std::vector<size_t>> columns_to_watch,
                                std::function<void (std::vector<AsyncQueryChange>, std::exception_ptr)> callback)
{
    m_realm->verify_thread();

    std::lock_guard<std::mutex> lock(m_callback_mutex);
    auto token = next_token();
    m_callbacks.push_back({std::move(callback), token, -1ULL, std::move(columns_to_watch)});
    if (m_callback_index == npos) { // Don't need to wake up if we're already sending notifications
        Realm::Internal::get_coordinator(*m_realm).send_commit_notifications();
        m_have_callbacks = true;
    }
    return token;
}

void AsyncQuery::remove_callback(size_t token)
{
    Callback old;
    {
        std::lock_guard<std::mutex> lock(m_callback_mutex);
        REALM_ASSERT(m_error || m_callbacks.size() > 0);

        auto it = find_if(begin(m_callbacks), end(m_callbacks),
                          [=](const auto& c) { return c.token == token; });
        // We should only fail to find the callback if it was removed due to an error
        REALM_ASSERT(m_error || it != end(m_callbacks));
        if (it == end(m_callbacks)) {
            return;
        }

        size_t idx = distance(begin(m_callbacks), it);
        if (m_callback_index != npos && m_callback_index >= idx) {
            --m_callback_index;
        }

        old = std::move(*it);
        m_callbacks.erase(it);

        m_have_callbacks = !m_callbacks.empty();
    }
}

void AsyncQuery::unregister() noexcept
{
    std::lock_guard<std::mutex> lock(m_target_mutex);
    m_target_results = nullptr;
    m_realm = nullptr;
}

void AsyncQuery::release_query() noexcept
{
    {
        std::lock_guard<std::mutex> lock(m_target_mutex);
        REALM_ASSERT(!m_realm && !m_target_results);
    }

    m_query = nullptr;
}

bool AsyncQuery::is_alive() const noexcept
{
    std::lock_guard<std::mutex> lock(m_target_mutex);
    return m_target_results != nullptr;
}

// Most of the inter-thread synchronization for run(), prepare_handover(),
// attach_to(), detach(), release_query() and deliver() is done by
// RealmCoordinator external to this code, which has some potentially
// non-obvious results on which members are and are not safe to use without
// holding a lock.
//
// attach_to(), detach(), run(), prepare_handover(), and release_query() are
// all only ever called on a single thread. call_callbacks() and deliver() are
// called on the same thread. Calls to prepare_handover() and deliver() are
// guarded by a lock.
//
// In total, this means that the safe data flow is as follows:
//  - prepare_handover(), attach_to(), detach() and release_query() can read
//    members written by each other
//  - deliver() can read members written to in prepare_handover(), deliver(),
//    and call_callbacks()
//  - call_callbacks() and read members written to in deliver()
//
// Separately from this data flow for the query results, all uses of
// m_target_results, m_callbacks, and m_callback_index must be done with the
// appropriate mutex held to avoid race conditions when the Results object is
// destroyed while the background work is running, and to allow removing
// callbacks from any thread.

static bool map_moves(size_t& idx, ChangeInfo const& changes)
{
    auto it = changes.moves.find(idx);
    if (it != changes.moves.end()) {
        idx = it->second;
        return true;
    }
    return false;
}

static bool row_did_change(Table& table, size_t idx, std::vector<ChangeInfo> const& modified, int depth = 0)
{
    if (depth > 16)  // arbitrary limit
        return false;

    size_t table_ndx = table.get_index_in_group();
    if (table_ndx < modified.size()) {
        auto const& changes = modified[table_ndx];
        map_moves(idx, changes);
        if (changes.changed.count(idx))
            return true;
    }

    for (size_t i = 0, count = table.get_column_count(); i < count; ++i) {
        auto type = table.get_column_type(i);
        if (type == type_Link) {
            auto& target = *table.get_link_target(i);
            auto dst = table.get_link(i, idx);
            return row_did_change(target, dst, modified, depth + 1);
        }
        if (type != type_LinkList)
            continue;

        auto& target = *table.get_link_target(i);
        auto lvr = table.get_linklist(i, idx);
        for (size_t j = 0; j < lvr->size(); ++j) {
            size_t dst = lvr->get(j).get_index();
            if (row_did_change(target, dst, modified, depth + 1))
                return true;
        }
    }

    return false;
}

std::vector<AsyncQueryChange> AsyncQuery::calculate_changes(size_t table_ndx,
                                                            std::vector<ChangeInfo> const& modified_rows,
                                                            bool sort)
{
    auto changes = table_ndx < modified_rows.size() ? &modified_rows[table_ndx] : nullptr;

    auto do_calculate_changes = [&](auto const& old_rows, auto const& new_rows) {
        std::vector<AsyncQueryChange> changeset;
        size_t i = 0, j = 0;
        int shift = 0;
        while (i < old_rows.size() && j < new_rows.size()) {
            auto old_index = old_rows[i];
            auto new_index = new_rows[j];
            if (old_index.first == new_index.first) {
                if (old_index.second != new_index.second + shift)
                    changeset.push_back({old_index.second, new_index.second + shift});
                else if (row_did_change(*m_query->get_table(), old_index.first, modified_rows))
                    changeset.push_back({old_index.second, old_index.second}); // FIXME: not i, j because scheme is dumb
                ++i;
                ++j;
            }
            else if (old_index.first < new_index.first) {
                changeset.push_back({old_index.second, -1ULL});
                ++shift;
                ++i;
            }
            else {
                changeset.push_back({-1ULL, new_index.second});
                --shift;
                ++j;
            }
        }

        for (; i < old_rows.size(); ++i)
            changeset.push_back({old_rows[i].second, -1ULL});
        for (; j < new_rows.size(); ++j)
            changeset.push_back({-1ULL, new_rows[j].second});

        return changeset;
    };

    std::vector<std::pair<size_t, size_t>> old_rows;
    for (size_t i = 0; i < m_previous_rows.size(); ++i) {
        auto ndx = m_previous_rows[i];
        old_rows.push_back({ndx, i});
    }
    std::stable_sort(begin(old_rows), end(old_rows), [](auto& lft, auto& rgt) {
        return lft.first < rgt.first;
    });

    // FIXME: optim: check if there's actually any outgoing links
//    if (changes && (!changes->moves.empty() || !changes->changed.empty())) {
        std::vector<std::pair<size_t, size_t>> new_rows;
        for (size_t i = 0; i < m_tv.size(); ++i) {
            auto ndx = m_tv[i].get_index();
            if (changes)
                map_moves(ndx, *changes);
            new_rows.push_back({ndx, i});
        }
        std::stable_sort(begin(new_rows), end(new_rows), [](auto& lft, auto& rgt) {
            return lft.first < rgt.first;
        });
        return do_calculate_changes(old_rows, new_rows);
//    }
//    else {
//        return do_calculate_changes(old_rows, old_rows);
//    }
}

void AsyncQuery::run(std::vector<ChangeInfo> const& modified_rows)
{
    REALM_ASSERT(m_sg);

    {
        std::lock_guard<std::mutex> target_lock(m_target_mutex);
        // Don't run the query if the results aren't actually going to be used
        if (!m_target_results || (!m_have_callbacks && !m_target_results->wants_background_updates())) {
            return;
        }
    }

    REALM_ASSERT(!m_tv.is_attached());

    size_t table_ndx = m_query->get_table()->get_index_in_group();

    // If we've run previously, check if we need to rerun
//    if (m_initial_run_complete) {
//        if (table_ndx > modified_rows.size())
//            return;
//        auto const& changes = modified_rows[table_ndx];
//        if (changes.changed.empty() && changes.moves.empty())
//            return;
//    }

    m_tv = m_query->find_all();
    if (m_sort) {
        m_tv.sort(m_sort.columnIndices, m_sort.ascending);
    }

    if (m_initial_run_complete) {
        m_new_changes = calculate_changes(table_ndx, modified_rows, (bool)m_sort);
        if (m_new_changes.empty()) {
            m_tv = TableView();
            return;
        }
    }

    m_previous_rows.clear();
    m_previous_rows.resize(m_tv.size());
    for (size_t i = 0; i < m_tv.size(); ++i)
        m_previous_rows[i] = m_tv[i].get_index();
}

void AsyncQuery::prepare_handover()
{
    m_sg_version = m_sg->get_version_of_current_transaction();

    if (!m_tv.is_attached()) {
        return;
    }

    REALM_ASSERT(m_tv.is_in_sync());

    m_initial_run_complete = true;
    m_handed_over_table_version = m_tv.outside_version();
    m_tv_handover = m_sg->export_for_handover(m_tv, MutableSourcePayload::Move);

    // FIXME: this is not actually correct
    m_changes.insert(m_changes.end(), m_new_changes.begin(), m_new_changes.end());
    m_new_changes.clear();

    // detach the TableView as we won't need it again and keeping it around
    // makes advance_read() much more expensive
    m_tv = TableView();
}

bool AsyncQuery::deliver(SharedGroup& sg, std::exception_ptr err)
{
    if (!is_for_current_thread()) {
        return false;
    }

    std::lock_guard<std::mutex> target_lock(m_target_mutex);

    // Target results being null here indicates that it was destroyed while we
    // were in the process of advancing the Realm version and preparing for
    // delivery, i.e. it was destroyed from the "wrong" thread
    if (!m_target_results) {
        return false;
    }

    // We can get called before the query has actually had the chance to run if
    // we're added immediately before a different set of async results are
    // delivered
    if (!m_initial_run_complete && !err) {
        return false;
    }

    if (err) {
        m_error = err;
        return m_have_callbacks;
    }

    REALM_ASSERT(!m_query_handover);

    auto realm_sg_version = Realm::Internal::get_shared_group(*m_realm).get_version_of_current_transaction();
    if (m_sg_version != realm_sg_version) {
        // Realm version can be newer if a commit was made on our thread or the
        // user manually called refresh(), or older if a commit was made on a
        // different thread and we ran *really* fast in between the check for
        // if the shared group has changed and when we pick up async results
        return false;
    }

    if (m_tv_handover) {
        m_tv_handover->version = m_sg_version;
        Results::Internal::set_table_view(*m_target_results,
                                          std::move(*sg.import_from_handover(std::move(m_tv_handover))));
        m_delievered_table_version = m_handed_over_table_version;

    }
    REALM_ASSERT(!m_tv_handover);
    return m_have_callbacks;
}

void AsyncQuery::call_callbacks()
{
    REALM_ASSERT(is_for_current_thread());

    while (auto fn = next_callback()) {
        fn(m_changes, m_error);
    }

    if (m_error) {
        // Remove all the callbacks as we never need to call anything ever again
        // after delivering an error
        std::lock_guard<std::mutex> callback_lock(m_callback_mutex);
        m_callbacks.clear();
    }
    m_changes.clear();
}

std::function<void (std::vector<AsyncQueryChange>, std::exception_ptr)> AsyncQuery::next_callback()
{
    std::lock_guard<std::mutex> callback_lock(m_callback_mutex);
    for (++m_callback_index; m_callback_index < m_callbacks.size(); ++m_callback_index) {
        auto& callback = m_callbacks[m_callback_index];
        if (m_error || callback.delivered_version != m_delievered_table_version) {
            callback.delivered_version = m_delievered_table_version;
            return callback.fn;
        }
    }

    m_callback_index = npos;
    return nullptr;
}

void AsyncQuery::attach_to(realm::SharedGroup& sg)
{
    REALM_ASSERT(!m_sg);
    REALM_ASSERT(m_query_handover);

    m_query = sg.import_from_handover(std::move(m_query_handover));
    m_sg = &sg;
}

void AsyncQuery::detatch()
{
    REALM_ASSERT(m_sg);
    REALM_ASSERT(m_query);
    REALM_ASSERT(!m_tv.is_attached());

    m_query_handover = m_sg->export_for_handover(*m_query, MutableSourcePayload::Move);
    m_sg = nullptr;
    m_query = nullptr;
}
