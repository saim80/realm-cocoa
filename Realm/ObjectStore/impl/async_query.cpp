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

static void map_moves(size_t& idx, ChangeInfo const& changes)
{
    auto it = changes.moves.find(idx);
    if (it != changes.moves.end())
        idx = it->second;
}

static bool check_path(TableRef table, size_t idx, std::vector<size_t> const& path, size_t path_ndx, std::vector<ChangeInfo> const& modified)
{
    if (path_ndx >= path.size())
        return false;
    if (table->get_index_in_group() >= modified.size() && path_ndx + 1 == path.size())
        return false;

    auto col = path[path_ndx];
    auto target = table->get_link_target(col);

    if (table->get_column_type(col) == type_Link) {
        auto dst = table->get_link(col, idx);
        if (target->get_index_in_group() < modified.size()) {
            auto const& changes = modified[target->get_index_in_group()];
            map_moves(dst, changes);
            if (changes.changed.count(dst))
                return true;
        }
        return check_path(target, dst, path, path_ndx + 1, modified);
    }
    REALM_ASSERT(table->get_column_type(col) == type_LinkList);

    auto lvr = table->get_linklist(col, idx);
    if (target->get_index_in_group() < modified.size()) {
        auto const& changes = modified[target->get_index_in_group()];
        for (size_t i = 0; i < lvr->size(); ++i) {
            size_t dst = lvr->get(i).get_index();
            map_moves(dst, changes);
            if (changes.changed.count(dst))
                return true;
            if (check_path(target, dst, path, path_ndx + 1, modified))
                return true;
        }
    }
    else {
        for (size_t i = 0; i < lvr->size(); ++i) {
            size_t dst = lvr->get(i).get_index();
            if (check_path(target, dst, path, path_ndx + 1, modified))
                return true;
        }
    }

    return false;
}

bool AsyncQuery::results_did_change(size_t table_ndx, std::vector<ChangeInfo> const& modified_rows) const noexcept
{
    if (!m_initial_run_complete)
        return true;
    if (m_tv.size() != m_handed_over_rows.size())
        return true;

    if (table_ndx < modified_rows.size()) {
        auto const& changes = modified_rows[table_ndx];

        for (size_t i = 0; i < m_tv.size(); ++i) {
            auto idx = m_tv[i].get_index();
            map_moves(idx, changes);
            if (m_handed_over_rows[i] != idx)
                return true;
            if (changes.changed.count(idx))
                return true;
        }
    }

    // Check if there are any linked observations at all
    std::set<size_t> watched_tables;
    for (auto const& cb : m_callbacks) {
        for (auto const& colpath : cb.columns_to_watch) {
            auto table = m_query->get_table();
            for (auto col : colpath) {
                auto target = table->get_link_target(col);
                watched_tables.insert(target->get_index_in_group());
                table = target;
            }
        }
    }

    if (watched_tables.empty()) {
        return false;
    }

    // Check if any of the observed linked tables changed
    bool any_watched_changed = false;
    for (auto table_ndx : watched_tables) {
        if (table_ndx >= modified_rows.size())
            continue;
        if (modified_rows[table_ndx].changed.empty())
            continue;
        any_watched_changed = true;
        break;
    }

    if (!any_watched_changed)
        return false;

    std::vector<std::vector<size_t>> paths_to_check;

    for (auto const& cb : m_callbacks) {
        for (auto const& colpath : cb.columns_to_watch) {
            auto table = m_query->get_table();
            for (auto col : colpath) {
                auto target = table->get_link_target(col);
                if (modified_rows.size() > target->get_index_in_group()) {
                    auto const& changes = modified_rows[target->get_index_in_group()];
                    if (!changes.changed.empty()) {
                        paths_to_check.push_back(colpath);
                        goto break2;

                    }
                }
                table = target;
            }
            break2:;
        }
    }

    auto table = m_query->get_table();
    for (auto idx : m_handed_over_rows) {
        for (auto const& path : paths_to_check) {
            if (check_path(table, idx, path, 0, modified_rows))
                return true;
        }
    }

    return false;
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

    if (!results_did_change(table_ndx, modified_rows)) {
        m_tv = TableView();
        return;
    }

    m_handed_over_rows.clear();
    for (size_t i = 0; i < m_tv.size(); ++i)
        m_handed_over_rows.push_back(m_tv[i].get_index());
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
        fn({}, m_error);
    }

    if (m_error) {
        // Remove all the callbacks as we never need to call anything ever again
        // after delivering an error
        std::lock_guard<std::mutex> callback_lock(m_callback_mutex);
        m_callbacks.clear();
    }
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
