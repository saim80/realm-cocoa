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

import Realm

/// Types that can be represented in a `RealmOptional`.
public protocol RealmOptionalType {}
extension Int: RealmOptionalType {}
extension Int8: RealmOptionalType {}
extension Int16: RealmOptionalType {}
extension Int32: RealmOptionalType {}
extension Int64: RealmOptionalType {}
extension Float: RealmOptionalType {}
extension Double: RealmOptionalType {}
extension Bool: RealmOptionalType {}

// Not all RealmOptionalType's can be cast to AnyObject, so handle casting logic here.
private func realmOptionalToAnyObject<T: RealmOptionalType>(value: T?) -> AnyObject? {
    if let anyObjectValue: AnyObject = value as? AnyObject {
        return anyObjectValue
    } else if let int8Value = value as? Int8 {
        return NSNumber(long: Int(int8Value))
    } else if let int16Value = value as? Int16 {
        return NSNumber(long: Int(int16Value))
    } else if let int32Value = value as? Int32 {
        return NSNumber(long: Int(int32Value))
    } else if let int64Value = value as? Int64 {
        return NSNumber(longLong: int64Value)
    }
    return nil
}

// Not all RealmOptionalType's can be cast from AnyObject, so handle casting logic here.
private func anyObjectToRealmOptional<T: RealmOptionalType>(anyObject: AnyObject?) -> T? {
    if T.self is Int8.Type {
        return ((anyObject as! NSNumber?)?.longValue).map { Int8($0) } as! T?
    } else if T.self is Int16.Type {
        return ((anyObject as! NSNumber?)?.longValue).map { Int16($0) } as! T?
    } else if T.self is Int32.Type {
        return ((anyObject as! NSNumber?)?.longValue).map { Int32($0) } as! T?
    } else if T.self is Int64.Type {
        return (anyObject as! NSNumber?)?.longLongValue as! T?
    }
    return anyObject as! T?
}

/**
A `RealmOptional` represents a optional value for types that can't be directly
declared as `dynamic` in Swift, such as `Int`s, `Float`, `Double`, and `Bool`.

It encapsulates a value in its `value` property, which is the only way to mutate
a `RealmOptional` property on an `Object`.
*/
public final class RealmOptional<T: RealmOptionalType>: RLMOptionalBase {
    /// The value this optional represents.
    public var value: T? {
        get {
            return anyObjectToRealmOptional(underlyingValue)
        }
        set {
            underlyingValue = realmOptionalToAnyObject(newValue)
        }
    }

    /// Creates a `RealmOptional` with a `nil` default value.
    public override init() {
        super.init()
    }
}
