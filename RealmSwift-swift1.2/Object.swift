////////////////////////////////////////////////////////////////////////////
//
// Copyright 2014 Realm Inc.
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

import Foundation
import Realm
import Realm.Private

/**
In Realm you define your model classes by subclassing `Object` and adding properties to be persisted.
You then instantiate and use your custom subclasses instead of using the Object class directly.

```swift
class Dog: Object {
    dynamic var name: String = ""
    dynamic var adopted: Bool = false
    let siblings = List<Dog>()
}
```

### Supported property types

- `String`, `NSString`
- `Int`
- `Int8`, `Int16`, `Int32`, `Int64`
- `Float`
- `Double`
- `Bool`
- `NSDate`
- `NSData`
- `RealmOptional<T>` for optional numeric properties
- `Object` subclasses for to-one relationships
- `List<T: Object>` for to-many relationships

`String`, `NSString`, `NSDate`, `NSData` and `Object` subclass properties can be
optional. `Int`, `Int8`, Int16`, Int32`, `Int64`, `Float`, `Double`, `Bool` 
and `List` properties cannot. To store an optional number, instead use 
`RealmOptional<Int>`, `RealmOptional<Float>`, `RealmOptional<Double>`, or 
`RealmOptional<Bool>` instead, which wraps an optional value of the generic type.

All property types except for `List` and `RealmOptional` *must* be declared as
`dynamic var`. `List` and `RealmOptional` properties must be declared as
non-dynamic `let` properties.

### Querying

You can gets `Results` of an Object subclass via the `objects(_:)` instance method on `Realm`.

### Relationships

See our [Cocoa guide](http://realm.io/docs/cocoa) for more details.
*/
@objc(RealmSwiftObject)
public class Object: RLMObjectBase, Equatable, Printable {

    // MARK: Initializers

    /**
    Initialize a standalone (unpersisted) `Object`.
    Call `add(_:)` on a `Realm` to add standalone objects to a realm.

    :see: Realm().add(_:)
    */
    public required override init() {
        super.init()
    }

    /**
    Initialize a standalone (unpersisted) `Object` with values from an `Array<AnyObject>` or `Dictionary<String, AnyObject>`.
    Call `add(_:)` on a `Realm` to add standalone objects to a realm.

    :param: value   The value used to populate the object. This can be any key/value coding compliant
                    object, or a JSON object such as those returned from the methods in `NSJSONSerialization`,
                    or an `Array` with one object for each persisted property. An exception will be
                    thrown if any required properties are not present and no default is set.
    */
    public init(value: AnyObject) {
        self.dynamicType.sharedSchema() // ensure this class' objectSchema is loaded in the partialSharedSchema
        super.init(value: value, schema: RLMSchema.partialSharedSchema())
    }


    // MARK: Properties

    /// The `Realm` this object belongs to, or `nil` if the object
    /// does not belong to a realm (the object is standalone).
    public var realm: Realm? {
        if let rlmReam = RLMObjectBaseRealm(self) {
            return Realm(rlmReam)
        }
        return nil
    }

    /// The `ObjectSchema` which lists the persisted properties for this object.
    public var objectSchema: ObjectSchema {
        return ObjectSchema(RLMObjectBaseObjectSchema(self))
    }

    /// Indicates if an object can no longer be accessed.
    ///
    /// An object can no longer be accessed if the object has been deleted from the containing
    /// `realm` or if `invalidate` is called on the containing `realm`.
    public override var invalidated: Bool { return super.invalidated }

    /// Returns a human-readable description of this object.
    public override var description: String { return super.description }

    #if os(OSX)
    /// Helper to return the class name for an Object subclass.
    public final override var className: String { return "" }
    #else
    /// Helper to return the class name for an Object subclass.
    public final var className: String { return "" }
    #endif

    // MARK: Object Customization

    /**
    Override to designate a property as the primary key for an `Object` subclass. Only properties of
    type String and Int can be designated as the primary key. Primary key
    properties enforce uniqueness for each value whenever the property is set which incurs some overhead.
    Indexes are created automatically for primary key properties.

    :returns: Name of the property designated as the primary key, or `nil` if the model has no primary key.
    */
    public class func primaryKey() -> String? { return nil }

    /**
    Override to return an array of property names to ignore. These properties will not be persisted
    and are treated as transient.

    :returns: `Array` of property names to ignore.
    */
    public class func ignoredProperties() -> [String] { return [] }

    /**
    Return an array of property names for properties which should be indexed. Only supported
    for string and int properties.
    :returns: `Array` of property names to index.
    */
    public class func indexedProperties() -> [String] { return [] }


    // MARK: Inverse Relationships

    /**
    Get an `Array` of objects of type `T` which have this object as the given property value. This can
    be used to get the inverse relationship value for `Object` and `List` properties.
    :param: type          The type of object on which the relationship to query is defined.
    :param: propertyName  The name of the property which defines the relationship.
    :returns: An `Array` of objects of type `T` which have this object as their value for the `propertyName` property.
    */
    public func linkingObjects<T: Object>(type: T.Type, forProperty propertyName: String) -> [T] {
        return RLMObjectBaseLinkingObjectsOfClass(self, T.className(), propertyName) as! [T]
    }

    // MARK: Key-Value Coding & Subscripting

    /// Returns or sets the value of the property with the given name.
    public subscript(key: String) -> AnyObject? {
        get {
            if realm == nil {
                return self.valueForKey(key)
            }
            let property = RLMValidatedGetProperty(self, key)
            if property.type == .Array {
                return self.listForProperty(property)
            }
            // No special logic is needed for optional numbers here because the NSNumber returned by RLMDynamicGet
            // is better for callers than the RealmOptional that optionalForProperty would give us.
            return RLMDynamicGet(self, property)
        }
        set(value) {
            if realm == nil {
                self.setValue(value, forKey: key)
            }
            else {
                RLMDynamicValidatedSet(self, key, value)
            }
        }
    }

    // MARK: Dynamic list

    /**
    This method is useful only in specialized circumstances, for example, when building
    components that integrate with Realm. If you are simply building an app on Realm, it is
    recommended to use instance variables or cast the KVC returns.

    Returns a List of DynamicObjects for a property name

    :warning: This method is useful only in specialized circumstances

    :param: propertyName The name of the property to get a List<DynamicObject>

    :returns: A List of DynamicObjects

    :nodoc:
    */
    public func dynamicList(propertyName: String) -> List<DynamicObject> {
        return unsafeBitCast(listForProperty(RLMValidatedGetProperty(self, propertyName)), List<DynamicObject>.self)
    }

    // MARK: Private functions

    // FIXME: None of these functions should be exposed in the public interface.

    /**
    WARNING: This is an internal initializer not intended for public use.
    :nodoc:
    */
    public override init(realm: RLMRealm, schema: RLMObjectSchema) {
        super.init(realm: realm, schema: schema)
    }

    /**
    WARNING: This is an internal initializer not intended for public use.
    :nodoc:
    */
    public override init(value: AnyObject, schema: RLMSchema) {
        super.init(value: value, schema: schema)
    }

    // Helper for getting the list object for a property
    internal func listForProperty(prop: RLMProperty) -> RLMListBase {
        return object_getIvar(self, prop.swiftIvar) as! RLMListBase
    }

    // Helper for getting the optional object for a property
    internal func optionalForProperty(prop: RLMProperty) -> RLMOptionalBase {
        return object_getIvar(self, prop.swiftIvar) as! RLMOptionalBase
    }
}

// MARK: Equatable

/// Returns whether both objects are equal.
/// Objects are considered equal when they are both from the same Realm
/// and point to the same underlying object in the database.
public func == <T: Object>(lhs: T, rhs: T) -> Bool {
    return RLMObjectBaseAreEqual(lhs, rhs)
}

/// Object interface which allows untyped getters and setters for Objects.
/// :nodoc:
public final class DynamicObject: Object {
    private var listProperties = [String: List<DynamicObject>]()
    private var optionalProperties = [String: RLMOptionalBase]()

    // Override to create List<DynamicObject> on access
    internal override func listForProperty(prop: RLMProperty) -> RLMListBase {
        if let list = listProperties[prop.name] {
            return list
        }
        let list = List<DynamicObject>()
        listProperties[prop.name] = list
        return list
    }

    // Override to create RealmOptional on access
    internal override func optionalForProperty(prop: RLMProperty) -> RLMOptionalBase {
        if let optional = optionalProperties[prop.name] {
            return optional
        }
        let optional = RLMOptionalBase()
        optional.property = prop
        optionalProperties[prop.name] = optional
        return optional
    }

    /// :nodoc:
    public override func valueForUndefinedKey(key: String) -> AnyObject? {
        return self[key]
    }

    /// :nodoc:
    public override func setValue(value: AnyObject?, forUndefinedKey key: String) {
        self[key] = value
    }

    /// :nodoc:
    public override class func shouldIncludeInDefaultSchema() -> Bool {
        return false
    }
}

/// :nodoc:
/// Internal class. Do not use directly.
@objc(RealmSwiftObjectUtil)
public class ObjectUtil: NSObject {
    @objc private class func swiftVersion() -> NSString {
        return "1.2"
    }

    @objc private class func ignoredPropertiesForClass(type: AnyClass) -> NSArray? {
        if let type = type as? Object.Type {
            return type.ignoredProperties() as NSArray?
        }
        return nil
    }

    @objc private class func indexedPropertiesForClass(type: AnyClass) -> NSArray? {
        if let type = type as? Object.Type {
            return type.indexedProperties() as NSArray?
        }
        return nil
    }

    // Get the names of all properties in the object which are of type List<>
    @objc private class func getGenericListPropertyNames(obj: AnyObject) -> NSArray {
        let reflection = reflect(obj)

        var properties = [String]()

        // Skip the first property (super):
        // super is an implicit property on Swift objects
        for i in 1..<reflection.count {
            let mirror = reflection[i].1
            if mirror.valueType is RLMListBase.Type {
                properties.append(reflection[i].0)
            }
        }

        return properties
    }

    @objc private class func initializeListProperty(object: RLMObjectBase, property: RLMProperty, array: RLMArray) {
        (object as! Object).listForProperty(property)._rlmArray = array
    }

    @objc private class func initializeOptionalProperty(object: RLMObjectBase, property: RLMProperty) {
        let optional = (object as! Object).optionalForProperty(property)
        optional.property = property
        optional.object = object
    }

    @objc private class func getOptionalProperties(object: AnyObject) -> NSDictionary {
        let reflection = reflect(object)

        var properties = [String:AnyObject]()

        // Skip the first property (super):
        // super is an implicit property on Swift objects
        for i in 1..<reflection.count {
            let mirror = reflection[i].1
            let name = reflection[i].0
            if mirror.valueType is Optional<String>.Type || mirror.valueType is Optional<NSString>.Type {
                properties[name] = Int(PropertyType.String.rawValue)
            } else if mirror.valueType is Optional<NSDate>.Type {
                properties[name] = Int(PropertyType.Date.rawValue)
            } else if mirror.valueType is Optional<NSData>.Type {
                properties[name] = Int(PropertyType.Data.rawValue)
            } else if mirror.valueType is Optional<Object>.Type {
                properties[name] = Int(PropertyType.Object.rawValue)
            } else if mirror.valueType is RealmOptional<Int>.Type ||
                      mirror.valueType is RealmOptional<Int8>.Type ||
                      mirror.valueType is RealmOptional<Int16>.Type ||
                      mirror.valueType is RealmOptional<Int32>.Type ||
                      mirror.valueType is RealmOptional<Int64>.Type {
                properties[name] = Int(PropertyType.Int.rawValue)
            } else if mirror.valueType is RealmOptional<Float>.Type {
                properties[name] = Int(PropertyType.Float.rawValue)
            } else if mirror.valueType is RealmOptional<Double>.Type {
                properties[name] = Int(PropertyType.Double.rawValue)
            } else if mirror.valueType is RealmOptional<Bool>.Type {
                properties[name] = Int(PropertyType.Bool.rawValue)
            } else if mirror.value as? RLMOptionalBase != nil {
                throwRealmException("'\(mirror.valueType)' is not a a valid RealmOptional type.")
            } else if mirror.disposition == .Optional {
                properties[name] = NSNull()
            }
        }

        return properties
    }

    @objc private class func requiredPropertiesForClass(_: AnyClass) -> NSArray? {
        return nil
    }
}
