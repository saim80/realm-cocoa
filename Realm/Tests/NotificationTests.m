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

#import "RLMTestCase.h"

#import "RLMRealmConfiguration_Private.h"

@interface NotificationTests : RLMTestCase
@property (nonatomic, strong) RLMNotificationToken *token;
@property (nonatomic) bool called;
@end

@implementation NotificationTests
- (void)setUp {
    @autoreleasepool {
        RLMRealm *realm = [RLMRealm defaultRealm];
        [realm transactionWithBlock:^{
            for (int i = 0; i < 10; ++i)
                [IntObject createInDefaultRealmWithValue:@[@(i)]];
        }];
    }

    _token = [self.query addNotificationBlock:^(RLMResults *results, NSError *error) {
        XCTAssertNotNil(results);
        XCTAssertNil(error);
        self.called = true;
        CFRunLoopStop(CFRunLoopGetCurrent());
    }];
    CFRunLoopRun();
}

- (void)tearDown {
    [_token stop];
    [super tearDown];
}

- (RLMResults *)query {
    return [IntObject objectsWhere:@"intCol > 0 AND intCol < 5"];
}

- (void)runAndWaitForNotification:(void (^)(RLMRealm *))block {
    _called = false;
    [self waitForNotification:RLMRealmDidChangeNotification realm:RLMRealm.defaultRealm block:^{
        RLMRealm *realm = [RLMRealm defaultRealm];
        [realm transactionWithBlock:^{
            block(realm);
        }];
    }];
}

- (void)expectNotification:(void (^)(RLMRealm *))block {
    [self runAndWaitForNotification:block];
    XCTAssertTrue(_called);
}

- (void)expectNoNotification:(void (^)(RLMRealm *))block {
    [self runAndWaitForNotification:block];
    XCTAssertFalse(_called);
}

- (void)testInsertObjectMatchingQuery {
    [self expectNotification:^(RLMRealm *realm) {
        [IntObject createInRealm:realm withValue:@[@3]];
    }];
}

- (void)testInsertObjectNotMatchingQuery {
    [self expectNoNotification:^(RLMRealm *realm) {
        [IntObject createInRealm:realm withValue:@[@10]];
    }];
}

- (void)testModifyObjectMatchingQuery {
    [self expectNotification:^(RLMRealm *realm) {
        [[IntObject objectsInRealm:realm where:@"intCol = 3"] setValue:@4 forKey:@"intCol"];
    }];
}

- (void)testModifyObjectToNoLongerMatchQuery {
    [self expectNotification:^(RLMRealm *realm) {
        [[IntObject objectsInRealm:realm where:@"intCol = 3"] setValue:@5 forKey:@"intCol"];
    }];
}

- (void)testModifyObjectNotMatchingQuery {
    [self expectNoNotification:^(RLMRealm *realm) {
        [[IntObject objectsInRealm:realm where:@"intCol = 5"] setValue:@6 forKey:@"intCol"];
    }];
}

- (void)testModifyObjectToMatchQuery {
    [self expectNotification:^(RLMRealm *realm) {
        [[IntObject objectsInRealm:realm where:@"intCol = 5"] setValue:@4 forKey:@"intCol"];
    }];
}

- (void)testDeleteObjectMatchingQuery {
    [self expectNotification:^(RLMRealm *realm) {
        [realm deleteObjects:[IntObject objectsInRealm:realm where:@"intCol = 4"]];
    }];
}

- (void)testDeleteObjectNotMatchingQuery {
    [self expectNoNotification:^(RLMRealm *realm) {
        [realm deleteObjects:[IntObject objectsInRealm:realm where:@"intCol = 5"]];
    }];
    [self expectNoNotification:^(RLMRealm *realm) {
        [realm deleteObjects:[IntObject objectsInRealm:realm where:@"intCol = 0"]];
    }];
}

- (void)testMoveMatchingObjectDueToDeletionOfNonMatchingObject {
    [self expectNotification:^(RLMRealm *realm) {
        // Make a matching object be the last row
        [realm deleteObjects:[IntObject objectsInRealm:realm where:@"intCol >= 5"]];
        // Delete a non-last, non-match row so that a matched row is moved
        [realm deleteObjects:[IntObject objectsInRealm:realm where:@"intCol = 0"]];
    }];
}

- (void)testNonMatchingObjectMovedToIndexOfMatchingRowAndMadeMatching {
    [self expectNotification:^(RLMRealm *realm) {
        // Make the last object match the query
        [[[IntObject allObjectsInRealm:realm] lastObject] setIntCol:3];
        // Move the now-matching object over a previously matching object
        [realm deleteObjects:[IntObject objectsInRealm:realm where:@"intCol = 2"]];
    }];
}
@end

@interface SortedNotificationTests : NotificationTests
@end
@implementation SortedNotificationTests
- (RLMResults *)query {
    return [[IntObject objectsWhere:@"intCol > 0 AND intCol < 5"] sortedResultsUsingProperty:@"intCol" ascending:NO];
}

- (void)testMoveMatchingObjectDueToDeletionOfNonMatchingObject {
    [self expectNoNotification:^(RLMRealm *realm) {
        // Make a matching object be the last row
        [realm deleteObjects:[IntObject objectsInRealm:realm where:@"intCol >= 5"]];
        // Delete a non-last, non-match row so that a matched row is moved
        [realm deleteObjects:[IntObject objectsInRealm:realm where:@"intCol = 0"]];
    }];
}

- (void)testMultipleMovesOfSingleRow {
    [self expectNotification:^(RLMRealm *realm) {
        [realm deleteObjects:[IntObject allObjectsInRealm:realm]];
        [IntObject createInRealm:realm withValue:@[@10]];
        [IntObject createInRealm:realm withValue:@[@10]];
        [IntObject createInRealm:realm withValue:@[@3]];
    }];

    [self expectNoNotification:^(RLMRealm *realm) {
        RLMResults *objects = [IntObject allObjectsInRealm:realm];
        [realm deleteObject:objects[1]];
        [realm deleteObject:objects[0]];
    }];
}
@end

@interface FilteredNotificationTests : RLMTestCase
@property (nonatomic, strong) RLMNotificationToken *token;
@property (nonatomic) bool called;
@end

@implementation FilteredNotificationTests
- (void)setUp {
    @autoreleasepool {
        RLMRealm *realm = [RLMRealm defaultRealm];
        [realm transactionWithBlock:^{
            for (int i = 0; i < 10; ++i) {
                IntObject *io = [IntObject createInDefaultRealmWithValue:@[@(i)]];
                [ArrayPropertyObject createInDefaultRealmWithValue:@[@"", @[], @[io]]];
            }
        }];
    }

    _token = [self.query addNotificationBlockWatchingKeypaths:@[@"intArray"] changes:^(RLMResults *results,
                                                                                       NSArray<RLMObjectChange *> *changes,
                                                                                       NSError *error) {
        XCTAssertNotNil(results);
        XCTAssertNil(error);
        self.called = true;
        CFRunLoopStop(CFRunLoopGetCurrent());
    }];
    CFRunLoopRun();
}

- (void)tearDown {
    [_token stop];
    [super tearDown];
}

- (RLMResults *)query {
    return [ArrayPropertyObject objectsWhere:@"ANY intArray.intCol > 0 AND ANY intArray.intCol < 5"];
}

- (void)runAndWaitForNotification:(void (^)(RLMRealm *))block {
    _called = false;
    [self waitForNotification:RLMRealmDidChangeNotification realm:RLMRealm.defaultRealm block:^{
        RLMRealm *realm = [RLMRealm defaultRealm];
        [realm transactionWithBlock:^{
            block(realm);
        }];
    }];
}

- (void)expectNotification:(void (^)(RLMRealm *))block {
    [self runAndWaitForNotification:block];
    XCTAssertTrue(_called);
}

- (void)expectNoNotification:(void (^)(RLMRealm *))block {
    [self runAndWaitForNotification:block];
    XCTAssertFalse(_called);
}

- (void)testInsertObjectMatchingQuery {
    [self expectNotification:^(RLMRealm *realm) {
        [ArrayPropertyObject createInRealm:realm withValue:@[@"", @[], [IntObject objectsInRealm:realm where:@"intCol = 3"]]];
    }];
}

- (void)testInsertObjectNotMatchingQuery {
    [self expectNoNotification:^(RLMRealm *realm) {
        [ArrayPropertyObject createInRealm:realm withValue:@[@"", @[], [IntObject objectsInRealm:realm where:@"intCol = 6"]]];
    }];
}

- (void)testModifyObjectMatchingQuery {
    [self expectNotification:^(RLMRealm *realm) {
        [[IntObject objectsInRealm:realm where:@"intCol = 3"] setValue:@4 forKey:@"intCol"];
    }];
}

- (void)testModifyObjectToNoLongerMatchQuery {
    [self expectNotification:^(RLMRealm *realm) {
        [[IntObject objectsInRealm:realm where:@"intCol = 3"] setValue:@5 forKey:@"intCol"];
    }];
}

- (void)testModifyObjectNotMatchingQuery {
    [self expectNoNotification:^(RLMRealm *realm) {
        [[IntObject objectsInRealm:realm where:@"intCol = 5"] setValue:@6 forKey:@"intCol"];
    }];
}

- (void)testModifyObjectToMatchQuery {
    [self expectNotification:^(RLMRealm *realm) {
        [[IntObject objectsInRealm:realm where:@"intCol = 5"] setValue:@4 forKey:@"intCol"];
    }];
}

- (void)testDeleteObjectMatchingQuery {
    [self expectNotification:^(RLMRealm *realm) {
        [realm deleteObjects:[IntObject objectsInRealm:realm where:@"intCol = 4"]];
    }];
}

- (void)testDeleteObjectNotMatchingQuery {
    [self expectNoNotification:^(RLMRealm *realm) {
        [realm deleteObjects:[IntObject objectsInRealm:realm where:@"intCol = 5"]];
    }];
    [self expectNoNotification:^(RLMRealm *realm) {
        [realm deleteObjects:[IntObject objectsInRealm:realm where:@"intCol = 0"]];
    }];
}

- (void)testNonMatchingObjectMovedToIndexOfMatchingRowAndMadeMatching {
    [self expectNotification:^(RLMRealm *realm) {
        // Make the last object match the query
        [[[IntObject allObjectsInRealm:realm] lastObject] setIntCol:3];
        // Move the now-matching object over a previously matching object
        [realm deleteObjects:[IntObject objectsInRealm:realm where:@"intCol = 2"]];
    }];
}
@end

@interface ChangesetTests : RLMTestCase
@property (nonatomic, strong) RLMNotificationToken *token;
@property (nonatomic, strong) NSArray<RLMObjectChange *> *changes;
@end

@implementation ChangesetTests
- (void)stuff:(void (^)(RLMRealm *))block {
    @autoreleasepool {
        RLMRealm *realm = [RLMRealm defaultRealm];
        [realm transactionWithBlock:^{
            for (int i = 0; i < 10; ++i) {
                IntObject *io = [IntObject createInDefaultRealmWithValue:@[@(i)]];
                [ArrayPropertyObject createInDefaultRealmWithValue:@[@"", @[], @[io]]];
            }
        }];
    }

    _token = [self.query addNotificationBlockWatchingKeypaths:@[] changes:^(RLMResults *results,
                                                                            NSArray<RLMObjectChange *> *changes,
                                                                            NSError *error) {
        XCTAssertNotNil(results);
        XCTAssertNil(error);
        _changes = changes;
        CFRunLoopStop(CFRunLoopGetCurrent());
    }];
    CFRunLoopRun();

    [self waitForNotification:RLMRealmDidChangeNotification realm:RLMRealm.defaultRealm block:^{
        RLMRealm *realm = [RLMRealm defaultRealm];
        [realm transactionWithBlock:^{
            block(realm);
        }];
    }];

    [_token stop];
}

- (RLMResults *)query {
    return [IntObject objectsWhere:@"intCol > 0 AND intCol < 5"];
}

- (void)testDelete {
    [self stuff:^(RLMRealm *realm) {
        [realm deleteObjects:[IntObject objectsInRealm:realm where:@"intCol = 2"]];
    }];
    XCTAssertEqual(1U, self.changes.count);
    XCTAssertEqual(1U, self.changes[0].oldIndex);
    XCTAssertEqual(NSNotFound, self.changes[0].newIndex);
}

- (void)testInsert {
    [self stuff:^(RLMRealm *realm) {
        [IntObject createInRealm:realm withValue:@[@3]];
    }];
    XCTAssertEqual(1U, self.changes.count);
    XCTAssertEqual(NSNotFound, self.changes[0].oldIndex);
    XCTAssertEqual(4U, self.changes[0].newIndex);
}

@end
