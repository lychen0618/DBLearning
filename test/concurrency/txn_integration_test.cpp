#include <fmt/format.h>
#include <fmt/ranges.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <atomic>
#include <chrono>  // NOLINT
#include <cstdio>
#include <memory>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>  //NOLINT
#include <utility>
#include <vector>

#include "common_checker.h"  // NOLINT

namespace bustub {

void CommitTest1() {
  // should scan changes of committed txn
  auto db = GetDbForCommitAbortTest("CommitTest1");
  auto txn1 = Begin(*db, IsolationLevel::READ_UNCOMMITTED);
  Insert(txn1, *db, 1);
  Commit(*db, txn1);
  auto txn2 = Begin(*db, IsolationLevel::READ_UNCOMMITTED);
  Scan(txn2, *db, {1, 233, 234});
  Commit(*db, txn2);
}

// NOLINTNEXTLINE
TEST(CommitAbortTest, CommitTestA) { CommitTest1(); }

void Test1(IsolationLevel lvl) {
  // should scan changes of committed txn
  auto db = GetDbForVisibilityTest("Test1");
  auto txn1 = Begin(*db, lvl);
  Delete(txn1, *db, 233);
  Commit(*db, txn1);
  auto txn2 = Begin(*db, lvl);
  Scan(txn2, *db, {234});
  Commit(*db, txn2);
}

void Test2(IsolationLevel lvl) {
  // should scan changes of committed txn
  auto db = GetDbForVisibilityTest("Test2");
  auto txn1 = Begin(*db, lvl);
  Delete(txn1, *db, 233);
  Scan(txn1, *db, {234});
  Commit(*db, txn1);
  auto txn2 = Begin(*db, lvl);
  Scan(txn2, *db, {234});
  Commit(*db, txn2);
}

// NOLINTNEXTLINE
TEST(VisibilityTest, TestA) {
  // only this one will be public :)
  Test1(IsolationLevel::READ_COMMITTED);
  Test1(IsolationLevel::READ_UNCOMMITTED);
  Test2(IsolationLevel::READ_COMMITTED);
}

void Test5(IsolationLevel lvl1, IsolationLevel lvl2) {
  // should scan changes of committed txn
  auto db = GetDbForVisibilityTest("Test5");
  auto txn1 = Begin(*db, lvl1);
  Delete(txn1, *db, 233);
  auto txn2 = Begin(*db, lvl2);
  Scan(txn2, *db, {234});
  Commit(*db, txn2);
  Commit(*db, txn1);
}

void Test6(IsolationLevel lvl) {
  // should scan changes of committed txn
  auto db = GetDbForVisibilityTest("Test6");
  auto txn1 = Begin(*db, lvl);
  Insert(txn1, *db, 1);
  Delete(txn1, *db, 1);
  Scan(txn1, *db, {233, 234});
  Commit(*db, txn1);
}

// NOLINTNEXTLINE
TEST(VisibilityTest, TestC) {
  // only this one will be public :)
  Test5(IsolationLevel::READ_COMMITTED, IsolationLevel::READ_UNCOMMITTED);
  Test6(IsolationLevel::READ_COMMITTED);
}

void AbortTest1() {
  auto db = GetDbForCommitAbortTest("AbortTest1");
  auto txn1 = Begin(*db, IsolationLevel::READ_UNCOMMITTED);
  Insert(txn1, *db, 1);
  Abort(*db, txn1);
  auto txn2 = Begin(*db, IsolationLevel::READ_UNCOMMITTED);
  Scan(txn2, *db, {233, 234});
  Commit(*db, txn2);
}

// NOLINTNEXTLINE
TEST(CommitAbortTest, AbortTestA) {
  // only this one will be public :)
  AbortTest1();
}

// NOLINTNEXTLINE
TEST(IsolationLevelTest, InsertTestA) {
  ExpectTwoTxn("InsertTestA.1", IsolationLevel::READ_UNCOMMITTED, IsolationLevel::READ_UNCOMMITTED, false, IS_INSERT,
               ExpectedOutcome::DirtyRead);
}

// NOLINTNEXTLINE
TEST(IsolationLevelTest, DeleteTestA) {
  ExpectTwoTxn("DeleteTestA.1", IsolationLevel::READ_COMMITTED, IsolationLevel::READ_UNCOMMITTED, false, IS_DELETE,
               ExpectedOutcome::BlockOnRead);
}

}  // namespace bustub
