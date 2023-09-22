//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard_test.cpp
//
// Identification: test/storage/page_guard_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <random>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/page/page_guard.h"

#include "gtest/gtest.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(PageGuardTest, SampleTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  auto guarded_page = BasicPageGuard(bpm.get(), page0);

  EXPECT_EQ(page0->GetData(), guarded_page.GetData());
  EXPECT_EQ(page0->GetPageId(), guarded_page.PageId());
  EXPECT_EQ(1, page0->GetPinCount());

  guarded_page.Drop();

  EXPECT_EQ(0, page0->GetPinCount());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

TEST(PageGuardTest, MoveTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  Page *init_page[6];
  page_id_t page_id_temp;
  for (int i = 0; i < 6; ++i) {
    init_page[i] = bpm->NewPage(&page_id_temp);
  }

  BasicPageGuard basic_guard0(bpm.get(), init_page[0]);
  BasicPageGuard basic_guard1(bpm.get(), init_page[1]);
  basic_guard0 = std::move(basic_guard1);
  EXPECT_EQ(0, init_page[0]->GetPinCount());
  EXPECT_EQ(1, init_page[1]->GetPinCount());
  BasicPageGuard basic_guard2(std::move(basic_guard0));
  EXPECT_EQ(1, init_page[1]->GetPinCount());

  ReadPageGuard read_guard0(bpm.get(), init_page[2]);
  ReadPageGuard read_guard1(bpm.get(), init_page[3]);
  read_guard0 = std::move(read_guard1);
  EXPECT_EQ(0, init_page[2]->GetPinCount());
  EXPECT_EQ(1, init_page[3]->GetPinCount());
  ReadPageGuard read_guard2(std::move(read_guard0));
  EXPECT_EQ(1, init_page[3]->GetPinCount());

  WritePageGuard write_guard0(bpm.get(), init_page[4]);
  WritePageGuard write_guard1(bpm.get(), init_page[5]);

  // Important: here, latch the page id 4 by Page*, outside the watch of WPageGuard, but the WPageGuard still needs to
  // unlatch page 4 in operator= where the page id 4 is on the left side of the operator =.
  // Error log:
  // terminate called after throwing an instance of 'std::system_error'
  //   what():  Resource deadlock avoided
  // 1/1 Test #64: PageGuardTest.MoveTest
  init_page[4]->WLatch();
  write_guard0 = std::move(write_guard1);
  EXPECT_EQ(0, init_page[4]->GetPinCount());
  EXPECT_EQ(1, init_page[5]->GetPinCount());
  init_page[4]->WLatch();  // A deadlock will appear here if the latch is still on after operator= is called.
  init_page[4]->WUnlatch();

  WritePageGuard write_guard2(std::move(write_guard0));
  EXPECT_EQ(1, init_page[5]->GetPinCount());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

TEST(PageGuardTest, HHTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp = 0;
  page_id_t page_id_temp_a;
  auto *page0 = bpm->NewPage(&page_id_temp);
  auto *page1 = bpm->NewPage(&page_id_temp_a);

  auto guarded_page = BasicPageGuard(bpm.get(), page0);
  auto guarded_page_a = BasicPageGuard(bpm.get(), page1);

  // after drop, whether destructor decrements the pin_count_ ?
  {
    auto read_guard1 = bpm->FetchPageRead(page_id_temp_a);
    EXPECT_EQ(2, page1->GetPinCount());
    read_guard1.Drop();
    EXPECT_EQ(1, page1->GetPinCount());
  }
  EXPECT_EQ(1, page0->GetPinCount());
  EXPECT_EQ(1, page1->GetPinCount());
  // test the move assignment
  {
    auto read_guard1 = bpm->FetchPageRead(page_id_temp);
    auto read_guard2 = bpm->FetchPageRead(page_id_temp_a);
    EXPECT_EQ(2, page0->GetPinCount());
    EXPECT_EQ(2, page1->GetPinCount());
    read_guard2 = std::move(read_guard1);
    EXPECT_EQ(2, page0->GetPinCount());
    EXPECT_EQ(1, page1->GetPinCount());
  }
  EXPECT_EQ(1, page0->GetPinCount());
  // test the move constructor
  {
    auto read_guard1 = bpm->FetchPageRead(page_id_temp);
    auto read_guard2(std::move(read_guard1));
    auto read_guard3(std::move(read_guard2));
    EXPECT_EQ(2, page0->GetPinCount());
  }
  EXPECT_EQ(1, page0->GetPinCount());
  EXPECT_EQ(page_id_temp, page0->GetPageId());

  // repeat drop
  guarded_page.Drop();
  EXPECT_EQ(0, page0->GetPinCount());
  guarded_page.Drop();
  EXPECT_EQ(0, page0->GetPinCount());

  disk_manager->ShutDown();
}

}  // namespace bustub
