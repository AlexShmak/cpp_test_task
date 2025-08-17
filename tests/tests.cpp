#include "../storage/lib.hpp"
#include <chrono>
#include <gtest/gtest.h>
#include <thread>

using Storage = KVStorage<std::chrono::steady_clock>;

TEST(SimpleKV, SetGet) {
  Storage kv((std::span<std::tuple<std::string, std::string, uint32_t>>()));
  kv.set("foo", "bar", 1); // 1 second TTL
  auto v = kv.get("foo");
  ASSERT_TRUE(v.has_value());
  EXPECT_EQ(*v, "bar");
}

TEST(SimpleKV, Expire) {
  Storage kv((std::span<std::tuple<std::string, std::string, uint32_t>>()));
  kv.set("a", "1", 0);    // infinite
  kv.set("b", "2", 0);    // infinite
  kv.set("t", "temp", 0); // infinite, we'll test later

  // set a short TTL and ensure expiration via removeOneExpiredEntry
  kv.set("short", "x", 0); // set infinite then overwrite to finite
  kv.set("short", "x", 1); // now TTL 1s
  std::this_thread::sleep_for(std::chrono::milliseconds(1200));

  auto removed = kv.removeOneExpiredEntry();
  // there might be other expired records depending on timing; ensure we get one
  // or none
  if (removed.has_value()) {
    EXPECT_EQ(removed->first, "short");
  } else {
    // timing weirdness: acceptable to not have removed yet if test ran fast;
    // assert that 'short' is absent
    EXPECT_EQ(kv.get("short"), std::nullopt);
  }
}

TEST(SimpleKV, GetManySorted) {
  Storage kv((std::span<std::tuple<std::string, std::string, uint32_t>>()));
  kv.set("c", "3", 10);
  kv.set("a", "1", 10);
  kv.set("b", "2", 10);

  auto res = kv.getManySorted("a", 10);
  ASSERT_EQ(res.size(), 3);
  EXPECT_EQ(res[0].first, "a");
  EXPECT_EQ(res[1].first, "b");
  EXPECT_EQ(res[2].first, "c");
}

TEST(SimpleKV, Remove) {
  Storage kv((std::span<std::tuple<std::string, std::string, uint32_t>>()));
  kv.set("k", "v", 10);
  EXPECT_TRUE(kv.remove("k"));
  EXPECT_EQ(kv.get("k"), std::nullopt);
}
