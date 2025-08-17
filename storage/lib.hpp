#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <optional>
#include <queue>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

template <typename Clock> class KVStorage {
public:
  using time_point = decltype(Clock::now());

  explicit KVStorage(
      std::span<std::tuple<std::string, std::string, uint32_t>> entries,
      Clock clock = Clock())
      : clock_(std::move(clock)) {
    for (auto &t : entries) {
      std::string k = std::get<0>(t);
      std::string v = std::get<1>(t);
      uint32_t ttl = std::get<2>(t);
      set(k, v, ttl);
    }
  };

  ~KVStorage() = default;

  void set(std::string key, std::string value, uint32_t ttl) {
    time_point expires = (ttl == 0) ? time_point::max()
                                    : Clock::now() + std::chrono::seconds(ttl);
    map_[key] = Entry{std::move(value), expires};

    expiry_heap_.push(ExpItem{key, expires});
  }

  bool remove(std::string_view key) {
    auto it = map_.find(std::string(key));
    if (it == map_.end())
      return false;
    map_.erase(it);

    return true;
  }

  std::optional<std::string> get(std::string_view key) const {
    auto it = map_.find(std::string(key));
    if (it == map_.end())
      return std::nullopt;
    auto now = Clock::now();
    if (it->second.expires_at != time_point::max() &&
        now >= it->second.expires_at) {
      return std::nullopt;
    }
    return it->second.value;
  }

  std::vector<std::pair<std::string, std::string>>
  getManySorted(std::string_view key, uint32_t count) const {
    auto now = Clock::now();
    std::vector<std::string> keys;
    keys.reserve(map_.size());
    for (const auto &kv : map_) {
      if (kv.second.expires_at == time_point::max() ||
          now < kv.second.expires_at) {
        keys.push_back(kv.first);
      }
    }

    std::sort(keys.begin(), keys.end());
    std::vector<std::pair<std::string, std::string>> res;

    auto it = std::lower_bound(keys.begin(), keys.end(), std::string(key));
    while (it != keys.end() && res.size() < count) {
      const auto &k = *it;
      auto mit = map_.find(k);
      if (mit != map_.end()) {
        res.emplace_back(k, mit->second.value);
      }
      ++it;
    }
    return res;
  }

  std::optional<std::pair<std::string, std::string>> removeOneExpiredEntry() {
    auto now = Clock::now();
    while (!expiry_heap_.empty()) {
      auto top = expiry_heap_.top();
      if (top.expires_at > now)
        return std::nullopt;

      expiry_heap_.pop();
      auto mit = map_.find(top.key);
      if (mit == map_.end()) {
        continue;
      }
      if (mit->second.expires_at != top.expires_at) {
        continue;
      }
      std::pair<std::string, std::string> ret{mit->first,
                                              std::move(mit->second.value)};
      map_.erase(mit);
      return ret;
    }
    return std::nullopt;
  }

private:
  struct Entry {
    std::string value;
    time_point expires_at;
  };

  struct ExpItem {
    std::string key;
    time_point expires_at;
    bool operator>(const ExpItem &o) const {
      if (expires_at != o.expires_at)
        return expires_at > o.expires_at;
      return key > o.key;
    }
  };

  Clock clock_;
  std::unordered_map<std::string, Entry> map_;
  std::priority_queue<ExpItem, std::vector<ExpItem>, std::greater<ExpItem>>
      expiry_heap_;
};
