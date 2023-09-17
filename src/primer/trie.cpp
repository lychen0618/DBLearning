#include "primer/trie.h"
#include <stack>
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  std::shared_ptr<const TrieNode> cur = root_;
  for (char c : key) {
    if (cur && cur->children_.find(c) != cur->children_.end()) {
      cur = cur->children_.find(c)->second;
    } else {
      return nullptr;
    }
  }
  auto res = dynamic_cast<const TrieNodeWithValue<T> *>(cur.get());
  return res ? res->value_.get() : nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  std::shared_ptr<TrieNode> new_prev;
  std::shared_ptr<TrieNode> new_cur = std::make_shared<TrieNode>();
  if (root_) {
    new_cur = root_->Clone();
  }
  std::shared_ptr<TrieNode> new_root = new_cur;
  size_t idx = 0;
  while (idx < key.size()) {
    std::shared_ptr<TrieNode> new_node = std::make_shared<TrieNode>();
    if (new_cur->children_.find(key[idx]) != new_cur->children_.end()) {
      new_node = new_cur->children_.find(key[idx])->second->Clone();
    }
    new_cur->children_[key[idx]] = new_node;
    new_prev = new_cur;
    new_cur = new_node;
    ++idx;
  }
  new_cur = std::make_shared<TrieNodeWithValue<T>>(new_cur->children_, std::make_shared<T>(std::move(value)));
  if (new_prev) {
    new_prev->children_[key.back()] = new_cur;
  } else {
    new_root = new_cur;
  }
  return Trie(new_root);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  std::shared_ptr<TrieNode> new_prev;
  std::shared_ptr<TrieNode> new_cur = std::make_shared<TrieNode>();
  if (root_) {
    new_cur = root_->Clone();
  }
  std::stack<std::shared_ptr<TrieNode>> st;
  std::shared_ptr<TrieNode> new_root = new_cur;
  for (char c : key) {
    st.push(new_cur);
    std::shared_ptr<TrieNode> new_node = new_cur->children_.find(c)->second->Clone();
    new_cur->children_[c] = new_node;
    new_prev = new_cur;
    new_cur = new_node;
  }
  new_cur = std::make_shared<TrieNode>(new_cur->children_);
  if (new_prev) {
    new_prev->children_[key.back()] = new_cur;
  } else {
    new_root = new_cur;
  }
  if (new_cur->children_.empty()) {
    size_t idx = key.size() - 1;
    while (!st.empty()) {
      auto t = st.top();
      t->children_.erase(key[idx]);
      idx--;
      if (t->is_value_node_ || !t->children_.empty()) {
        break;
      }
      st.pop();
    }
  }
  return Trie(new_root);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
