
#ifndef SLICE_H
#define SLICE_H
// os
// c
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdint.h>
// c++
#include <string>

class Slice {
 public:
  Slice() : data_(""),size_(0) { }
  Slice(const char* d, size_t n) : data_(d), size_(n) { }
  Slice(const std::string& s) : data_(s.data()), size_(s.size()) { }
  Slice(const char* s) : data_(s), size_(strlen(s)) { }

  size_t size() const { return size_; }
  bool empty() const { return size_ == 0; }
  char operator[](size_t n) const {
    assert(n < size_);
    return data_[n];
  }
  const char* data() const { return data_; }

  void clear() { data_ = ""; size_ = 0; }

  std::string ToString() const { return std::string(data_, size_); }

  int compare(const Slice& b) const;

  bool starts_with(const Slice& x) const {
    return (size_ >= x.size_) && (memcmp(data_, x.data_, x.size_) == 0);
  }
 private:
  const char* data_;
  size_t size_;
};

inline bool operator == (const Slice& x, const Slice& y) {
  return ((x.size() == y.size()) &&
          (memcmp(x.data(), y.data(), x.size()) == 0));
}

inline bool operator != (const Slice& x, const Slice& y) {
  return !(x == y);
}

inline int Slice::compare(const Slice& b) const {
  const int min_len = (size_ < b.size_) ? size_ : b.size_;
  int r = memcmp(data_, b.data_, min_len);
  if (r == 0) {
    if (size_ < b.size_) r = -1;
    else if (size_ > b.size_) r = 1;
  }
  return r;
}

#endif
