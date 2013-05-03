#include <time.h>
#include <stdint.h>

#include "codec.h"
#include "hstore.h"

#include "slice.h"
#include <string.h>
#include <stdio.h>
#include <string>


int FLAGS_scan_threads = 0;

class Beansdb {
 public:
  bool Open(const std::string& base_path, int ndir, bool use_exiting_db) {
    if (ndir != 1 && ndir != 16 && ndir != 16*16 && ndir != 16*16*16) {
      fprintf(stderr, "Beansdb ndir %d error\n", ndir);
      return false;
    }
    base_path_ = base_path;
    ndir_ = ndir;
    // hs_open(path, height, before, scan_threads)
    int before = 0;
    store_ = hs_open(const_cast<char*>(base_path.c_str()), ndir / 16,
                     before, FLAGS_scan_threads);
    if (store_ == NULL) {
      fprintf(stderr, "Beansdb open %s failed\n", base_path.c_str());
      return false;
    } else {
      return true;
    }
  }

  bool Get(const Slice& key, std::string* value) {
    int vlen = 0;
    int flag = 0;
    char* svalue = hs_get(store_, key.data(), &vlen, &flag);
    if (svalue == NULL) {
      return false;
    } else {
      value->assign(svalue, vlen);
      free(svalue);
      return true;
    }
  }

  bool Put(const Slice& key, const Slice& value) {
    int flag = 0;
    int version = 0;
    bool status = hs_set(store_, key.data(), value.data(), value.size(), flag, version);
    return status;
  }
  void Close() {
    if (store_ != NULL) {
      hs_close(store_);
    }
    return true;
  }
 private:
  HStore* store_;
  std::string base_path_;
  int ndir_;
};

int main() {
  int kValueSize = 100;
  Beansdb db;
  db.Open("./beans-data", 1, true);
  int kNum = 100;
  for (int i = 0; i < kNum; i++) {
    char key[100];
    snprintf(key, sizeof(key), "%016d", i);
    char value[kValueSize];
    snprintf(value, sizeof(value), "%016d", i);
    bool s = db.Put(key, value);
    if (!s) {
      printf("add %d error\n", i);
      return 1;
    }
  }

  for (int i = 0; i < kNum; i++) {
    char key[100];
    snprintf(key, sizeof(key), "%016d", i);
    std::string value;
    bool s = db.Get(key, &value);
    if (!s) {
      printf("get %d error\n", i);
      return 1;
    }
    if (value != key) {
      printf("get %d error\n", i);
      printf("value = %s, key=%s\n", value.c_str(), key);
    }
  }
  printf("test passed\n");

  return 0;
}
