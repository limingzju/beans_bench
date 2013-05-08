#ifndef MONITOR_H
#define MONITOR_H
#include "string_util.h"
#include "slice.h"

#include <sys/types.h>
#include <unistd.h>

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <time.h>

#include <string>
#include <vector>



class FreeMonitor {
 private:
  struct FreeStat {
    time_t now;
    std::string mem_total;
    std::string mem_free;
    std::string buffers;
    std::string cached;
  };

  std::vector<FreeStat> memory_stats_;

 public:
  FreeMonitor(const std::string& name) {
    fname_ = name;
    FILE* f = fopen(fname_.c_str(), "w");
    fprintf(f, "time\tMemTotal\tMemFree\tBuffers\tCached\n");
    fclose(f);
  }
  ~FreeMonitor() {
    Write();
  }

  void Write() {
    if (memory_stats_.size() != 0) {
      FILE* f = fopen(fname_.c_str(), "a+");
      if (f == NULL) {
        fprintf(stderr, "IOError: Open %s failed\n", fname_.c_str());
        return;
      }
      for (size_t i = 0; i < memory_stats_.size(); i++) {
        fprintf(f, "%ld\t%s\t%s\t%s\t%s\n",
                memory_stats_[i].now,
                memory_stats_[i].mem_total.c_str(),
                memory_stats_[i].mem_free.c_str(),
                memory_stats_[i].buffers.c_str(),
                memory_stats_[i].cached.c_str());
      }
      fclose(f);
      memory_stats_.clear();
    }
  }

  void StatFree() {
    if (memory_stats_.size() >= 1024) {
      Write();
    }
    FILE* f = fopen("/proc/meminfo", "r");
    if (f == NULL) {
        fprintf(stderr, "IOError: Open %s failed\n", fname_.c_str());
        return;
    }
    char line[1000];
    FreeStat freestat;
    time_t now = time(NULL);
    freestat.now = now;
    while (fgets(line, sizeof(line), f) != NULL) {
      const char* sep = strchr(line, ':');
      if (sep == NULL) continue;
      Slice key = TrimSpace(Slice(line, sep - line));
      Slice value = TrimSpace(Slice(sep+1));
      if (key == "MemTotal") {
        freestat.mem_total = value.ToString();
      } else if (key == "MemFree") {
        freestat.mem_free = value.ToString();
      } else if (key == "Buffers") {
        freestat.buffers = value.ToString();
      } else if (key == "Cached") {
        freestat.cached = value.ToString();
      }
    }
    memory_stats_.push_back(freestat);
    fclose(f);
  }
 private:
  std::string fname_;
};

class Monitor {
 private:
  struct MemStat{
    time_t now;
    int64_t vm_size;
    int64_t res_size;
  };

  std::vector<MemStat> mem_stats_;

 public:
  Monitor(const std::string& fname) {
    page_size_ = sysconf(_SC_PAGESIZE);
    assert(page_size_ > 0);
    fprintf(stdout, "page_size = %d\n", page_size_);
    fname_ = fname;
    FILE* f = fopen(fname_.c_str(), "w");
    fprintf(f, "time\tvm_size\tres_size\n");
    fclose(f);

  }

  void Write() {
      FILE* fstats = fopen(fname_.c_str(), "a+");
      if (fstats == NULL) {
        fprintf(stderr, "IOError: Open %s failed\n", fname_.c_str());
        return;
      }
      for (size_t i = 0; i != mem_stats_.size(); i++) {
        fprintf(fstats, "%ld\t%ld\t%ld\n", mem_stats_[i].now, mem_stats_[i].vm_size, mem_stats_[i].res_size);
      }
      fclose(fstats);
      mem_stats_.clear();

  }
  ~Monitor() {
    if (mem_stats_.size() != 0) {
      Write();
    }
  }

  void StatMemory() {
    if (mem_stats_.size() >= 1024) {
      Write();
    }

    // const int kVirtIndex = 22;
    // const int kResIndex = 23;
    pid_t pid = getpid();
    char fname[256];
    snprintf(fname, sizeof(fname), "/proc/%d/statm", pid);
    time_t current = time(NULL);
    FILE* f = fopen(fname, "r");
    if (f == NULL) {
      fprintf(stderr, "IOError: Open %s failed\n", fname);
      return;
    }

    int vm_page = 0;
    int res_page = 0;
    if (fscanf(f, "%d %d", &vm_page, &res_page) != 2) {
      fprintf(stderr, "IOError: scanf %s failed\n", fname);
      return;
    }
    fclose(f);


    int64_t vm_size = vm_page * page_size_;
    //int vm_size = vm_page;
    int64_t res_size = res_page * page_size_;
    //int res_size = res_page;

    MemStat memstat;
    memstat.now = current;
    memstat.vm_size = vm_size;
    memstat.res_size = res_size;
    mem_stats_.push_back(memstat);
  }
 private:
  int page_size_;
  std::string fname_;
};

#endif
