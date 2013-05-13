// project
#include "monitor.h"
#include "slice.h"
#include "random.h"
#include "histogram.h"
#include "concurrent.h"
#include "string_util.h"
#include "hstore.h"
// os
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/time.h>
// c
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdint.h>
// c++
#include <string>

#include <google/profiler.h>

// third lib
//#include <boost/scoped_ptr.hpp>

#define ASSERT_TRUE(s) {if (!(s)) {fprintf(stderr, "%s failed\n", #s); exit(1);}}
#define ASSERT_FALSE(s) {if (s) {fprintf(stderr, "%s failed\n", #s); exit(1);}}
#define ASSERT_EQ(a, b) {if ((a) != (b)) {fprintf(stderr, "%s != %s Failed\n", #a, #b); exit(1);}}


static const char* FLAGS_benchmarks =
"write,"
"read,"
"readhot,"
"readwhilewriting"
;


volatile int g_exit = 0;

template <typename T>
class scoped_ptr {
 public:
  scoped_ptr(T *ptr) : ptr_(ptr) {
  }
  ~scoped_ptr() {
    if (ptr_ != NULL) {
      delete ptr_;
    }
  }
 private:
  T* ptr_;
};

// Number of key/values to place in database
static int FLAGS_num = 10000;
static int FLAGS_reads = 100;
// size of each value
static int FLAGS_value_size = 100000; // 100KB
static bool FLAGS_histogram = false;
static int FLAGS_num_threads = 1;
static bool FLAGS_use_existing_db = true;
static int FLAGS_filesystem_ndir = 1;
static double FLAGS_read_write_ratio = 1;

// Beansdb
static int FLAGS_scan_threads = 0;

//FLAGS_db in {"beansdb", "fdb"}
static const char* FLAGS_db = "beansdb";



void PrintFlags() {
  printf("------------------------------------------------------\n");
  printf("FLAGS_benchmarks = %s\n", FLAGS_benchmarks);
  printf("FLAGS_num = %d\n", FLAGS_num);
  printf("FLAGS_reads = %d\n", FLAGS_reads);
  printf("FLAGS_value_size = %d\n", FLAGS_value_size);
  printf("FLAGS_histogram = %d\n", FLAGS_histogram);
  printf("FLAGS_num_threads = %d\n", FLAGS_num_threads);
  printf("FLAGS_filesystem_ndir = %d\n", FLAGS_filesystem_ndir);
  printf("FLAGS_read_write_ration = %lf\n", FLAGS_read_write_ratio);
  printf("FLAGS_db = %s\n", FLAGS_db);
  printf("FLAGS_scan_threads = %d\n", FLAGS_scan_threads);
  printf("------------------------------------------------------\n");
}

inline char* string_as_array(std::string* str) {
  assert(str != NULL);
  return str->empty() ? NULL : &*str->begin();
}


static void AppendWithSpace(std::string* str, Slice msg) {
  if (msg.empty()) return;
  if (!str->empty()) {
    str->push_back(' ');
  }
  str->append(msg.data(), msg.size());
}

namespace Env {
bool CreateDir(const std::string& name) {
  if (mkdir(name.c_str(), 0755) != 0) {
    fprintf(stderr, "IOError: %s %s\n", name.c_str(), strerror(errno));
    return false;
  }
  return true;
}

bool FileExists(const std::string& name) {
  return access(name.c_str(), F_OK) == 0;
}

bool DeleteFile(const std::string& name) {
  if (unlink(name.c_str()) != 0) {
    fprintf(stderr, "IOError: %s %s\n", name.c_str(), strerror(errno));
    return false;
  }
  return true;
}

// An ugly implemention
bool DeleteDir(const std::string& name) {
  char cmd[256];
  snprintf(cmd, sizeof(cmd), "rm -rf %s", name.c_str());
  int ret = system(cmd);
  return ret == 0;

}

bool GetFileSize(const std::string& name, uint64_t* size) {
  struct stat sbuf;
  if (stat(name.c_str(), &sbuf) != 0) {
    *size = 0;
    fprintf(stderr, "IOError: %s %s\n", name.c_str(), strerror(errno));
    return false;
  } else {
    *size = sbuf.st_size;
  }
  return true;
}

bool GetFileSize(int fd, uint64_t* size) {
  struct stat sbuf;
  if (fstat(fd, &sbuf) != 0) {
    *size = 0;
    fprintf(stderr, "IOError: %s\n", strerror(errno));
    return false;
  } else {
    *size = sbuf.st_size;
  }
  return true;
}

uint64_t NowMicros() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

void SleepForMicroseconds(int micros) {
  usleep(micros);
}

bool RenameFile(const std::string& src, const std::string& target) {
  if (rename(src.c_str(), target.c_str()) != 0) {
    fprintf(stderr, "IOError: %s %s %s", src.c_str(), target.c_str(), strerror(errno));
    return false;
  }
  return true;
}

};

#ifndef FALLTHROUGH_INTENDED
#define FALLTHROUGH_INTENDED do { } while (0)
#endif

uint32_t DecodeFixed32(const char* ptr) {
  uint32_t result;
  memcpy(&result, ptr, sizeof(result));
  return result;
}
uint32_t Hash(const char* data, size_t n/*, uint32_t seed*/) {
  // Similar to murmur hash
  uint32_t seed = 0;
  const uint32_t m = 0xc6a4a793;
  const uint32_t r = 24;
  const char* limit = data + n;
  uint32_t h = seed ^ (n * m);

  // Pick up four bytes at a time
  while (data + 4 <= limit) {
    uint32_t w = DecodeFixed32(data);
    data += 4;
    h += w;
    h *= m;
    h ^= (h >> 16);
  }

  // Pick up remaining bytes
  switch (limit - data) {
  case 3:
    h += data[2] << 16;
    FALLTHROUGH_INTENDED;
  case 2:
    h += data[1] << 8;
    FALLTHROUGH_INTENDED;
  case 1:
    h += data[0];
    h *= m;
    h ^= (h >> r);
    break;
  }
  return h;
}

// uint32_t Hash(const char* data, size_t n) {
//   const static int FNV_32_PRIME = 0x01000193;
//   const static int FNV_32_INIT = 0x811c9dc5;

//   uint32_t h = FNV_32_INIT;
//   for (size_t i = 0; i < n; i++) {
//     h ^= (uint32_t) data[i];
//     h *= FNV_32_PRIME;
//   }
//   return h;
// }

class ScopedFile {
 public:
  ScopedFile(int fd) : fd_(fd) { }
  ~ScopedFile() {
    close(fd_);
  }
 private:
  int fd_;
};

class DB {
 public:
  DB() {}
  virtual ~DB() {}
  virtual bool Open(const std::string& base_path, int ndir, bool use_exiting_db) = 0;
  virtual bool Get(const Slice& key, std::string* value) = 0;
  virtual bool Put(const Slice& key, const Slice& value) = 0;
};

class Beansdb : public DB {
 public:
  Beansdb() : store_(NULL) {}
  ~Beansdb() {
    if (store_ != NULL) {
      hs_close(store_);
    }
  }
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
    uint32_t flag = 0x00000010;
    char* svalue = hs_get(store_, const_cast<char*>(key.data()), &vlen, &flag);
    if (svalue == NULL) {
      return false;
    } else {
      value->assign(svalue, vlen);
      free(svalue);
      return true;
    }
  }

  bool Put(const Slice& key, const Slice& value) {
    uint32_t flag = 0x00000010;
    int version = 0;
    bool status = hs_set(store_, const_cast<char*>(key.data()),
                         const_cast<char*>(value.data()),value.size(),
                         flag, version);
    return status;
  }
 private:
  HStore* store_;
  std::string base_path_;
  int ndir_;
};

class FileSystemDB : public DB {
 public:
  bool Open(const std::string& base_path, int ndir, bool use_existing_db) {
    base_path_ = base_path;
    ndir_ = ndir;
    bool status = false;

    if (use_existing_db) {
      if (!Env::FileExists(base_path)) {
        status = Env::CreateDir(base_path);
        if (!status) return false;
        for  (int i = 0; i < ndir; i++) {
          char fullname[256];
          snprintf(fullname, sizeof(fullname), "%s/%d", base_path.c_str(), i);
          if (!Env::FileExists(fullname)) {
            status = Env::CreateDir(fullname);
            if (!status) return false;
          }
          for (int j = 0; j < 1000; j++) {
            snprintf(fullname, sizeof(fullname), "%s/%d/%d", base_path.c_str(), i, j);
            // printf("Create dir %s\n", fullname);
            if (!Env::FileExists(fullname)) {
              status = Env::CreateDir(fullname);
              if (!status) return false;
            }
          }
        }
      }
    } else {
      fprintf(stderr, "use_existing_db must be true\n");
      exit(1);
    }
    return true;
  }

  bool Put(const Slice& key, const Slice& value) {
    uint32_t hash  = Hash(key.data(), key.size());
    int segment = hash % ndir_;
    int segment2 = (hash >> 16) % 1000;
    char filename[512];
    char filename_tmp[512];
    snprintf(filename, sizeof(filename), "%s/%d/%d/%s", base_path_.c_str(), segment, segment2, key.data());
    snprintf(filename_tmp, sizeof(filename_tmp), "%s/%d/%d/%s.tmp", base_path_.c_str(), segment, segment2, key.data());
    // File System Put is very ineffienct
    // We write it to a temp file then rename it
    // There are some problem with the filename_tmp, because may be multiple
    // writers, but as it is a simple program we just ignore it
    {
      {
        int fd = open(filename_tmp, O_WRONLY|O_CREAT|O_TRUNC, 0755);
        if (fd < 0) {
          fprintf(stderr, "IOError: open %s %s\n", key.data(), strerror(errno));
          return false;
        }
        ScopedFile scoped_file(fd);
        ssize_t n = write(fd, value.data(), value.size());
        if (n < 0 || static_cast<size_t>(n) != value.size()) {
          fprintf(stderr, "IOError: write %s %s\n", key.data(), strerror(errno));
          return false;
        }
      }

      if (rename(filename_tmp, filename) != 0) {
        fprintf(stderr, "IOError: rename %s %s %s\n",
                filename_tmp, filename, strerror(errno));
        return false;
      }
    }
    return true;
  }

  bool Get(const Slice& key, std::string* value) {
    uint32_t hash = Hash(key.data(), key.size());
    int segment = hash % ndir_;
    int segment2 = (hash >> 16) % 1000;

    char filename[512];
    snprintf(filename, sizeof(filename), "%s/%d/%d/%s", base_path_.c_str(), segment, segment2, key.data());
    int fd = open(filename, O_RDONLY);
    if (fd < 0) {
      return false;
    }
    ScopedFile scoped_file(fd);

    uint64_t size = 0;
    bool s = Env::GetFileSize(fd, &size);
    if (!s) {
      return s;
    }

    value->resize(size);
    char* buf = string_as_array(value);

    ssize_t nread = read(fd, buf, size);
    if (nread < 0 || static_cast<size_t>(nread) != size) {
      fprintf(stderr, "IOError: %s %s\n", key.data(), strerror(errno));
      return false;
    }
    return true;
  }

 private:
  std::string base_path_;
  int ndir_;
};


class Stats {
 private:
  double start_;
  double finish_;
  double seconds_;
  int done_;
  int next_report_;
  int64_t bytes_;
  double last_op_finish_;
  Histogram hist_;
  std::string message_;

 public:
  Stats() { Start(); }

  void Start() {
    next_report_ = 100;
    last_op_finish_ = start_;
    hist_.Clear();
    done_ = 0;
    bytes_ = 0;
    seconds_ = 0;
    start_ = Env::NowMicros();
    finish_ = start_;
    message_.clear();
  }

  void Merge(const Stats& other) {
    hist_.Merge(other.hist_);
    done_ += other.done_;
    bytes_ += other.bytes_;
    seconds_ += other.seconds_;
    if (other.start_ < start_) start_ = other.start_;
    if (other.finish_ > finish_) finish_ = other.finish_;

    // Just keep the messages from one thread
    if (message_.empty()) message_ = other.message_;
  }

  void Stop() {
    finish_ = Env::NowMicros();
    seconds_ = (finish_ - start_) * 1e-6;
  }

  void AddMessage(Slice msg) {
    AppendWithSpace(&message_, msg);
  }

  void FinishedSingleOp() {
    if (FLAGS_histogram) {
      double now = Env::NowMicros();
      double micros = now - last_op_finish_;
      hist_.Add(micros);
//      if (micros > 20000) {
//        fprintf(stderr, "long op: %.1f micros%30s\n", micros, "");
//        fflush(stderr);
//      }
      last_op_finish_ = now;
    }

    done_++;
    if (done_ >= next_report_) {
      if      (next_report_ < 1000)   next_report_ += 100;
      else if (next_report_ < 5000)   next_report_ += 500;
      else if (next_report_ < 10000)  next_report_ += 1000;
      else if (next_report_ < 50000)  next_report_ += 5000;
      else if (next_report_ < 100000) next_report_ += 10000;
      else if (next_report_ < 500000) next_report_ += 50000;
      else                            next_report_ += 100000;
      fprintf(stderr, "... finished %d ops%30s\n", done_, "");
//      fflush(stderr);
    }
  }

  void AddBytes(int64_t n) {
    bytes_ += n;
  }

  void Report(const Slice& name) {
    // Pretend at least one op was done in case we are running a benchmark
    // that does not call FinishedSingleOp().
    if (done_ < 1) done_ = 1;

    std::string extra;
    double elapsed = (finish_ - start_) * 1e-6;
    if (bytes_ > 0) {
      // Rate is computed on actual elapsed time, not the sum of per-thread
      // elapsed times.
      char rate[100];
      snprintf(rate, sizeof(rate), "%6.1f MB/s",
               (bytes_ / 1048576.0) / elapsed);
      extra = rate;
    }

    AppendWithSpace(&extra, message_);

    fprintf(stdout, "start    %lld\n", static_cast<uint64_t>(start_*1e-6));
    fprintf(stdout, "finish   %lld\n", static_cast<uint64_t>(finish_*1e-6));
    fprintf(stdout, "elapsed  %lld\n", static_cast<uint64_t>((finish_ - start_)*1e-6));
    fprintf(stdout, "%-12s : %11.3f ops/s ;%s%s\n",
            name.ToString().c_str(),
            done_ / elapsed,
            (extra.empty() ? "" : " "),
            extra.c_str());
    if (FLAGS_histogram) {
      fprintf(stdout, "Microseconds per op:\n%s\n", hist_.ToString().c_str());
    }
    fflush(stdout);
  }
};

struct SharedState {
  Mutex mu;
  CondVar cv;
  int total;

  int num_initialized;
  int num_done;
  bool start;

  SharedState() : cv(&mu) {}
};

struct ThreadState {
  int tid;
  Random rand;
  Stats stats;
  SharedState* shared;

  ThreadState(int index)
      : tid(index),
      rand(static_cast<int>(time(NULL)) + index)
      /* rand(1000 + index) */{
      }
};

class Benchmark {
 public:
  Benchmark(DB* db) {
    db_ = db;
  }
  ~Benchmark() {
    //    Env::DeleteDir("./fdb_test");
  }

  bool Open() {
    double start = Env::NowMicros();
    ASSERT_TRUE(db_->Open("./dbtest", FLAGS_filesystem_ndir, FLAGS_use_existing_db));
    double finish = Env::NowMicros();
    fprintf(stdout, "Start time = %d ms\n", (int)((finish - start) * 1e-3));
    return true;
  }

  void Run() {
    PrintHeader();
    Open();
    const char* functions[] = {"write", "read", "readhot", "readwhilewriting"};
    typedef void (Benchmark::*Method)(ThreadState*);
    Method methods[] = {&Benchmark::WriteSequential, &Benchmark::ReadRandom,
      &Benchmark::ReadHot, &Benchmark::ReadWhileWriting};
    Method method;

    std::vector<std::string> strs;
    split_string(FLAGS_benchmarks, ',', &strs);

//    ProfilerStart("db_bench.prof");
    for (size_t i = 0; i < strs.size(); i++) {
      method = NULL;
      size_t j;
      for (j = 0; j < sizeof(functions)/sizeof(functions[0]); j++) {
        if (functions[j] == strs[i]) {
          method = methods[j];
          break;
        }
      }
      if (method != NULL) {
        RunBenchMark(FLAGS_num_threads, Slice(functions[j]), method);
      }
    }
//    ProfilerStop();

    g_exit = 1;
    fprintf(stdout, "RUN TEST DONE\n");
    fflush(stdout);
  }

 private:
  struct ThreadArg {
    Benchmark* bm;
    SharedState* shared;
    ThreadState* thread;
    void (Benchmark::*method)(ThreadState*);
  };

  static void* ThreadBody(void* v) {
//    ProfilerRegisterThread();
    ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
    SharedState* shared = arg->shared;
    ThreadState* thread = arg->thread;
    {
      MutexLock l(&shared->mu);
      shared->num_initialized++;
      if (shared->num_initialized >= shared->total) {
        shared->cv.SignalAll();
      }

      while (!shared->start) {
        shared->cv.Wait();
      }
    }

    thread->stats.Start();
    (arg->bm->*(arg->method))(thread);
    thread->stats.Stop();

    {
      MutexLock l(&shared->mu);
      shared->num_done++;
      if (shared->num_done >= shared->total) {
        shared->cv.SignalAll();
      }
    }
    return (void*)NULL;
  }


  void PrintHeader() {
//    const int kKeySize = 16;
    PrintEnviroment();
    PrintFlags();
    //    fprintf(stdout, "Keys:        %d bytes each\n", kKeySize);
    //    fprintf(stdout, "Valuees:     %d bytes each\n", FLAGS_value_size);
    //    fprintf(stdout, "Entries:     %d\n", FLAGS_num);
    fflush(stdout);
  }

  void PrintEnviroment() {
    time_t now = time(NULL);
    fprintf(stdout, "Date:       %s", ctime(&now));

    FILE* cpuinfo = fopen("/proc/cpuinfo", "r");
    if (cpuinfo != NULL) {
      char line[1000];
      int num_cpus = 0;
      std::string cpu_type;
      std::string cache_size;
      while (fgets(line, sizeof(line), cpuinfo) != NULL) {
        const char* sep = strchr(line, ':');
        if (sep == NULL) {
          continue;
        }
        Slice key = TrimSpace(Slice(line, sep - 1 - line));
        Slice val = TrimSpace(Slice(sep + 1));
        if (key == "model name") {
          ++num_cpus;
          cpu_type = val.ToString();
        } else if (key == "cache size") {
          cache_size = val.ToString();
        }
      }
      fclose(cpuinfo);
      fprintf(stdout, "CPU:        %d * %s\n", num_cpus, cpu_type.c_str());
      fprintf(stdout, "CPUCache:   %s\n", cache_size.c_str());
    }
  }

  void WriteSequential(ThreadState* thread) {
    std::string value;
    value.resize(FLAGS_value_size);
    for (int i = 0; i < FLAGS_value_size; i++) {
      value[i] = 'a' + i % 26;
    }

    int64_t bytes = 0;

    int step = FLAGS_num / FLAGS_num_threads;
    int start = step * thread->tid;
    int end = (thread->tid == FLAGS_num_threads-1) ? FLAGS_num : step * (thread->tid +1);

    printf("Tid %d start %d end %d\n", thread->tid, start, end);
    for (int i = start; i < end; i++) {
      char key[100];
      snprintf(key, sizeof(key), "%016d", i);
      Slice skey(key);
      ASSERT_TRUE(db_->Put(skey, Slice(value)));
      thread->stats.FinishedSingleOp();

      bytes += FLAGS_value_size + skey.size();
    }
    thread->stats.AddBytes(bytes);
  }

  void ReadRandom(ThreadState* thread) {
    int found = 0;
    for (int i = 0; i < FLAGS_reads; i++) {
      char key[100];
      int k = thread->rand.Next() % FLAGS_num;
      snprintf(key, sizeof(key), "%016d", k);
      std::string value;
      if (db_->Get(Slice(key), &value)) {
        found++;
      }
//      else {
//        fprintf(stderr, "NotFound %s\n", key);
//      }
      thread->stats.FinishedSingleOp();
    }
    char msg[100];
    snprintf(msg, sizeof(msg), "(%d of %d found)", found, FLAGS_num);
    thread->stats.AddMessage(msg);
  }

  void ReadWhileWriting(ThreadState* thread) {
    const int write_thread_num = int(FLAGS_num_threads / (1 + FLAGS_read_write_ratio));
    const int read_thread_num = FLAGS_num_threads - write_thread_num;
    printf("ReadWhileWriting: num_write %d num_read %d\n",
           write_thread_num, read_thread_num);
    if (thread->tid < write_thread_num) {
      // Write threads that keep writing until all read threads done
      int step = FLAGS_num / write_thread_num;
      int start = step * thread->tid;
      int end = (thread->tid == write_thread_num - 1) ? FLAGS_num : step * (thread->tid + 1);
      printf("WriteThread tid %d start %d end %d\n", thread->tid, start, end);

      std::string value;
      value.resize(FLAGS_value_size);
      for (int i = 0; i < FLAGS_value_size; i++) {
        value[i] = 'a' + i % 26;
      }

      int bytes = 0;

      while (true) {
        {
          MutexLock l(&thread->shared->mu);
          if (thread->shared->num_done >= read_thread_num) {
            break;
          }
        }
        int k = thread->rand.Next() % FLAGS_num;
        char key[100];
        snprintf(key, sizeof(key), "%016d", k);
        Slice skey(key);
        ASSERT_TRUE(db_->Put(skey, Slice(value)));
        bytes += skey.size() + FLAGS_value_size;
        thread->stats.FinishedSingleOp();
      }
      thread->stats.AddBytes(bytes);
    } else {
      // Do read
      ReadRandom(thread);
    }
  }

  void ReadHot(ThreadState* thread) {
    const int range = (FLAGS_num + 99) / 100;
    int found = 0;
    for (int i = 0; i < FLAGS_reads; i++) {
      int k = thread->rand.Next() % range;
      char key[100];
      snprintf(key, sizeof(key), "%016d", k);
      std::string value;
      if (db_->Get(Slice(key), &value)) {
        found++;
      }
      thread->stats.FinishedSingleOp();
    }
    char msg[100];
    snprintf(msg, sizeof(msg), "(%d of %d found)", found, range);
    thread->stats.AddMessage(msg);
  }

  void RunBenchMark(int n, Slice name,
                    void (Benchmark::*method)(ThreadState*)) {

    fprintf(stdout, "\n\n===================================\n");
    fprintf(stdout, "method = %s\n", name.data());
    SharedState shared;
    shared.total = n;
    shared.num_initialized = 0;
    shared.num_done = 0;
    shared.start = false;

    ThreadArg* arg = new ThreadArg[n];
    scoped_ptr<ThreadArg> scoped_arg(arg);
    pthread_t* tids = new pthread_t[n];
    for (int i = 0; i < n; i++) {
      arg[i].bm = this;
      arg[i].method = method;
      arg[i].shared = &shared;
      arg[i].thread = new ThreadState(i);
      arg[i].thread->shared = &shared;
      pthread_create(&tids[i], NULL, ThreadBody, &arg[i]);
    }

    shared.mu.Lock();
    while (shared.num_initialized < n) {
      shared.cv.Wait();
    }
    shared.start = true;
    shared.cv.SignalAll();
    while (shared.num_done < n) {
      shared.cv.Wait();
    }
    shared.mu.Unlock();

    time_t now = time(NULL);
    fprintf(stdout, "Date:       %s", ctime(&now));

    if (name == "readwhilewriting") {
      const int write_thread_num = int(FLAGS_num_threads / (1 + FLAGS_read_write_ratio));
//      const int read_thread_num = FLAGS_num_threads - write_thread_num;
//
//      for (int i = 0; i < n; i++) {
//        arg[i].thread->stats.Report(name);
//      }

      for (int i = 1; i < write_thread_num; i++) {
        arg[0].thread->stats.Merge(arg[i].thread->stats);
      }
      printf("\n\n***Write Performance***\n");
      arg[0].thread->stats.Report(name);

      for (int i = write_thread_num + 1; i < n; i++) {
        arg[write_thread_num].thread->stats.Merge(arg[i].thread->stats);
      }
      printf("\n\n***Read Performance***\n");
      arg[write_thread_num].thread->stats.Report(name);

    } else {
      for (int i = 1; i < n; i++) {
        arg[0].thread->stats.Merge(arg[i].thread->stats);
      }
      arg[0].thread->stats.Report(name);
    }
    for (int i = 0; i < n; i++) {
      delete arg[i].thread;
    }

  }

 private:
  FileSystemDB fdb_;
  DB* db_;
};

void TEST_FDB() {
  FileSystemDB fdb;
  ASSERT_TRUE(fdb.Open("./fdb", 5, true));

  for (int i = 0; i < 1000; i++) {
    char key[64];
    char value[64];
    snprintf(key, sizeof(key), "%016d", i);
    snprintf(value, sizeof(value), "%016d", i);

    bool s = fdb.Put(Slice(key), Slice(value));
    ASSERT_TRUE(s);
  }

  for (int i = 0; i < 1000; i++) {
    char key[64];
    char expect_value[64];
    snprintf(key, sizeof(key), "%016d", i);
    snprintf(expect_value, sizeof(expect_value), "%016d", i);
    std::string value;
    bool s = fdb.Get(Slice(key), &value);
    ASSERT_TRUE(s);
    ASSERT_EQ(expect_value, value);
  }

  for (int i = 1000; i < 2000; i++) {
    char key[64];
    char expect_value[64];
    snprintf(key, sizeof(key), "%016d", i);
    snprintf(expect_value, sizeof(expect_value), "%016d", i);
    std::string evalue;
    ASSERT_FALSE(fdb.Get(Slice(key), &evalue));
  }

  Env::DeleteDir("./fdb");

  printf("FileSystemDB Open Get Put Success\n");

}

void TEST_BEANSDB() {
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
      return;
    }
  }

  for (int i = 0; i < kNum; i++) {
    char key[100];
    snprintf(key, sizeof(key), "%016d", i);
    std::string value;
    bool s = db.Get(key, &value);
    if (!s) {
      printf("get %d error\n", i);
      return;
    }
    if (value != key) {
      printf("get %d error\n", i);
      printf("value = %s, key=%s\n", value.c_str(), key);
    }
  }
  printf("beansdb test passed\n");
}

void* StatsThread(void* args) {
  int32_t t = static_cast<int32_t>(time(NULL));
  char name[256];
  snprintf(name, sizeof(name), "%d_%s_%s_%s", t, FLAGS_db, FLAGS_benchmarks, "free.txt");
  FreeMonitor free(name);
  snprintf(name, sizeof(name), "%d_%s_%s_%s", t, FLAGS_db, FLAGS_benchmarks, "memory.txt");
  Monitor memory(name);

  while (g_exit == 0) {
    free.StatFree();
    memory.StatMemory();
    sleep(1);
  }
  return (void*) NULL;

}

int main(int argc, char* argv[]) {
  int n;
  char junk;
  double x;

  for (int i = 1; i < argc; i++) {
    if (Slice(argv[i]).starts_with("--benchmarks=")) {
      FLAGS_benchmarks = argv[i] + strlen("--benchmarks=");
      if (strcmp(FLAGS_benchmarks, "test") == 0) {
        TEST_BEANSDB();
        TEST_FDB();
        return 0;
      }
    } else if(Slice(argv[i]).starts_with("--db=")) {
      FLAGS_db = argv[i] + strlen("--db=");	  
    } else if (sscanf(argv[i], "--value_size=%d%c", &n, &junk) == 1) {
      FLAGS_value_size = n;
    } else if (sscanf(argv[i], "--num=%d%c", &n, &junk) == 1) {
      FLAGS_num = n;
    } else if (sscanf(argv[i], "--reads=%d%c", &n, &junk) == 1) {
      FLAGS_reads = n;
    } else if (sscanf(argv[i], "--histogram=%d%c", &n, &junk) == 1) {
      FLAGS_histogram = (n != 0);
    } else if (sscanf(argv[i], "--num_threads=%d%c", &n, &junk) == 1) {
      FLAGS_num_threads = n;
    } else if (sscanf(argv[i], "--use_existing_db=%d%c", &n, &junk) == 1) {
      FLAGS_use_existing_db =  (n!=0);
    } else if (sscanf(argv[i], "--filesystem_ndir=%d%c", &n, &junk) == 1) {
      FLAGS_filesystem_ndir = n;
    } else if (sscanf(argv[i], "--read_write_ratio=%lf%c", &x, &junk)== 1) {
      FLAGS_read_write_ratio = x;
    } else if (sscanf(argv[i], "--scan_threads=%d%c", &n, &junk) == 1) {
      FLAGS_scan_threads = n;
    } else {
      fprintf(stderr, "Invalid flag %s\n", argv[i]);
      exit(1);
    }
  }

  pthread_t tid;
  pthread_create(&tid, NULL, StatsThread, NULL);
  if (strcmp(FLAGS_db, "beansdb") == 0) {
    DB* db = new Beansdb();
    //boost::scoped_ptr<DB> scoped_db(db);
    scoped_ptr<DB> scoped_db(db);
    Benchmark benchmark(db);
    benchmark.Run();
    g_exit = 1;
    pthread_join(tid, NULL);
  } else if (strcmp(FLAGS_db, "fdb") == 0) {
    DB* db = new FileSystemDB;
    //boost::scoped_ptr<DB> scoped_db(db);
    scoped_ptr<DB> scoped_db(db);
    Benchmark benchmark(db);
    benchmark.Run();
    g_exit = 1;
    pthread_join(tid, NULL);
  } else {
    fprintf(stderr, "FLAGS_db must in ['beansdb', 'fdb']\n");
  }

  return 0;
}

