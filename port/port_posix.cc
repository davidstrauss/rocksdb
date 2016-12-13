//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "port/port_posix.h"

#include <assert.h>
#if defined(__i386__) || defined(__x86_64__)
#include <cpuid.h>
#endif
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <fcntl.h>
#include <string.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <unistd.h>
#include <cstdlib>
#include "util/logging.h"
//#include "rocksdb/env.h"

namespace rocksdb {
namespace port {

static int PthreadCall(const char* label, int result) {
  if (result != 0 && result != ETIMEDOUT) {
    fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
    abort();
  }
  return result;
}

Mutex::Mutex(bool adaptive) {
#ifdef ROCKSDB_PTHREAD_ADAPTIVE_MUTEX
  pthread_mutexattr_t mutex_attr;
  char fd_template[] = "/tmp/port_posix_mutex.XXXXXX";
  //int zfd = open("/tmp/port_posix_mutex.XXXXXX", O_RDWR | O_CREAT, 0666);
  //int zfd = open("/dev/zero", O_RDWR);
  int zfd = mkstemp(fd_template);
  posix_fallocate(zfd, 0, sizeof(pthread_mutex_t));
  mu_ = (pthread_mutex_t*) mmap(0, sizeof(pthread_mutex_t), PROT_READ | PROT_WRITE, MAP_SHARED, zfd, 0);
  close(zfd);
  //mu_ = (pthread_mutex_t*) mmap(NULL, sizeof(pthread_mutex_t), PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_SHARED, -1, 0);

  PthreadCall("init mutex attr", pthread_mutexattr_init(&mutex_attr));
  PthreadCall("set mutex pshared",
              pthread_mutexattr_setpshared(&mutex_attr,
                                           PTHREAD_PROCESS_SHARED));
  if (adaptive) {
    PthreadCall("set mutex attr",
                pthread_mutexattr_settype(&mutex_attr,
                                          PTHREAD_MUTEX_ADAPTIVE_NP));
  }

  //int fd = open("/tmp/test_write.XXXXXX", O_RDWR | O_CREAT, 0666);
  //write(fd, "hello\n", 6);
  //close(fd);
  //Log(InfoLogLevel::INFO_LEVEL, db_options_->info_log, "Created process-safe locks.");

  PthreadCall("init mutex", pthread_mutex_init(mu_, &mutex_attr));
  PthreadCall("destroy mutex attr", pthread_mutexattr_destroy(&mutex_attr));
#else
  PthreadCall("init mutex", pthread_mutex_init(mu_, nullptr));
#endif // ROCKSDB_PTHREAD_ADAPTIVE_MUTEX
}

Mutex::~Mutex() { PthreadCall("destroy mutex", pthread_mutex_destroy(mu_)); }

void Mutex::Lock() {
  PthreadCall("lock", pthread_mutex_lock(mu_));
#ifndef NDEBUG
  locked_ = true;
#endif
}

void Mutex::Unlock() {
#ifndef NDEBUG
  locked_ = false;
#endif
  PthreadCall("unlock", pthread_mutex_unlock(mu_));
}

void Mutex::AssertHeld() {
#ifndef NDEBUG
  assert(locked_);
#endif
}

CondVar::CondVar(Mutex* mu)
    : mu_(mu) {

    pthread_condattr_t cvar_attr;

    // Map concurrency primitives to memory accessible across process boundaries.
    //int zfd = open("/tmp/port_posix_cvar.XXXXXX", O_RDWR | O_CREAT, 0666);
    char fd_template[] = "/tmp/port_posix_cvar.XXXXXX";
    //int zfd = open("/dev/zero", O_RDWR);
    int zfd = mkstemp(fd_template);
    posix_fallocate(zfd, 0, sizeof(pthread_cond_t));

    cv_ = (pthread_cond_t*) mmap(0, sizeof(pthread_cond_t), PROT_READ | PROT_WRITE, MAP_SHARED, zfd, 0);
    //cv_ = (pthread_cond_t*) mmap(NULL, sizeof(pthread_cond_t), PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_SHARED, -1, 0);
    close(zfd);

    // Initialize a process-sharable condition variable.
    PthreadCall("init cvar attr", pthread_condattr_init(&cvar_attr));
    PthreadCall("set cvar pshared",
                pthread_condattr_setpshared(&cvar_attr, PTHREAD_PROCESS_SHARED));
    PthreadCall("init cvar", pthread_cond_init(cv_, &cvar_attr));
    PthreadCall("destroy cvar attr", pthread_condattr_destroy(&cvar_attr));


    //PthreadCall("init cv", pthread_cond_init(cv_, nullptr));
}

CondVar::~CondVar() { PthreadCall("destroy cv", pthread_cond_destroy(cv_)); }

void CondVar::Wait() {
#ifndef NDEBUG
  mu_->locked_ = false;
#endif
  PthreadCall("wait", pthread_cond_wait(cv_, mu_->mu_));
#ifndef NDEBUG
  mu_->locked_ = true;
#endif
}

bool CondVar::TimedWait(uint64_t abs_time_us) {
  struct timespec ts;
  ts.tv_sec = static_cast<time_t>(abs_time_us / 1000000);
  ts.tv_nsec = static_cast<suseconds_t>((abs_time_us % 1000000) * 1000);

#ifndef NDEBUG
  mu_->locked_ = false;
#endif
  int err = pthread_cond_timedwait(cv_, mu_->mu_, &ts);
#ifndef NDEBUG
  mu_->locked_ = true;
#endif
  if (err == ETIMEDOUT) {
    return true;
  }
  if (err != 0) {
    PthreadCall("timedwait", err);
  }
  return false;
}

void CondVar::Signal() {
  PthreadCall("signal", pthread_cond_signal(cv_));
}

void CondVar::SignalAll() {
  PthreadCall("broadcast", pthread_cond_broadcast(cv_));
}

RWMutex::RWMutex() {
  PthreadCall("init mutex", pthread_rwlock_init(mu_, nullptr));
}

RWMutex::~RWMutex() { PthreadCall("destroy mutex", pthread_rwlock_destroy(mu_)); }

void RWMutex::ReadLock() { PthreadCall("read lock", pthread_rwlock_rdlock(mu_)); }

void RWMutex::WriteLock() { PthreadCall("write lock", pthread_rwlock_wrlock(mu_)); }

void RWMutex::ReadUnlock() { PthreadCall("read unlock", pthread_rwlock_unlock(mu_)); }

void RWMutex::WriteUnlock() { PthreadCall("write unlock", pthread_rwlock_unlock(mu_)); }

int PhysicalCoreID() {
#if defined(__i386__) || defined(__x86_64__)
  // if you ever find that this function is hot on Linux, you can go from
  // ~200 nanos to ~20 nanos by adding the machinery to use __vdso_getcpu
  unsigned eax, ebx = 0, ecx, edx;
  __get_cpuid(1, &eax, &ebx, &ecx, &edx);
  return ebx >> 24;
#else
  // getcpu or sched_getcpu could work here
  return -1;
#endif
}

void InitOnce(OnceType* once, void (*initializer)()) {
  PthreadCall("once", pthread_once(once, initializer));
}

void Crash(const std::string& srcfile, int srcline) {
  fprintf(stdout, "Crashing at %s:%d\n", srcfile.c_str(), srcline);
  fflush(stdout);
  kill(getpid(), SIGTERM);
}

int GetMaxOpenFiles() {
#if defined(RLIMIT_NOFILE)
  struct rlimit no_files_limit;
  if (getrlimit(RLIMIT_NOFILE, &no_files_limit) != 0) {
    return -1;
  }
  // protect against overflow
  if (no_files_limit.rlim_cur >= std::numeric_limits<int>::max()) {
    return std::numeric_limits<int>::max();
  }
  return static_cast<int>(no_files_limit.rlim_cur);
#endif
  return -1;
}

}  // namespace port
}  // namespace rocksdb
