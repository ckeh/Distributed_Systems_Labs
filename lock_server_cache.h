#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>
#include <set>
#include <map>
#include <queue>

#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"


class lock_server_cache {
 private:
  class slock_info {
    public:
      std::string owner;
      std::deque<std::string> waitlist;
      pthread_mutex_t lock;
      volatile bool revoked = false;
  };
  int nacquire;
  std::map<lock_protocol::lockid_t, slock_info> slocks;
  pthread_mutex_t maplock;
 public:
  lock_server_cache();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  int acquire(lock_protocol::lockid_t, std::string id, int &);
  int release(lock_protocol::lockid_t, std::string id, int &);
};

#endif
