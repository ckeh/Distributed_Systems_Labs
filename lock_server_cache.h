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
  int nacquire;
  std::map<lock_protocol::lockid_t, std::string> lock_to_owner;
  std::map<lock_protocol::lockid_t, std::deque<std::string>> lock_to_waitlist;
  pthread_mutex_t maplock;
  pthread_cond_t torevoke;
 public:
  lock_server_cache();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  int acquire(lock_protocol::lockid_t, std::string id, int &);
  int release(lock_protocol::lockid_t, std::string id, int &);
};

#endif
