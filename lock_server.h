// this is the lock server
// the lock client has a similar interface

#ifndef lock_server_h
#define lock_server_h

#include <string>
#include "lock_protocol.h"
#include "lock_client.h"
#include "rpc.h"

class lock_server {

 protected:
  int nacquire;
  std::map<lock_protocol::lockid_t, bool> lid_to_locked;
  pthread_cond_t cv;

 public:
  lock_server();
  ~lock_server() {};
  lock_protocol::status stat(int clt, lock_protocol::lockid_t lid, int &);
  virtual lock_protocol::status acquire(lock_protocol::lockid_t, int &);
  virtual lock_protocol::status release(lock_protocol::lockid_t, int &);
};

#endif 







