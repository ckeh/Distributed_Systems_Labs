// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <algorithm>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"


lock_server_cache::lock_server_cache()
{
  VERIFY(pthread_mutex_init(&maplock, NULL) == 0);
  VERIFY(pthread_cond_init(&torevoke, NULL) == 0);
}


int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, 
                               int &)
{
  static volatile bool revoked = false;

  pthread_mutex_lock(&maplock);
  tprintf("ckeh:lock_server_cache::aquire: client:%s, thread::%ld lid:%lld, revoked:%d\n", id.c_str(), pthread_self(), lid, revoked);

  lock_protocol::status ret = lock_protocol::OK;
  if(lock_to_owner.find(lid) == lock_to_owner.end()){
    //entry not found so is free
    std::deque<std::string> cp = lock_to_waitlist[lid];
    for(std::string s : cp){
      tprintf("\tckeh:lock_server_cache::aquire WAITLIST: %s\n", s.c_str());
    }

    if(lock_to_waitlist[lid].front().compare(id) == 0){
      tprintf("\tckeh:lock_server_cache::aquire: POPPED WAITLIST client:%s, thread::%ld lid:%lld, waitlist_size:%ld\n", id.c_str(), pthread_self(), lid, lock_to_waitlist[lid].size());
      lock_to_waitlist[lid].pop_front();
    }
    lock_to_owner[lid] = id;
    if(lock_to_waitlist[lid].size() > 0){
      revoked = true;
      handle h(lock_to_owner[lid]);
      rpcc* cl = h.safebind();
      VERIFY(cl);
      int r;
      pthread_mutex_unlock(&maplock);
      ret = cl->call(rlock_protocol::revoke, lid, r);
      
    } else {
      revoked = false;
      pthread_mutex_unlock(&maplock);
    }
    tprintf("\tckeh:lock_server_cache::aquire: OWNER client:%s, thread::%ld lid:%lld\n", id.c_str(), pthread_self(), lid);
  } else {
    tprintf("\tckeh:lock_server_cache::aquire: WAITING AND REVOKE client:%s, thread::%ld lid:%lld\n", id.c_str(), pthread_self(), lid);
    ret = lock_protocol::RETRY;
    // if(std::find(lock_to_waitlist[lid].begin(), lock_to_waitlist[lid].end(), id) == lock_to_waitlist[lid].end()){
      lock_to_waitlist[lid].push_back(id);
    // }
    //found entry so another thread owns it 
    if(!revoked){
      handle h(lock_to_owner[lid]);
      revoked = true;
      rpcc* cl = h.safebind();

      VERIFY(cl);
      int r, retr;
      pthread_mutex_unlock(&maplock);
      retr = cl->call(rlock_protocol::revoke, lid, r);
      VERIFY(retr == lock_protocol::OK);
      
    } else {
      pthread_mutex_unlock(&maplock);
    }
  }
  tprintf("\tckeh:lock_server_cache::aquire: RET client:%s, thread::%ld lid:%lld, ret:%d\n", id.c_str(), pthread_self(), lid, ret);
  return ret;
}

int 
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id, 
         int &r)
{
  pthread_mutex_lock(&maplock);
  // ScopedLock sl(&maplock);

  tprintf("ckeh:lock_server_cache::release: client:%s, thread::%ld lid:%lld\n", id.c_str(), pthread_self(), lid);

  lock_protocol::status ret = lock_protocol::OK;
  if(lock_to_owner.find(lid) == lock_to_owner.end()){
    ret = lock_protocol::NOENT;
    pthread_mutex_unlock(&maplock);
  } else {
    tprintf("\tckeh:lock_server_cache::release: RELEASING client:%s, thread::%ld lid:%lld\n", id.c_str(), pthread_self(), lid);

    lock_to_owner.erase(lid);
    if(lock_to_waitlist[lid].size() == 0){
      return ret;
    }

    std::string cur_retry = lock_to_waitlist[lid].front();
    handle h(cur_retry);
    rpcc* cl = h.safebind();

    VERIFY(cl);
    int r;
    tprintf("\t\tckeh:lock_server_cache::release: CALLING RETRY FOR %s, client:%s, thread::%ld\n", lock_to_waitlist[lid].front().c_str(), id.c_str(), pthread_self());
    pthread_mutex_unlock(&maplock);


    while((ret = cl->call(rlock_protocol::retry, lid, r)) == rlock_protocol::SKIP){

      pthread_mutex_lock(&maplock);
      if(lock_to_waitlist[lid].front() == cur_retry){
        lock_to_waitlist[lid].pop_front();
      }
      
      tprintf("\t\tckeh:lock_server_cache::release: RECALLING RETRY FOR %s, client:%s, thread::%ld\n", lock_to_waitlist[lid].front().c_str(), id.c_str(), pthread_self());
      if(lock_to_waitlist[lid].size() == 0){
        ret = lock_protocol::OK;
        break;
      }
      cur_retry = lock_to_waitlist[lid].front();

      h = handle(cur_retry);
      cl = h.safebind();
      VERIFY(cl);
      pthread_mutex_unlock(&maplock);
    }
  }

  tprintf("ckeh:lock_server_cache::release: RETURNING client:%s, thread::%ld lid:%lld\n", id.c_str(), pthread_self(), lid);

  return ret;
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  ScopedLock sl(&maplock);

  tprintf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}

