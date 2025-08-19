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
}


int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, 
                               int &)
{
  pthread_mutex_lock(&maplock);
  if(slocks.find(lid) == slocks.end()){
    VERIFY(pthread_mutex_init(&slocks[lid].lock, NULL) == 0);
  }
  slock_info& cur = slocks[lid];
  pthread_mutex_lock(&cur.lock);
  pthread_mutex_unlock(&maplock);

  tprintf("ckeh:lock_server_cache::aquire: client:%s, thread::%ld lid:%lld, revoked:%d\n", id.c_str(), pthread_self(), lid, cur.revoked);

  lock_protocol::status ret = lock_protocol::OK;
  if(cur.owner.compare("") == 0){
    //entry not found so is free
    std::deque<std::string> cp = cur.waitlist;
    for(std::string s : cp){
      tprintf("\tckeh:lock_server_cache::aquire WAITLIST: %s\n", s.c_str());
    }

    if(cur.waitlist.front().compare(id) == 0){
      tprintf("\tckeh:lock_server_cache::aquire: POPPED WAITLIST client:%s, thread::%ld lid:%lld, waitlist_size:%ld\n", id.c_str(), pthread_self(), lid, cur.waitlist.size());
      cur.waitlist.pop_front();
    }
    cur.owner = id;
    if(cur.waitlist.size() > 0){
      cur.revoked = true;
      handle h(cur.owner);
      pthread_mutex_unlock(&cur.lock);

      rpcc* cl = h.safebind();
      VERIFY(cl);
      int r;
      ret = cl->call(rlock_protocol::revoke, lid, r);
      
    } else {
      cur.revoked = false;
      pthread_mutex_unlock(&cur.lock);
    }
    tprintf("\tckeh:lock_server_cache::aquire: OWNER client:%s, thread::%ld lid:%lld\n", id.c_str(), pthread_self(), lid);
  } else {
    tprintf("\tckeh:lock_server_cache::aquire: WAITING AND REVOKE client:%s, thread::%ld lid:%lld\n", id.c_str(), pthread_self(), lid);
    ret = lock_protocol::RETRY;
    cur.waitlist.push_back(id);

    //found entry so another thread owns it 
    if(!cur.revoked){
      handle h(cur.owner);
      cur.revoked = true;
      pthread_mutex_unlock(&cur.lock);

      rpcc* cl = h.safebind();

      VERIFY(cl);
      int r, retr;
      retr = cl->call(rlock_protocol::revoke, lid, r);
      VERIFY(retr == lock_protocol::OK);
      
    } else {
      pthread_mutex_unlock(&cur.lock);
    }
  }
  tprintf("\tckeh:lock_server_cache::aquire: RETURING client:%s, thread::%ld lid:%lld, ret:%d\n", id.c_str(), pthread_self(), lid, ret);
  return ret;
}

int 
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id, 
         int &r)
{
  pthread_mutex_lock(&maplock);
  // ScopedLock sl(&maplock);

  tprintf("ckeh:lock_server_cache::release: client:%s, thread::%ld lid:%lld\n", id.c_str(), pthread_self(), lid);
  slock_info& cur = slocks[lid];
  pthread_mutex_lock(&cur.lock);
  pthread_mutex_unlock(&maplock);

  lock_protocol::status ret = lock_protocol::OK;
  if(cur.owner.compare("") == 0){
    ret = lock_protocol::NOENT;
    pthread_mutex_unlock(&cur.lock);
  } else {
    tprintf("\tckeh:lock_server_cache::release: RELEASING client:%s, thread::%ld lid:%lld\n", id.c_str(), pthread_self(), lid);
    cur.owner = "";
    if(cur.waitlist.size() == 0){
      pthread_mutex_unlock(&cur.lock);
      return ret;
    }

    std::string cur_retry = cur.waitlist.front();
    handle h(cur_retry);
    pthread_mutex_unlock(&cur.lock);

    rpcc* cl = h.safebind();

    VERIFY(cl);
    int r;
    tprintf("\t\tckeh:lock_server_cache::release: CALLING RETRY FOR %s, client:%s, thread::%ld\n", cur.waitlist.front().c_str(), id.c_str(), pthread_self());


    while((ret = cl->call(rlock_protocol::retry, lid, r)) == rlock_protocol::SKIP){

      pthread_mutex_lock(&cur.lock);
      if(cur.waitlist.front() == cur_retry){
        cur.waitlist.pop_front();
      }
      
      tprintf("\t\tckeh:lock_server_cache::release: RECALLING RETRY FOR %s, client:%s, thread::%ld\n", cur.waitlist.front().c_str(), id.c_str(), pthread_self());
      if(cur.waitlist.size() == 0){
        ret = lock_protocol::OK;
        pthread_mutex_unlock(&cur.lock);
        break;
      }
      cur_retry = cur.waitlist.front();

      h = handle(cur_retry);
      pthread_mutex_unlock(&cur.lock);

      cl = h.safebind();
      VERIFY(cl);
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

