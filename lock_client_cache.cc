// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"


lock_client_cache::lock_client_cache(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
  rpcs *rlsrpc = new rpcs(0);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);

  const char *hname;
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlsrpc->port();
  id = host.str();
  VERIFY(pthread_mutex_init(&state_lock, NULL) == 0);
  // VERIFY(pthread_cond_init(&retry, NULL) == 0);
  // VERIFY(pthread_mutex_init(&lock, NULL) == 0);
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
  pthread_mutex_lock(&state_lock);
  if(locks.find(lid) == locks.end()){
    pthread_mutex_init(&locks[lid].lock, NULL);
    pthread_cond_init(&locks[lid].retry, NULL);
  }
  tprintf("ckeh:lock_client_cache::acquire client:%s thread:%ld, lid:%lld, locks[lid]:%d\n", id.c_str(), pthread_self(), lid, locks[lid].lock_state);
  lock_info &cur = locks[lid];
  pthread_mutex_lock(&cur.lock);
  pthread_mutex_unlock(&state_lock);
  int ret = lock_protocol::OK;

retry:
  switch(cur.lock_state){
    case NONE:
      cur.lock_state = ACQUIRING;
      int r;
      pthread_mutex_unlock(&cur.lock);
      if((ret = cl->call(lock_protocol::acquire, lid, id, r)) != lock_protocol::OK){
        pthread_mutex_lock(&cur.lock);
        cur.waiting_replay++;
        while(cur.lock_state == ACQUIRING){
          pthread_cond_wait(&cur.retry, &cur.lock);
        }
        cur.waiting_replay--;
        goto retry;
      }
      pthread_mutex_lock(&cur.lock);
      cur.lock_state = LOCKED;
      break;
    case RELEASING:
      pthread_mutex_unlock(&cur.lock);
      // put yourself back onto waitlist
      if((ret = cl->call(lock_protocol::acquire, lid, id, r)) != lock_protocol::OK){
        pthread_mutex_lock(&cur.lock);
        cur.waiting_replay++;
        while(cur.lock_state == RELEASING){
          pthread_cond_wait(&cur.retry, &cur.lock);
        }
        cur.waiting_replay--;
        goto retry;
      }
      pthread_mutex_lock(&cur.lock);
      cur.lock_state = LOCKED;

      break;

    case FREE:
      cur.lock_state = LOCKED;
      break;
    case LOCKED:
    case ACQUIRING:
      cur.waiting++;
      while(cur.lock_state != FREE){
        pthread_cond_wait(&cur.retry, &cur.lock);
      }
      cur.waiting--;
      goto retry;
      break;
    default:
      break;
  }
  tprintf("ckeh:lock_client_cache::acquire RETURNING client:%s thread:%ld, lid:%lld, locks[lid]:%d\n", id.c_str(), pthread_self(), lid, locks[lid].lock_state);

  pthread_mutex_unlock(&cur.lock);
  return lock_protocol::OK;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{

  pthread_mutex_lock(&state_lock);
  tprintf("ckeh:lock_client_cache::release client:%s thread:%ld, lid:%lld, locks[lid]:%d\n", id.c_str(), pthread_self(), lid, locks[lid].lock_state);
  lock_info &cur = locks[lid];
  pthread_mutex_lock(&cur.lock);
  pthread_mutex_unlock(&state_lock);

  switch(cur.lock_state){
    // case NONE:
    //   break;
    // case FREE:
    //   break;
    case LOCKED:
      if(cur.revoke_requested && cur.waiting == 0 && cur.waiting_replay == 0){
        tprintf("\tckeh:lock_client_cache::release DELAYED REVOKE client:%s thread:%ld, lid:%lld, locks[lid]:%d\n", id.c_str(), pthread_self(), lid, locks[lid].lock_state);

        cur.revoke_requested = false;
        cur.lock_state = RELEASING;
        int r;
        pthread_mutex_unlock(&cur.lock);
        lock_protocol::status retr = cl->call(lock_protocol::release, lid, id, r);
        pthread_mutex_lock(&cur.lock);

        if(retr == lock_protocol::OK && cur.lock_state == RELEASING){
          cur.lock_state = NONE;
        }
      } else {
        tprintf("\tckeh:lock_client_cache::release BROADCASTING client:%s thread:%ld, lid:%lld, locks[lid]:%d\n", id.c_str(), pthread_self(), lid, locks[lid].lock_state);
        cur.lock_state = FREE;
        // pthread_cond_broadcast(&owned);
        pthread_cond_broadcast(&cur.retry);        
      }
      break;
    // case ACQUIRING:
    //   break;
    // case RELEASING:
    //   break;
    default:
      tprintf("\tckeh:lock_client_cache::release BAD STATE: client:%s thread:%ld, lid:%lld, locks[lid]:%d\n", id.c_str(), pthread_self(), lid, locks[lid].lock_state);
      break;

  }

  pthread_mutex_unlock(&cur.lock);
  tprintf("\tckeh:lock_client_cache::release RETURNING: client:%s thread:%ld, lid:%lld, locks[lid]:%d\n", id.c_str(), pthread_self(), lid, locks[lid].lock_state);

  return lock_protocol::OK;
}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, 
                                  int &)
{
  pthread_mutex_lock(&state_lock);

  tprintf("ckeh:lock_client_cache::revoke_handler client:%s thread:%ld, lid:%lld, locks[lid]:%d\n", id.c_str(), pthread_self(), lid, locks[lid].lock_state);
  lock_info &cur = locks[lid];
  pthread_mutex_lock(&cur.lock);
  pthread_mutex_unlock(&state_lock);

  int ret = rlock_protocol::OK;
  if(cur.lock_state != FREE || cur.waiting > 0 || cur.waiting_replay > 0){
    tprintf("\tckeh:lock_client_cache::revoke_handler FLAG REVOKE client:%s thread:%ld, lid:%lld, locks[lid]:%d\n", id.c_str(), pthread_self(), lid, locks[lid].lock_state);

    cur.revoke_requested = true;
  } else {
    tprintf("\tckeh:lock_client_cache::revoke_handler FREE TO RELEASE client:%s thread:%ld, lid:%lld, locks[lid]:%d\n", id.c_str(), pthread_self(), lid, locks[lid].lock_state);

    int r;
    cur.lock_state = RELEASING;
    pthread_mutex_unlock(&cur.lock);
    lock_protocol::status retr = cl->call(lock_protocol::release, lid, id, r);
    pthread_mutex_lock(&cur.lock);

    tprintf("\t\tckeh:lock_client_cache::revoke_handler RETURNED FROM RELEASE client:%s thread:%ld, lid:%lld, locks[lid]:%d, retr:%d\n", id.c_str(), pthread_self(), lid, locks[lid].lock_state, retr);

    VERIFY (retr == lock_protocol::OK);
    cur.lock_state = (cur.lock_state == RELEASING)? NONE : cur.lock_state;

  }
  pthread_mutex_unlock(&cur.lock);
  tprintf("\tckeh:lock_client_cache::revoke_handler RETURNING client:%s thread:%ld, lid:%lld, locks[lid]:%d\n", id.c_str(), pthread_self(), lid, locks[lid].lock_state);

  return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, 
                                 int &)
{
  pthread_mutex_lock(&state_lock);
  tprintf("ckeh:lock_client_cache::retry_handler: client:%s thread:%ld locks[lid]:%d\n", id.c_str(), pthread_self(), locks[lid].lock_state);

  lock_info &cur = locks[lid];
  pthread_mutex_lock(&cur.lock);
  pthread_mutex_unlock(&state_lock);

  int ret = rlock_protocol::OK;
  if(cur.lock_state == ACQUIRING){
    cur.lock_state = NONE;
    pthread_cond_broadcast(&cur.retry);
    // pthread_cond_broadcast(&owned);
    pthread_mutex_unlock(&cur.lock);
    return ret;
  }
  tprintf("ckeh:lock_client_cache::retry_handler: NUMBER OF THREADS waiting:%d, waiting_replay:%d, client:%s thread:%ld locks[lid]:%d\n", cur.waiting, cur.waiting_replay, id.c_str(), pthread_self(), locks[lid].lock_state);
  if(cur.waiting == 0 && cur.waiting_replay == 0){
    cur.lock_state = NONE;
    pthread_mutex_unlock(&cur.lock);
    return rlock_protocol::SKIP;
  }
  cur.lock_state = NONE;
  pthread_cond_broadcast(&cur.retry);
  // pthread_cond_broadcast(&owned);
  pthread_mutex_unlock(&cur.lock);

  tprintf("ckeh:lock_client_cache::retry_handler: RETURNING client:%s thread:%ld locks[lid]:%d\n", id.c_str(), pthread_self(), locks[lid].lock_state);

  return ret;
}



int
lock_client_cache::stat(lock_protocol::lockid_t lid)
{
  int r;
  lock_protocol::status ret = cl->call(lock_protocol::stat, cl->id(), lid, r);
  VERIFY (ret == lock_protocol::OK);
  return r;
}