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
  VERIFY(pthread_cond_init(&retry, NULL) == 0);
  VERIFY(pthread_cond_init(&owned, NULL) == 0);
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
  pthread_mutex_lock(&state_lock);
  tprintf("ckeh:lock_client_cache::acquire client:%s thread:%ld, lid:%lld, lock_states[lid]:%d\n", id.c_str(), pthread_self(), lid, lock_states[lid]);

  int ret = lock_protocol::OK;
retry:
  switch(lock_states[lid]){
    case NONE:
      lock_states[lid] = ACQUIRING;
      int r;
      pthread_mutex_unlock(&state_lock);
      if((ret = cl->call(lock_protocol::acquire, lid, id, r)) != lock_protocol::OK){
        pthread_mutex_lock(&state_lock);
        waiting_replay++;
        while(lock_states[lid] != NONE){
          pthread_cond_wait(&retry, &state_lock);
        }
        waiting_replay--;
        goto retry;
      }
      pthread_mutex_lock(&state_lock);
      lock_states[lid] = LOCKED;
      break;
    case FREE:
      lock_states[lid] = LOCKED;
      break;
    case LOCKED:
      waiting++;
      while(lock_states[lid] != FREE){
        pthread_cond_wait(&owned, &state_lock);
      }
      waiting--;
      goto retry;
      break;
    case ACQUIRING:
      waiting++;
      while(lock_states[lid] != FREE){
        pthread_cond_wait(&owned, &state_lock);
      }
      waiting--;
      goto retry;
      break;
    case RELEASING:
      pthread_mutex_unlock(&state_lock);
      // put yourself back onto waitlist
      if((ret = cl->call(lock_protocol::acquire, lid, id, r)) != lock_protocol::OK){
        pthread_mutex_lock(&state_lock);
        waiting_replay++;
        while(lock_states[lid] != NONE){
          pthread_cond_wait(&retry, &state_lock);
        }
        waiting_replay--;
      }
      goto retry;
      break;
    default:
      break;
  }
  tprintf("ckeh:lock_client_cache::acquire RETURNING client:%s thread:%ld, lid:%lld, lock_states[lid]:%d\n", id.c_str(), pthread_self(), lid, lock_states[lid]);

  pthread_mutex_unlock(&state_lock);
  return lock_protocol::OK;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{

  pthread_mutex_lock(&state_lock);
  tprintf("ckeh:lock_client_cache::release client:%s thread:%ld, lid:%lld, lock_states[lid]:%d\n", id.c_str(), pthread_self(), lid, lock_states[lid]);
  switch(lock_states[lid]){
    // case NONE:
    //   break;
    // case FREE:
    //   break;
    case LOCKED:
      if(revoke_requested && waiting == 0 && waiting_replay == 0){
        tprintf("\tckeh:lock_client_cache::release DELAYED REVOKE client:%s thread:%ld, lid:%lld, lock_states[lid]:%d\n", id.c_str(), pthread_self(), lid, lock_states[lid]);

        revoke_requested = false;
        lock_states[lid] = RELEASING;
        int r;
        pthread_mutex_unlock(&state_lock);
        lock_protocol::status retr = cl->call(lock_protocol::release, lid, id, r);
        pthread_mutex_lock(&state_lock);

        if(retr == lock_protocol::OK){
          lock_states[lid] = NONE;
        }
      } else {
        tprintf("\tckeh:lock_client_cache::release BROADCASTING client:%s thread:%ld, lid:%lld, lock_states[lid]:%d\n", id.c_str(), pthread_self(), lid, lock_states[lid]);
        lock_states[lid] = FREE;
        pthread_cond_broadcast(&owned);
        pthread_cond_broadcast(&retry);        
      }
      break;
    // case ACQUIRING:
    //   break;
    // case RELEASING:
    //   break;
    default:
      tprintf("\tckeh:lock_client_cache::release BAD STATE: client:%s thread:%ld, lid:%lld, lock_states[lid]:%d\n", id.c_str(), pthread_self(), lid, lock_states[lid]);
      break;

  }

  pthread_mutex_unlock(&state_lock);
  tprintf("\tckeh:lock_client_cache::release RETURNING: client:%s thread:%ld, lid:%lld, lock_states[lid]:%d\n", id.c_str(), pthread_self(), lid, lock_states[lid]);

  return lock_protocol::OK;
}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, 
                                  int &)
{
  pthread_mutex_lock(&state_lock);

  tprintf("ckeh:lock_client_cache::revoke_handler client:%s thread:%ld, lid:%lld, lock_states[lid]:%d\n", id.c_str(), pthread_self(), lid, lock_states[lid]);
  int ret = rlock_protocol::OK;
  if(lock_states[lid] != FREE){
    tprintf("\tckeh:lock_client_cache::revoke_handler FLAG REVOKE client:%s thread:%ld, lid:%lld, lock_states[lid]:%d\n", id.c_str(), pthread_self(), lid, lock_states[lid]);

    revoke_requested = true;
  } else {
    tprintf("\tckeh:lock_client_cache::revoke_handler FREE TO RELEASE client:%s thread:%ld, lid:%lld, lock_states[lid]:%d\n", id.c_str(), pthread_self(), lid, lock_states[lid]);

    int r;
    lock_states[lid] = RELEASING;
    pthread_mutex_unlock(&state_lock);
    lock_protocol::status retr = cl->call(lock_protocol::release, lid, id, r);
    pthread_mutex_lock(&state_lock);

    tprintf("\t\tckeh:lock_client_cache::revoke_handler RETURNED FROM RELEASE client:%s thread:%ld, lid:%lld, lock_states[lid]:%d, retr:%d\n", id.c_str(), pthread_self(), lid, lock_states[lid], retr);

    VERIFY (retr == lock_protocol::OK);
    if(retr == lock_protocol::OK){
      lock_states[lid] = NONE;
    }
  }
  pthread_mutex_unlock(&state_lock);
  tprintf("\tckeh:lock_client_cache::revoke_handler RETURNING client:%s thread:%ld, lid:%lld, lock_states[lid]:%d\n", id.c_str(), pthread_self(), lid, lock_states[lid]);

  return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, 
                                 int &)
{
  tprintf("ckeh:lock_client_cache::retry_handler: client:%s thread:%ld lock_states[lid]:%d\n", id.c_str(), pthread_self(), lock_states[lid]);
  pthread_mutex_lock(&state_lock);

  int ret = rlock_protocol::OK;
  // int r;
  // lock_states[lid] = ACQUIRING;
  // pthread_mutex_unlock(&state_lock);
  // lock_protocol::status retaq = cl->call(lock_protocol::acquire, lid, id, r);
  // pthread_mutex_lock(&state_lock);
  // if(retaq == lock_protocol::OK){
    // lock_states[lid] = FREE;
    // pthread_cond_broadcast(&owned);
    if(lock_states[lid] == ACQUIRING){
      lock_states[lid] = NONE;
      pthread_cond_broadcast(&retry);
      // pthread_cond_broadcast(&owned);

      pthread_mutex_unlock(&state_lock);
      return ret;
    }
    tprintf("ckeh:lock_client_cache::retry_handler: NUMBER OF THREADS waiting:%d, waiting_replay:%d, client:%s thread:%ld lock_states[lid]:%d\n", waiting, waiting_replay, id.c_str(), pthread_self(), lock_states[lid]);
    if(waiting == 0 && waiting_replay == 0){
      lock_states[lid] = NONE;
      pthread_mutex_unlock(&state_lock);
      return rlock_protocol::SKIP;
    }
    lock_states[lid] = NONE;
    pthread_cond_broadcast(&retry);
    pthread_cond_broadcast(&owned);
  // } else if(retaq == lock_protocol::RETRY){
    // shouldnt happen
  // }
  pthread_mutex_unlock(&state_lock);

  tprintf("ckeh:lock_client_cache::retry_handler: RETURNING client:%s thread:%ld lock_states[lid]:%d\n", id.c_str(), pthread_self(), lock_states[lid]);

  return ret;
}



