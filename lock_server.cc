// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
pthread_mutex_t map_lock;

lock_server::lock_server():
  nacquire (0)
{
  pthread_mutexattr_t attr;
  pthread_mutexattr_init(&attr);
  // pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);

  VERIFY(pthread_mutex_init(&map_lock, &attr) == 0);
  VERIFY(pthread_cond_init(&cv, NULL) == 0);
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}
lock_protocol::status lock_server::acquire(lock_protocol::lockid_t lid, int &r){
  printf("ckeh:acquire:pid:%d, GRABBING MAP LOCK INUM LOCK %lld\n", getpid(), lid);
  pthread_mutex_lock(&map_lock);
  while(lid_to_locked[lid]){
    printf("ckeh:acquire:pid:%d, SLEEPING INUM LOCK %lld\n", getpid(), lid);

    pthread_cond_wait(&cv, &map_lock);
    printf("ckeh:acquire:pid:%d, WAKING UP INUM LOCK %lld\n", getpid(), lid);

  }
  
  lid_to_locked[lid] = true;
  pthread_mutex_unlock(&map_lock);
  printf("ckeh:acquire:pid:%d, UNLOCKING AND AQUIRED INUM LOCK %lld\n", getpid(), lid);

  return lock_protocol::xxstatus::OK;
}
lock_protocol::status lock_server::release(lock_protocol::lockid_t lid, int &r){
    printf("ckeh:release:pid:%d, GRABBING MAP LOCK INUM LOCK %lld\n", getpid(), lid);

  // ScopedLock sl(&map_lock);
  pthread_mutex_lock(&map_lock);

  // bool* locked = &lid_to_locked[lid];
  if(lid_to_locked[lid]){
    lid_to_locked[lid] = false;
    printf("ckeh:release:pid:%d, BROADCASTING MAP LOCK INUM LOCK %lld\n", getpid(), lid);

    pthread_cond_broadcast(&cv);
  } else {
    printf("Released without owning\n");
  }
  printf("ckeh:release:pid:%d, RELEASING MAP LOCK INUM LOCK %lld\n", getpid(), lid);

  pthread_mutex_unlock(&map_lock);

  return lock_protocol::xxstatus::OK;

}



