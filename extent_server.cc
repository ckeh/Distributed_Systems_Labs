// the extent server implementation

#include "extent_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

extent_server::extent_server() {
    VERIFY(pthread_mutex_init(&extents_lock, NULL) == 0);
    extents_map[rootdir] = {};
    int temp;
    put(rootdir, "", temp);
}

extent_server::~extent_server() {
    VERIFY(pthread_mutex_destroy(&extents_lock) == 0);
}


int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
  ScopedLock sl(&extents_lock);

  time_t t;
  time(&t);
  printf("buf: %s\n", (void*)buf.c_str());

  if(extents_map.find(id) != extents_map.end()){
    extents_map[id].second.mtime = t;
    extents_map[id].second.ctime = t;
    extents_map[id].first = buf;
    extents_map[id].second.size = buf.size();

  } else {
    extent_protocol::attr at = {t, t, t, buf.size()};
    extents_map[id] = {buf, at};

  }
  // You fill this in for Lab 2.
  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  ScopedLock sl(&extents_lock);

  if(extents_map.find(id) == extents_map.end()){
    return extent_protocol::NOENT;
  }

  time_t t;
  time(&t);
  std::pair<std::string, extent_protocol::attr>& extent = extents_map[id];
  buf = extent.first;
  extent.second.atime = t;


  // You fill this in for Lab 2.
  return extent_protocol::OK;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  ScopedLock sl(&extents_lock);

  if(extents_map.find(id) == extents_map.end()){
    return extent_protocol::NOENT;
  }

  // You fill this in for Lab 2.
  // You replace this with a real implementation. We send a phony response
  // for now because it's difficult to get FUSE to do anything (including
  // unmount) if getattr fails.
  a = extents_map[id].second;
  return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  ScopedLock sl(&extents_lock);

  if(extents_map.find(id) == extents_map.end()){
    return extent_protocol::NOENT;
  }

  extents_map.erase(id);
  // You fill this in for Lab 2.
  return extent_protocol::OK;
}

