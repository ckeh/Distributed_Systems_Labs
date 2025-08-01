// this is the extent server

#ifndef extent_server_h
#define extent_server_h

#include <string>
#include <map>
#include "extent_protocol.h"

class extent_server {
 private:
   pthread_mutex_t extents_lock;
   extent_protocol::extentid_t rootdir = 0x00000001;
   std::map<extent_protocol::extentid_t, std::pair<std::string, extent_protocol::attr>> extents_map;

 public:

  extent_server();
  ~extent_server();

  int put(extent_protocol::extentid_t id, std::string, int &);
  int get(extent_protocol::extentid_t id, std::string &);
  int getattr(extent_protocol::extentid_t id, extent_protocol::attr &);
  int remove(extent_protocol::extentid_t id, int &);
};

#endif 







