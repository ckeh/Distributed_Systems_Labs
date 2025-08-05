#ifndef yfs_client_h
#define yfs_client_h

#include <string>
//#include "yfs_protocol.h"
#include "extent_client.h"
#include <vector>

#include "lock_protocol.h"
#include "lock_client.h"

class yfs_client {
  extent_client *ec;
  lock_client* lc;
 public:

  typedef unsigned long long inum;

  enum xxstatus { OK, RPCERR, NOENT, IOERR, EXIST };
  typedef int status;

  struct fileinfo {
    unsigned long long size;
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirinfo {
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirent {
    std::string name;
    yfs_client::inum inum;
  };

 private:
  static std::string filename(inum);
  static inum n2i(std::string);
  static inum markinum(inum, bool);

 public:

  yfs_client(std::string, std::string);

  bool isfile(inum);
  bool isdir(inum);

  int setsize(inum inum, off_t newsize);
  int getfile(inum, fileinfo &);
  int getdir(inum, dirinfo &);
  int create(inum, std::string, inum &, bool);
  int lookup(inum, std::string, inum &);
  int unlink(inum, std::string);
  int readdir(inum parent, std::vector<dirent>& );
  int readfile(inum file, off_t offset, size_t size, std::string& buf);
  int writefile(inum file, off_t offset, size_t size, std::string buf);

};

#endif 
