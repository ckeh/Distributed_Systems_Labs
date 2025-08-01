// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  ec = new extent_client(extent_dst);

}

yfs_client::inum
yfs_client::n2i(std::string n)
{
  std::istringstream ist(n);
  unsigned long long finum;
  ist >> finum;
  return finum;
}

std::string
yfs_client::filename(inum inum)
{
  std::ostringstream ost;
  ost << inum;
  return ost.str();
}

yfs_client::inum
yfs_client::markFile(inum inum){
  return inum | (1ULL << 31U);
}
yfs_client::inum
yfs_client::markDir(inum inum){
  return inum & ~(1ULL << 31U);
}
bool
yfs_client::isfile(inum inum)
{
  if(inum & 0x80000000)
    return true;
  return false;
}

bool
yfs_client::isdir(inum inum)
{
  return ! isfile(inum);
}

int
yfs_client::setsize(inum inum, off_t newsize)
{
  std::string buf;
  int ret;
  if((ret = ec->get(inum, buf)) != OK){
    return ret; 
  }
  off_t cur = sizeof(buf);
  if(cur < newsize){
    ec->put(inum, buf + std::string('\0', newsize-cur));
  } else if(cur > newsize){
    ec->put(inum, buf.substr(0, newsize));
  }
  return OK;
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
  int r = OK;
  extent_protocol::status s;
  printf("getfile %016llx\n", inum);
  extent_protocol::attr a;
  if ((s = ec->getattr(inum, a)) != extent_protocol::OK) {
    switch(s){
      case extent_protocol::IOERR:
        r = IOERR;
        break;
      case extent_protocol::NOENT:
        r = NOENT;
        break;
      default:
        printf("yfs_client::getfile: unrecognized return for getattr %d\n", s);
        r = NOENT;
    }
    goto release;
  }

  fin.atime = a.atime;
  fin.mtime = a.mtime;
  fin.ctime = a.ctime;
  fin.size = a.size;
  printf("getfile %016llx -> sz %llu\n", inum, fin.size);

 release:

  return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
  int r = OK;
  extent_protocol::status s;
  printf("getdir %016llx\n", inum);
  extent_protocol::attr a;
  if ((s = ec->getattr(inum, a)) != extent_protocol::OK) {
    switch(s){
      case extent_protocol::IOERR:
        r = IOERR;
        break;
      case extent_protocol::NOENT:
        r = NOENT;
        break;
      default:
        printf("yfs_client::getfile: unrecognized return for getattr %d\n", s);
        r = NOENT;
    }
    goto release;
  }
  din.atime = a.atime;
  din.mtime = a.mtime;
  din.ctime = a.ctime;

 release:
  return r;
}


int 
yfs_client::create(inum parent, std::string name, inum& new_file){
  inum newinum = markFile(rand());
  std::string buf;
  // get a free inum
  while(ec->get(newinum, buf) == extent_protocol::OK){
    newinum = markFile(rand());
  }
  printf("ckeh:yfs_client::create: newinum:%lld\n", newinum);
  // put newinum
  int ret = OK;
  if((ret = ec->put(newinum, "")) != extent_protocol::OK){
    printf("ckeh:yfs_client::create: error:%d\n", ret);
    return ret;
  }
  ret = ec->get(parent, buf);
  printf("ckeh:yfs_client::create: parent data:%s\n", buf.c_str());

  buf += filename(newinum) + " " + name + " " ;
  if((ret = ec->put(parent, buf)) != extent_protocol::OK){
    printf("ckeh:yfs_client::create: error:%d\n", ret);

    return ret;
  }
  new_file = newinum;
  
  return OK;
}

int 
yfs_client::lookup(inum parent, std::string name, inum& inum){
  printf("ckeh:yfs_client::lookup: parent:%lld, name:%s\n", parent, name.c_str());

  std::string data;
  extent_protocol::status s = ec->get(parent, data);
  printf("ckeh:yfs_client::lookup: data:%s\n", data.c_str());

  if(s == extent_protocol::OK){
    std::stringstream ss(data);
    struct dirent ent;

    while(ss >> ent.inum >> ent.name){
      printf("ckeh:yfs_client::lookup: \tinum:%lld, name:%s\n", ent.inum, ent.name.c_str());
      if(ent.name.compare(name) == 0){
        inum = ent.inum;
        return OK;
      }
    }
  }
  return NOENT;
}


int 
yfs_client::readdir(inum parent, std::vector<dirent>& dirlist){
  std::string data;
  extent_protocol::status s = ec->get(parent, data);
  if(s == extent_protocol::OK){
    std::stringstream ss(data);
    dirent ent;

    while(ss >> ent.inum >> ent.name){
      printf("ckeh:yfs_client::readdir: inum:%lld, name:%s\n", ent.inum, ent.name.c_str());
      dirlist.push_back(ent);
    }
    return OK;
  } 

  return s;
}

int
yfs_client::readfile(inum file, off_t offset, size_t size, std::string& buf){
  std::string read;
  int ret; 
  if((ret = ec->get(file, read)) != OK){
    printf("ckeh:yfs_client::readfile:Error: \n\tread:%s\n\tret:%s", read.c_str(), buf.c_str());
    return ret;
  }
  buf = read.substr(offset, size);
  printf("ckeh:yfs_client::readfile: \n\treadsize:%ld\n\tread:%.*s\n\tretsize:%ld\n\tret:%.*s", read.size(), (int)read.size(), read.c_str(), buf.size(), (int)buf.size(), buf.c_str());

  return OK;
}

int
yfs_client::writefile(inum file, off_t offset, size_t size, std::string buf){
  std::string read, next;
  int ret; 
  if((ret = ec->get(file, read)) != OK){
    return ret;
  }
  if(offset > read.size()){
    printf("ckeh:yfs_client::writefile: OFF > SIZE\n\toff-size:%ld\n\treadsize:%ld\n", offset-read.size(), read.size());
    read.resize(offset, '\0');
    // read += (std::string('\0', offset-read.size()));
    printf("\n\treadsize after adding nulls:%ld\n", read.size());

    read += buf.substr(0, size);
    next = read;
    printf("\n\treadsize after adding buf:%ld\n", read.size());

  } else {
    std::string beg = read.substr(0, offset);
    beg += buf.substr(0, size);
    beg += (read.size() > offset+size)? read.substr(offset+size): "";
    next = beg;
  }
  ec->put(file, next);
  printf("ckeh:yfs_client::writefile: \n\tcur:%s\n\tat_off:%ld\n\tsize:%ld\n\tadd:%s\n\tnew:%s\n\tnewsize:%ld\n", read.c_str(), offset, size, buf.c_str(), next.c_str(), next.size());

  return OK;
}

