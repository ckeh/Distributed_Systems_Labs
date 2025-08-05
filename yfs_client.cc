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
#include <cstdlib>


yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  srand(getpid());

  ec = new extent_client(extent_dst);
  lc = new lock_client(lock_dst);
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
yfs_client::markinum(inum inum, bool markfile){
  if(markfile){
    return inum | (1ULL << 31U);
  } else {
    return inum & ~(1ULL << 31U);
  }
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
  lc->acquire(inum);
  std::string buf;
  int ret;
  if((ret = ec->get(inum, buf)) != OK){
    lc->release(inum);
    return ret; 
  }
  off_t cur = sizeof(buf);
  if(cur < newsize){
    ec->put(inum, buf + std::string('\0', newsize-cur));
  } else if(cur > newsize){
    ec->put(inum, buf.substr(0, newsize));
  }
  lc->release(inum);

  return OK;
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
  lc->acquire(inum);
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
  lc->release(inum);
  return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
  lc->acquire(inum);
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
  lc->release(inum);
  return r;
}


int 
yfs_client::create(inum parent, std::string name, inum& new_file, bool createfile){
  int ret = OK;

  lc->acquire(parent);
  std::string buf;
  ret = ec->get(parent, buf);

  if(ret != OK){
    return ret;
  }

  std::stringstream ss(buf);
  struct dirent ent;

  while(ss >> ent.inum >> ent.name){
    if(ent.name.compare(name) == 0){
      lc->release(parent);
      return yfs_client::EXIST;
    }
  }

  inum newinum = markinum(rand(), createfile);
  // get a free inum
  std::string temp;
  while(ec->get(newinum, temp) == extent_protocol::OK){
    newinum = markinum(rand(), createfile);
  }
  printf("ckeh:yfs_client::create: parent:%lld, filename:%s, newinum:%lld\n", parent, name.c_str(), newinum);
  // put newinum
  if((ret = ec->put(newinum, "")) != extent_protocol::OK){
    printf("ckeh:yfs_client::create: error:%d\n", ret);
    lc->release(parent);
    lc->release(newinum);
    return ret;
  }
  lc->release(newinum);

  printf("ckeh:yfs_client::create: parent data:%s\n", buf.c_str());

  buf += filename(newinum) + " " + name + " " ;
  if((ret = ec->put(parent, buf)) != extent_protocol::OK){
    printf("ckeh:yfs_client::create: error:%d\n", ret);
    lc->release(parent);
    return ret;
  }
  new_file = newinum;
  lc->release(parent);

  return OK;
}

int 
yfs_client::lookup(inum parent, std::string name, inum& inum){
  lc->acquire(parent);

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
        lc->release(parent);
        return OK;
      }
    }
  }
  lc->release(parent);
  return NOENT;
}

int
yfs_client::unlink(inum parent, std::string name){
  lc->acquire(parent);

  std::string data;
  bool found = false;
  std::string dirbuf;

  extent_protocol::status s = ec->get(parent, data);
  if(s != extent_protocol::OK){
    return s;
  }
  std::stringstream ss(data);
  dirent dirent;

  while(ss >> dirent.inum >> dirent.name){
    // printf("ckeh:yfs_client::unlink: inum:%lld, name:%s\n", dirent.inum, dirent.name.c_str());
    if(dirent.name.compare(name) == 0){
      if(isdir(dirent.inum)){
        lc->release(parent);
        return NOENT;
      }
      lc->acquire(dirent.inum);
      ec->remove(dirent.inum);
      found = true;
      lc->release(dirent.inum);
      continue;
    }
  
    dirbuf += filename(dirent.inum) + " " + dirent.name + " ";
  }
  // if((ret = readdir(parent, dirlist)) != OK){
  //   lc->release(parent);
  //   return ret;
  // }
  if(!found){
    lc->release(parent);
    return NOENT;
  } 
  ec->put(parent, dirbuf);
  lc->release(parent);
  return OK;
}

int 
yfs_client::readdir(inum parent, std::vector<dirent>& dirlist){
  lc->acquire(parent);

  std::string data;
  extent_protocol::status s = ec->get(parent, data);
  if(s == extent_protocol::OK){
    std::stringstream ss(data);
    dirent ent;

    while(ss >> ent.inum >> ent.name){
      printf("ckeh:yfs_client::readdir: inum:%lld, name:%s\n", ent.inum, ent.name.c_str());
      dirlist.push_back(ent);
    }
    lc->release(parent);
    return OK;
  } 
  lc->release(parent);
  return s;
}

int
yfs_client::readfile(inum file, off_t offset, size_t size, std::string& buf){
  lc->acquire(file);

  std::string read;
  int ret; 
  if((ret = ec->get(file, read)) != OK){
    printf("ckeh:yfs_client::readfile:Error: \n\tread:%s\n\tret:%s\n", read.c_str(), buf.c_str());
    lc->release(file);
    return ret;
  }
  buf = read.substr(offset, size);
  printf("ckeh:yfs_client::readfile: \n\treadsize:%ld\n\tread:%.*s\n\tretsize:%ld\n\tret:%.*s\n", read.size(), (int)read.size(), read.c_str(), buf.size(), (int)buf.size(), buf.c_str());
  lc->release(file);

  return OK;
}

int
yfs_client::writefile(inum file, off_t offset, size_t size, std::string buf){
  lc->acquire(file);

  std::string read, next;
  int ret; 
  if((ret = ec->get(file, read)) != OK){
    lc->release(file);
    return ret;
  }
  if(offset > (int) read.size()){
    printf("ckeh:yfs_client::writefile: OFF > SIZE\n\toff-size:%ld\n\tre adsize:%ld\n", offset-read.size(), read.size());
    read.resize(offset, '\0');
    // read += (std::string('\0', offset-read.size()));
    printf("\n\treadsize after adding nulls:%ld\n", read.size());

    read += buf.substr(0, size);
    next = read;
    printf("\n\treadsize after adding buf:%ld\n", read.size());

  } else {
    std::string beg = read.substr(0, offset);
    beg += buf.substr(0, size);
    if(read.size() > offset + size){
      beg += read.substr(offset+size);
    }
    next = beg;
  }
  ec->put(file, next);
  printf("ckeh:yfs_client::writefile: \n\tcur:%s\n\tat_off:%ld\n\tsize:%ld\n\tadd:%s\n\tnew:%s\n\tnewsize:%ld\n", read.c_str(), offset, size, buf.c_str(), next.c_str(), next.size());
  lc->release(file);
  return OK;
}

