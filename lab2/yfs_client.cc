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

yfs_client::yfs_client()
{
    ec = new extent_client();
}

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client();
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
    
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

bool
yfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    printf("isfile: %lld is not a file\n", inum);
    return false;
}

bool
yfs_client::isdir(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_DIR) {
        printf("isdir: %lld is a dir\n", inum);
        return true;
    }
    printf("isdir: %lld is not a dir\n", inum);
    return false;
}

/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */
bool
yfs_client::issymlink(inum inum) {
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_SYMLINK) {
        printf("issymlink: %lld is a symlink\n", inum);
        return true;
    }

    printf("issymlink: %lld is not a symlink\n", inum);
    return false;
}

int
yfs_client::readlink(inum inum, std::string &dest) {
    printf("readlink %016llx\n", inum);
    return ec->get(inum, dest);
}

int
yfs_client::symlink(inum parent, const char *name, const char *link, inum &ino_out) {
    int r = OK;

    printf("symlink parent %016llx, name %s, link %s\n", parent, name, link);

    dir dirnode;
    if ((r = getdirnode(parent, dirnode)) != OK) return r;

    bool found;
    inum found_inode;
    
    dirnode.lookup(name, found, found_inode);
    if (found) return EXIST;
    if ((r = ec->create(extent_protocol::T_SYMLINK, ino_out)) != OK) return r;
    if ((r = ec->put(ino_out, std::string(link))) != OK) return r;
    dirnode.addentry(name, 0, ino_out);

    savedirnode(parent, dirnode);

    return r;
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
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

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
yfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */

    printf("setattr %016llx\n", ino);

    std::string buf;
    if ((r = ec->get(ino, buf)) != OK) return r;
    
    if (buf.size() == size) return r;
    
    buf.resize(size, '\0');
    r = ec->put(ino, buf);

    return r;
}

int
yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    printf("create parent: %016llx, name: %s\n", parent, name);

    /*
     * your lab2 code goes here.
     * note: lookup is what you need to check if file EXIST;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    
    dir dirnode;
    if ((r = getdirnode(parent, dirnode)) != OK) return r;

    bool found;
    inum found_inode;
    
    dirnode.lookup(name, found, found_inode);
    if (found) return EXIST;
    if ((r = ec->create(extent_protocol::T_FILE, ino_out)) != OK) return r;
    
    dirnode.addentry(name, mode, ino_out);
    
    savedirnode(parent, dirnode);
 
    return r;
}

int
yfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    printf("mkdir parent: %016llx, name: %s\n", parent, name);
    /*
     * your lab2 code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    
    dir dirnode;
    if ((r = getdirnode(parent, dirnode)) != OK) return r;
    
    bool found;
    inum found_inode;
    
    dirnode.lookup(name, found, found_inode);
    if (found) return EXIST;
    if ((r = ec->create(extent_protocol::T_DIR, ino_out)) != OK) return r;

    dirnode.addentry(name, mode, ino_out);
    
    savedirnode(parent, dirnode);

    return r;
}

int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    printf("lookup %s\n", name);

    /*
     * your lab2 code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */

    dir dirnode;
    if ((r = getdirnode(parent, dirnode)) != OK) return r;

    dirnode.lookup(name, found, ino_out);

    return r;
}

int
yfs_client::readdir(inum parent, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */

    printf("readdir %016llx\n", parent);

    dir dirnode;
    if ((r = getdirnode(parent, dirnode)) != OK) return r;

    dirnode.getentries(list);

    return r;
}

int
yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: read using ec->get().
     */
    
    printf("read %016llx, size %lu off %lu\n", ino, size, off);

    r = ec->get(ino, data);
    data.erase(0, off);
    
    if (data.size() > size)
      data.resize(size);

    printf("read %016llx actual size %lu\n", ino, data.size());
    return r;
}

int
yfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */

    printf("write %016llx, size %lu, off %lu\n", ino, size, off);

    std::string buf;

    if ((r = ec->get(ino, buf)) != OK) return r;
    
    bytes_written = off + size - buf.size();
    
    if (off + size > buf.size()) {
        buf.resize(off + size, '\0');
    }
    buf.replace(off, size, data, size);
    r = ec->put(ino, buf);
    return r;
}

int yfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */

    dir dirnode;
    if ((r = getdirnode(parent, dirnode)) != OK) return r;
    
    bool found;
    inum found_inode;

    dirnode.lookup(name, found, found_inode);
    if (!found) return NOENT;

    if ((r = ec->remove(found_inode)) != OK) return r;
    
    dirnode.removeentry(name);

    r = savedirnode(parent, dirnode);

    return r;
}

int yfs_client::getdirnode(inum ino, dir &dir_out) {
    int r = OK;
    std::string buf;
    if (!isdir(ino)) return IOERR;
    if ((r = ec->get(ino, buf)) != OK) return r;

    dir_out = dir(buf);
    
    return r;
}

int yfs_client::savedirnode(inum ino, const dir &dirnode) {
    std::string buf;
    dirnode.dump(buf);
    return ec->put(ino, buf);
}

yfs_client::dir::dir(const std::string &buf) {
    const char *b = buf.c_str();
    uint32_t cnt = *(uint32_t *)b;
    b += sizeof(uint32_t);

    while (cnt --> 0) {
        uint8_t len = *(uint8_t *)b;
        b += sizeof(uint8_t);
        std::string name(b, b + len);
        b += len;
        inum ino = *(inum *)b;
        b += sizeof(inum);
        entries[name] = ino;
    }

}

void yfs_client::dir::dump(std::string &out) const {
    uint32_t cnt = entries.size();
    char *buf = new char[cnt * (sizeof(uint8_t) + sizeof(uint32_t) + MAX_NAME_LEN) + sizeof(uint32_t)];

    char *b = buf;
    *(uint32_t *)b = cnt;
    b += sizeof(uint32_t);
    
    for (const auto &entry: entries) {
        const auto &name = entry.first;
        inum ino = entry.second;
        *(uint8_t *)b = name.size();
        b += sizeof(uint8_t);
        memcpy(b, name.c_str(), name.size());
        b += name.size();
        *(inum *)b = ino;
        b += sizeof(inum);
    }
    
    out = std::string(buf, b);

    delete[] buf;
}

void yfs_client::dir::lookup(const std::string &name, bool &found, inum &ino_out) const {
    printf("_lookup %s\n", name.c_str());

    const auto it = entries.find(name);
    if (it != entries.end()) {
        found = true;
        ino_out = it->second;
    } else {
        found = false;
    }
}

void yfs_client::dir::addentry(const std::string &name, mode_t mode, inum ino) {
    entries[name] = ino;
}

void yfs_client::dir::removeentry(const std::string &name) {
    entries.erase(name);
}

void yfs_client::dir::getentries(std::list<dirent> &entries) const {
    for (const auto &entry: this->entries) {
        entries.push_back(dirent { entry.first, entry.second });
    }
}

