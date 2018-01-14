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

using std::string;

yfs_client::yfs_client()
{
  ec = NULL;
  lc = NULL;
}

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst, const char* cert_file)
{
  ec = new extent_client(extent_dst);
  lc = new lock_client(lock_dst);
  lc->acquire(1);
  if (ec->put(1, "") != extent_protocol::OK)
      printf("error init root dir\n"); // XYB: init root dir
  lc->release(1);
}

int
yfs_client::verify(const char* name, unsigned short *uid)
{
  	int ret = OK;

	return ret;
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
	bool ret = false;

	lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
    } else if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        ret = true;
    } else { 
		printf("isfile: %lld is a dir\n", inum);
	}
	lc->release(inum);
    return ret;
}

bool
yfs_client::isdir(inum inum)
{
    // Oops! is this still correct when you implement symlink?
	extent_protocol::attr a;
	bool ret = false;

	lc->acquire(inum);
	if (ec->getattr(inum, a) != extent_protocol::OK) {
		printf("error getting attr\n");
	} else if (a.type == extent_protocol::T_DIR) {
		printf("isdir: %lld is a dir\n", inum);
		ret = true;
	} else {
		printf("isdir: %lld isn't a dir\n", inum);
	}
	lc->release(inum);
    return ret;
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
	lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
    } else {
		fin.atime = a.atime;
		fin.mtime = a.mtime;
		fin.ctime = a.ctime;
		fin.size = a.size;
		printf("getfile %016llx -> sz %llu\n", inum, fin.size);
	}
	lc->release(inum);
    return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
	lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
    } else {
		din.atime = a.atime;
		din.mtime = a.mtime;
		din.ctime = a.ctime;
	}
	lc->release(inum);

    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("[ERROR] EXT_RPC: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        return r; \
    } \
} while (0)

// Only support set size of attr
int
yfs_client::setattr(inum ino, filestat st, unsigned long size)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length
     */
	string buf;
	lc->acquire(ino);
	EXT_RPC(ec->get(ino, buf));

	if (buf.size() > size) 
		buf.resize(size);
	else
		buf.resize(size, '\0');
	ec->put(ino, buf);
	lc->release(ino);

    return r;
}

int
yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
	printf("[LOG::create] parent:[%d], name:[%s]\n", (int)parent, name);
	bool b = false;
	lc->acquire(parent);
	lookup(parent, name, b, ino_out);
	if (b) {
		printf("[ERROR::create] create a existing file\n");
		r = EXIST;
	} else {
		EXT_RPC(ec->create(extent_protocol::T_FILE, ino_out));
		string buf;
		EXT_RPC(ec->get(parent, buf));
		buf.append(string(name) + "," + filename(ino_out) + ";");
		EXT_RPC(ec->put(parent, buf));

		printf("[LOG::create] ino:[%d] complete\n", (int)ino_out);
		/*
		lookup(parent, name, b, ino_out);
		if (!b) {
			printf("[ERROR::create] Can't find file after create\n");
			r = EXIST;
		}*/
	}
	lc->release(parent);
    return r;
}

int
yfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
	printf("[LOG::mkdir] parent:[%d], name:[%s]\n", (int)parent, name);
	bool b = false;
	lc->acquire(parent);
	lookup(parent, name, b, ino_out);
	if (b) {
		printf("[ERROR::mkdir] create a exisiting directory\n");
		r = EXIST;
	} else {
		EXT_RPC(ec->create(extent_protocol::T_DIR, ino_out));
		string buf;
		EXT_RPC(ec->get(parent, buf));

		buf.append(string(name) + "," + filename(ino_out) + ";");
		EXT_RPC(ec->put(parent, buf));

		printf("[LOG::mkdir] ino:[%d] complete\n", (int)ino_out);
	}
	lc->release(parent);
    return r;
}

int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
	string buf;
	EXT_RPC(ec->get(parent, buf));

	printf("[LOG::lookup] parent:[%d], name:[%s], buf_len:[%d]\n", (int)parent, name, (int)buf.size());
	size_t pos = buf.find(string(name)+",");
	if (pos == buf.npos) {
		found = false;
		printf("[LOG::lookup] not found\n");
		printf("\t[lookup.buf] [%s]\n", buf.c_str());
		return NOENT;
	}
	found = true;
	size_t start_pos = buf.find(",", pos+1),
		   end_pos = buf.find(";", start_pos+1);
	string ino_str = buf.substr(start_pos+1, end_pos-start_pos-1);
	ino_out = n2i(ino_str);
	
	printf("[LOG::lookup] found\n");
    return r;
}

int
yfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
	if (!isdir(dir)) {
		printf("[ERROR] readdir from a file\n");
		return IOERR;
	}
	string buf;
	EXT_RPC(ec->get(dir, buf));
	size_t start_pos=0, end_pos=0, len = buf.length();
	printf("[LOG::readdir] dir:[%d]\n", (int)dir);
	while (start_pos < len) {
		dirent dir;
		end_pos = buf.find(",", start_pos+1);
		dir.name = buf.substr(start_pos, end_pos-start_pos);

		start_pos = buf.find(";", end_pos+1);
		dir.inum = n2i(buf.substr(end_pos+1, start_pos-end_pos-1));
		list.push_back(dir);
		start_pos++;
	}
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
	string buf;
	lc->acquire(ino);
	EXT_RPC(ec->get(ino, buf));
	int buf_size = buf.size();
	if (buf_size < off)
		data = "\0";
	else {
		if (buf_size >= off + (int)size)
			data = buf.substr(off, size);
		else
			data = buf.substr(off, buf_size-off);
	}
	lc->release(ino);

    return r;
}

int
yfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
	printf("[LOG::write] ino:[%d], size:[%d], data:[%.8s]\n", (int)ino, (int)size, data);
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
	string buf;
	lc->acquire(ino);
	EXT_RPC(ec->get(ino, buf));
	int buf_size = buf.size();
	if (buf_size < off) {
		buf.resize(off, '\0');
		buf.append(data, size);
	} else {
		if (buf_size >= off + (int)size)
			buf.replace(off, size, string(data, size));
		else {
			buf.resize(off);
			buf.append(data, size);
		}
	}

	bytes_written = size;
	EXT_RPC(ec->put(ino, buf));
	lc->release(ino);
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
	bool b;
	inum ino;
	lc->acquire(parent);
	lookup(parent, name, b, ino);
	if (!b) {
		printf("[ERROR::unlink] unlink a inexistent file/dir\n");
		r = NOENT;
	} else {
		EXT_RPC(ec->remove(ino));
		string buf;
		EXT_RPC(ec->get(parent, buf));
		
		size_t pos = buf.find(string(name)+","),
			   end_pos = buf.find(";", pos+1);
		buf.replace(pos, end_pos-pos+1, "");
		EXT_RPC(ec->put(parent, buf));
	}
	lc->release(parent);
    return r;
}

int yfs_client::symlink(inum parent, const string name, const string path, inum &ino_out)
{
	int r = OK;

	bool b = false;
	lc->acquire(parent);
	lookup(parent, name.c_str(), b, ino_out);
	if (b) {
		printf("[ERROR::symlink] name exists\n");
		r = EXIST;
	} else {
		string buf;
		EXT_RPC(ec->create(extent_protocol::T_LINK, ino_out));
		EXT_RPC(ec->get(parent, buf));
		buf.append(name+","+filename(ino_out)+";");
		EXT_RPC(ec->put(parent, buf));

		EXT_RPC(ec->put(ino_out, path));
		printf("[LOG::symlink] parent=[%d], name=[%s], path=[%s], ino=[%d]\n", (int)parent, name.c_str(), path.c_str(), (int)ino_out);
	}
	lc->release(parent);
	return r;
}

int yfs_client::readlink(inum ino, std::string &path)
{
	int r = OK;

	EXT_RPC(ec->get(ino, path));
	printf("[LOG::readlink] ino=[%d], path=[%s]", (int)ino, path.c_str());
	
	return r;
}
