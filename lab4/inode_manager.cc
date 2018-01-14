#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk()
{
	bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
  /*
   *your lab1 code goes here.
   *if id is smaller than 0 or larger than BLOCK_NUM 
   *or buf is null, just return.
   *put the content of target block into buf.
   *hint: use memcpy
  */
	if (id<0 || id>BLOCK_NUM || buf==NULL) {
		printf("\tdisk:error! read_block: Input invalid. [id=%d]\n", id);
		return;
	}

	memcpy(buf, blocks[id], BLOCK_SIZE);
	//printf("\t\tdisk: read_block [#%d] [%.*s]\n", id, 16, buf);
}

void
disk::write_block(blockid_t id, const char *buf)
{
  /*
   *your lab1 code goes here.
   *hint: just like read_block
  */
	if (id<0 || id>BLOCK_NUM || buf==NULL) {
		printf("\tdisk:error! write_block: Input invalid.\n");
		return;
	}

	memcpy(blocks[id], buf, BLOCK_SIZE);
	//printf("\t\tdisk: write_block [#%d] [%.*s]\n", id, 16, buf);
}

// block layer -----------------------------------------

// the minimum block id which can be allocate
#define MIN_BLOCKID (2+BLOCK_NUM/BPB+INODE_NUM/IPB)

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your lab1 code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.

   *hint: use macro IBLOCK and BBLOCK.
          use bit operation.
          remind yourself of the layout of disk.
   */
	blockid_t bid = 0;
	char buf[BLOCK_SIZE];

	while (bid < sb.nblocks) {
		read_block(BBLOCK(bid), buf);

		for (unsigned int i=0; i<BLOCK_SIZE; i++) {
			unsigned char mask = 1;
			while (mask) {
				if ((buf[i] & mask) == 0) {
					buf[i] |= mask;
					write_block(BBLOCK(bid), buf);
					return bid;
				}
				bid++;
				mask<<=1;
			}
		}
	}
	printf("\tbm:error! alloc_block: Can't find empty block.\n");	
	exit(0);
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your lab1 code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
	if (id<0 || id > sb.nblocks) {
		printf("\tbm:error! free_block: block %d is empty.\n", id);
	}
	char buf[BLOCK_SIZE];
	read_block(BBLOCK(id), buf);
	unsigned int i = id / 8;
	unsigned mask = ~(1<<(id % 8));
	buf[i] &= mask;
	write_block(BBLOCK(id), buf);
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;
  // alloc metadata block
  for (int i=0; i<MIN_BLOCKID; i++)
	  alloc_block();

  char buf[BLOCK_SIZE];
  memset(buf, 0, BLOCK_SIZE);
  memcpy(buf, &sb, sizeof(sb));
  write_block(1, buf);
  
}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your lab1 code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   * if you get some heap memory, do not forget to free it.
   */
	struct inode *ino;
	unsigned int inum = 1;
	char buf[BLOCK_SIZE];
	
	while (inum < bm->sb.ninodes) {
		bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
		
		for (unsigned int i=0; i<IPB && inum < bm->sb.ninodes; i++) {	
			ino = (inode_t *)buf + i;
		
			if (ino->type == 0) {
				ino->type = type;
				ino->size = 0;
				ino->atime = time(0);
				ino->mtime = time(0);
				ino->ctime = time(0);
				bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
				return inum;
			}
			inum++;
		}
	}
	printf("\tim: error! alloc_inode: No free inode.\n");
	exit(0);
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your lab1 code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   * do not forget to free memory if necessary.
   */
	//printf("\tim: free_inode [#%d]\n", inum);
	struct inode *ino = get_inode(inum);

	if (ino == NULL) {
		printf("\tim: error! free_inode: #%d is freed again.\n", inum); 
		exit(0);
	}
	memset(ino, 0, sizeof(inode_t));
	put_inode(inum, ino);
	free(ino);
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  //printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode*)buf + inum%IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your lab1 code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_out
   */
	//printf("\tim: \033[1;32mread_file\033[0m to \033[1;33m%d\033[0m\n", inum);
	struct inode *ino = get_inode(inum);
	unsigned int block_num = (ino->size) / BLOCK_SIZE;
	char tmp[BLOCK_SIZE];
	blockid_t *indirect;
	
	// direct read
	char *buf = (char *)malloc(sizeof(char)*(ino->size));
	*buf_out = buf;
	for (unsigned int i = 0; i < block_num && i < NDIRECT; i++) {
		bm->read_block(ino->blocks[i], buf);
		buf += BLOCK_SIZE;
	}

	// indirect read
	if (block_num>NDIRECT) {
		bm->read_block(ino->blocks[NDIRECT], tmp);
		indirect = (blockid_t *)tmp;
		for (unsigned int i=0; i < block_num-NDIRECT; i++) {
			bm->read_block(*indirect, buf);
			buf += BLOCK_SIZE;
			indirect++;
		}
	}

	// deal with leftover
	if (ino->size % BLOCK_SIZE) {
		if (block_num>NDIRECT)
			bm->read_block(*indirect, tmp);
		else {
			bm->read_block(ino->blocks[block_num], tmp);
		}
		memcpy(buf, tmp, ino->size % BLOCK_SIZE);
	}
	// modify inode
	*size = ino->size;
	ino->atime = time(0);
	ino->ctime = time(0);
	put_inode(inum, ino);
	free(ino);

	//printf("\tim: \033[1;32mread_file\033[0m to \033[1;33m%d\033[0m - [%.*s]\n", inum, 15, *buf_out);
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your lab1 code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode.
   * you should free some blocks if necessary.
   */
	
	inode_t* inode = get_inode(inum);
	uint32_t old_blocks = (inode->size+BLOCK_SIZE-1) / BLOCK_SIZE;

	inode->size = MIN(MAXFILE *BLOCK_SIZE, (unsigned)size);
	
	char indirect_tmp[BLOCK_SIZE];
	uint32_t* indirect_array;
	uint32_t new_blocks = (inode->size+BLOCK_SIZE-1) / BLOCK_SIZE;

	// free all block
	if (old_blocks > NDIRECT) { 
		char tmp[BLOCK_SIZE];
		bm->read_block(inode->blocks[NDIRECT], tmp);
		indirect_array = (uint32_t*)tmp;
		for (uint32_t i = 0; i < old_blocks - NDIRECT; i++)
			bm->free_block(indirect_array[i]);
		bm->free_block(inode->blocks[NDIRECT]);
	}
	for (uint32_t i = 0; i < old_blocks && i < NDIRECT; i++)
		bm->free_block(inode->blocks[i]);
	
	// alloc new block and write
	if (new_blocks > NDIRECT) {
		inode->blocks[NDIRECT] = bm->alloc_block();
		bm->read_block(inode->blocks[NDIRECT], indirect_tmp);
		indirect_array = (uint32_t*)indirect_tmp;
	}

	uint32_t remain_len = inode->size;
	uint32_t write_len = 0;
	char write_buffer[BLOCK_SIZE];
	for (uint32_t i = 0; i < new_blocks; i++) {
		write_len = MIN(remain_len, BLOCK_SIZE);
		memset(write_buffer, 0, BLOCK_SIZE);
		memcpy(write_buffer, buf, write_len);
		if (i < NDIRECT) {
			inode->blocks[i] = bm->alloc_block();
			bm->write_block(inode->blocks[i] , write_buffer);
		}
		else {
			indirect_array[i - NDIRECT] = bm->alloc_block();
			bm->write_block(indirect_array[i - NDIRECT], write_buffer);
		}
		buf += write_len;
		remain_len -= write_len;
	}
	bm->write_block(inode->blocks[NDIRECT], indirect_tmp);

	// change metadata
	inode->ctime = time(0);
	inode->mtime = time(0);
	put_inode(inum, inode);
	free(inode);
	return;
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your lab1 code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
	struct inode *ino = get_inode(inum);

	if (ino) {
		a.type = ino->type;
		a.size = ino->size;
		a.atime = ino->atime;
		a.mtime = ino->mtime;
		a.ctime = ino->ctime;
	}
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your lab1 code goes here
   * note: you need to consider about both the data block and inode of the file
   * do not forget to free memory if necessary.
   */
	struct inode *ino = get_inode(inum);
	if (!ino) {
		printf("\tim: error! remove_file: Input invalid.\n");
		exit(0);
	}

	unsigned int block_num = (ino->size + BLOCK_SIZE -1) / BLOCK_SIZE;
	// if the inode is indirect
	if (block_num > NDIRECT) {
		for (unsigned int i = 0; i < NDIRECT; i++) {
			bm->free_block(ino->blocks[i]);
		}

		char buf[BLOCK_SIZE];
		bm->read_block(ino->blocks[NDIRECT], buf);
		blockid_t *indirect = (blockid_t *)buf;
		for (unsigned int i = 0; i < block_num - NDIRECT; i++) {
			bm->free_block(*indirect);
			indirect++;
		}
		bm->free_block(ino->blocks[NDIRECT]);
	// if the inode is direct
	} else {
		for (unsigned int i=0; i<block_num; i++) {
			bm->free_block(ino->blocks[i]);
		}
	}	
	free_inode(inum);
	free(ino);
}
