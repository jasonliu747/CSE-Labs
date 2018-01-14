#include <pthread.h>



#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk()
{
  pthread_t id;
  int ret;
  bzero(blocks, sizeof(blocks));

  ret = pthread_create(&id, NULL, test_daemon, (void*)blocks);
  if(ret != 0)
	  printf("FILE %s line %d:Create pthread error\n", __FILE__, __LINE__);
}

void
disk::read_block(blockid_t id, char *buf)
{
  if (id < 0 || id >= BLOCK_NUM || buf == NULL)
    return;

  memcpy(buf, blocks[id], BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf)
{
  if (id < 0 || id >= BLOCK_NUM || buf == NULL)
    return;

  memcpy(blocks[id], buf, BLOCK_SIZE); 
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
   */
	blockid_t bid = 0;
	char buf[HALF_BLOCK_SIZE];

	while (bid < sb.nblocks) {
		read_block(BBLOCK(bid), buf);

		for (unsigned int i=0; i<HALF_BLOCK_SIZE; i++) {
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
	char buf[HALF_BLOCK_SIZE];
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
  sb.size = HALF_BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;
  // alloc metadata block
  for (int i=0; i<MIN_BLOCKID; i++)
	  alloc_block();

  char buf[HALF_BLOCK_SIZE];
  memset(buf, 0, HALF_BLOCK_SIZE);
  memcpy(buf, &sb, sizeof(sb));
  write_block(1, buf);
  
}

/*
 * lab7: Erasure Coding
 *
 * Using hamming code to detect and repair error.
 */

// macros for bit operation
#define MASK(n) (0x1<<(n))
#define GET_BIT(byte, n) ((byte&MASK(n))>>n)
#define SET_BIT(byte, n) (byte=(byte|MASK(n)))
#define CLEAR_BIT(byte, n) (byte=(byte&(~MASK(n))))

// hamming encode & decode
char *hamming_encode(const char* buf)
{
  char *ret = (char *)malloc(sizeof(char) * BLOCK_SIZE);
  //memcpy(ret, buf, HALF_BLOCK_SIZE);

  for (int i = 0; i < HALF_BLOCK_SIZE; i++)
  {
    char oldc = buf[i];
#ifdef HAMMING_DEBUG
    cout << "[in i:_" << i << "_] "; print_bin(oldc);
#endif
    for (int j = 0; j < 2; j++)
    {
      char newc = 0;
      // put original bits to char
      if (GET_BIT(oldc, 0 + 4 * j))
        SET_BIT(newc, 2);
      if (GET_BIT(oldc, 1 + 4 * j))
        SET_BIT(newc, 4);
      if (GET_BIT(oldc, 2 + 4 * j))
        SET_BIT(newc, 5);
      if (GET_BIT(oldc, 3 + 4 * j))
        SET_BIT(newc, 6);

      // put error check bits to char
      if (GET_BIT(newc, 2) ^ GET_BIT(newc, 4) ^ GET_BIT(newc, 6))
        SET_BIT(newc, 0);
      if (GET_BIT(newc, 2) ^ GET_BIT(newc, 5) ^ GET_BIT(newc, 6))
        SET_BIT(newc, 1);
      if (GET_BIT(newc, 4) ^ GET_BIT(newc, 5) ^ GET_BIT(newc, 6))
        SET_BIT(newc, 3);

#ifdef HAMMING_DEBUG
      cout << "[out j:_"<<j<<"_] ";print_bin(newc);
#endif
      ret[2 * i + j] = newc;
    }
  }
  return ret;
}

void hamming_decode(char *dst, const char *src)
{
  //memcpy(dst, src, HALF_BLOCK_SIZE);
  for (int i = 0; i < HALF_BLOCK_SIZE; i++)
  {
    char newc = 0;
    for (int j = 0; j < 2; j++)
    {
      char oldc = src[2 * i + j];
      // errorn: the position of error (0 means correct)
      unsigned short errorn = 0;

      // calculate errorn
      if (GET_BIT(oldc, 0) ^ GET_BIT(oldc, 2) ^ GET_BIT(oldc, 4) ^ GET_BIT(oldc, 6))
        SET_BIT(errorn, 0);
      if (GET_BIT(oldc, 1) ^ GET_BIT(oldc, 2) ^ GET_BIT(oldc, 5) ^ GET_BIT(oldc, 6))
        SET_BIT(errorn, 1);
      if (GET_BIT(oldc, 3) ^ GET_BIT(oldc, 4) ^ GET_BIT(oldc, 5) ^ GET_BIT(oldc, 6))
        SET_BIT(errorn, 2);

      if (errorn)
      {
        errorn--;
#ifdef HAMMING_DEBUG
        cout << "[detect] pos=" << errorn << endl;
#endif
      if (!GET_BIT(oldc, errorn))
        SET_BIT(oldc, errorn);
      else
        CLEAR_BIT(oldc, errorn);
      }

#ifdef HAMMING_DEBUG
      cout << "[in j:_" << j << "_] "; print_bin(oldc);
#endif

      if (GET_BIT(oldc, 2))
        SET_BIT(newc, 0 + 4 * j);
      if (GET_BIT(oldc, 4))
        SET_BIT(newc, 1 + 4 * j);
      if (GET_BIT(oldc, 5))
        SET_BIT(newc, 2 + 4 * j);
      if (GET_BIT(oldc, 6))
        SET_BIT(newc, 3 + 4 * j);
    }
#ifdef HAMMING_DEBUG
    cout << "[in i:_" << i << "_] "; print_bin(newc);
#endif
    dst[i] = newc;
  }
}


void
block_manager::read_block(uint32_t id, char *buf)
{
  char tmp[BLOCK_SIZE];
  d->read_block(id, tmp);
  hamming_decode(buf, tmp);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, hamming_encode(buf));
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
   */
	struct inode *ino;
	unsigned int inum = 1;
	char buf[HALF_BLOCK_SIZE];
	
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
   */
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
  char buf[HALF_BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

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
  char buf[HALF_BLOCK_SIZE];
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
   * and copy them to buf_Out
   */
	//printf("\tim: \033[1;32mread_file\033[0m to \033[1;33m%d\033[0m\n", inum);
	struct inode *ino = get_inode(inum);
	unsigned int block_num = (ino->size) / HALF_BLOCK_SIZE;
	char tmp[HALF_BLOCK_SIZE];
	blockid_t *indirect;
	
	// direct read
	char *buf = (char *)malloc(sizeof(char)*(ino->size));
	*buf_out = buf;
	for (unsigned int i = 0; i < block_num && i < NDIRECT; i++) {
		bm->read_block(ino->blocks[i], buf);
		buf += HALF_BLOCK_SIZE;
	}

	// indirect read
	if (block_num>NDIRECT) {
		bm->read_block(ino->blocks[NDIRECT], tmp);
		indirect = (blockid_t *)tmp;
		for (unsigned int i=0; i < block_num-NDIRECT; i++) {
			bm->read_block(*indirect, buf);
			buf += HALF_BLOCK_SIZE;
			indirect++;
		}
	}

	// deal with leftover
	if (ino->size % HALF_BLOCK_SIZE) {
		if (block_num>NDIRECT)
			bm->read_block(*indirect, tmp);
		else {
			bm->read_block(ino->blocks[block_num], tmp);
		}
		memcpy(buf, tmp, ino->size % HALF_BLOCK_SIZE);
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
   * is larger or smaller than the size of original inode
   */
	
	inode_t* inode = get_inode(inum);
	uint32_t old_blocks = (inode->size+HALF_BLOCK_SIZE-1) / HALF_BLOCK_SIZE;

	inode->size = MIN(MAXFILE *HALF_BLOCK_SIZE, (unsigned)size);
	
	char indirect_tmp[HALF_BLOCK_SIZE];
	uint32_t* indirect_array;
	uint32_t new_blocks = (inode->size+HALF_BLOCK_SIZE-1) / HALF_BLOCK_SIZE;

	// free all block
	if (old_blocks > NDIRECT) { 
		char tmp[HALF_BLOCK_SIZE];
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
	char write_buffer[HALF_BLOCK_SIZE];
	for (uint32_t i = 0; i < new_blocks; i++) {
		write_len = MIN(remain_len, HALF_BLOCK_SIZE);
		memset(write_buffer, 0, HALF_BLOCK_SIZE);
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
   */
	struct inode *ino = get_inode(inum);
	if (!ino) {
		printf("\tim: error! remove_file: Input invalid.\n");
		exit(0);
	}

	unsigned int block_num = (ino->size + HALF_BLOCK_SIZE -1) / HALF_BLOCK_SIZE;
	// if the inode is indirect
	if (block_num > NDIRECT) {
		for (unsigned int i = 0; i < NDIRECT; i++) {
			bm->free_block(ino->blocks[i]);
		}

		char buf[HALF_BLOCK_SIZE];
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
