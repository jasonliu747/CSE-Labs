// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

//#define LOCK_DEBUG

lock_server::lock_server():
  nacquire (0)
{
	pthread_mutex_init(&mutex, NULL);
	pthread_cond_init(&cond, NULL);
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab4 code goes here
  r = 0;
#ifdef LOCK_DEBUG
  printf("[lock.server.acquire.log] clt:[%i], lid:[%lld]\n", clt, lid);
#endif

  pthread_mutex_lock(&mutex);
  if (locked.find(lid) != locked.end()) {
	while (locked[lid]) {
#ifdef LOCK_DEBUG
  printf("[lock.server.acquire.log] pthread_cond_wait\n");
#endif
		pthread_cond_wait(&cond, &mutex);
	}
  }
  locked[lid] = true;
  nacquire++;
  pthread_mutex_unlock(&mutex);

  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab4 code goes here
  r = 0;
#ifdef LOCK_DEBUG
  printf("[lock.server.release.log] clt:[%i], lid[%lld]\n", clt, lid);
#endif
  
  pthread_mutex_lock(&mutex);

  if (locked.find(lid) == locked.end() ||
		  !locked[lid]) {
	  ret = lock_protocol::NOENT;
  } else {
	  locked[lid] = false;
	  pthread_cond_broadcast(&cond);
  }

  pthread_mutex_unlock(&mutex);
  return ret;
}
