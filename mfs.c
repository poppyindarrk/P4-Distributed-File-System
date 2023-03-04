#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/select.h>
#include "mfs.h"
#include "udp.h"

int sd;
struct sockaddr_in addrSnd, addrRcv;

int udp_send(message_t *request) {
    int rc = UDP_Write(sd, &addrSnd, (char *) request, sizeof(message_t));
    if (rc < 0) {
     // failed to send
        return -1;
    }
    return 0;
}

int udp_receive(message_t *response) {
    // wait for reply
    struct timeval timeout;
    // set the timeout to 30s
    timeout.tv_sec = 30;    
    timeout.tv_usec = 0;

    fd_set readfds;
    FD_ZERO(&readfds);
    FD_SET(sd, &readfds);

    if (select(sd+1, &readfds, NULL, NULL, &timeout) < 0) {
        // select err
        return -1;
    }

    int rc;
    if (FD_ISSET(sd, &readfds)) {
        rc = UDP_Read(sd, &addrRcv, (char *) response, sizeof(message_t));
    } else {
        printf("client:: request timeout\n");
        return -1;
    }

    if (rc > 0) {
        return 0;
    } else {
        return -1;
    }
}

int MFS_Init(char *hostname, int port){
    int MIN_PORT = 20000;
    int MAX_PORT = 40000;

    srand(time(0));
    int mfs_port = (rand() % (MAX_PORT - MIN_PORT) + MIN_PORT);

    // Bind random client port number
    sd = UDP_Open(mfs_port);
    if (sd < 0) {
        // udp_open failed
     return -1;
    }

    int rc = UDP_FillSockAddr(&addrSnd, hostname, port);
    if (rc < 0) {
        // init failed
     return -1;
    }
    return 0;
}

int MFS_Lookup(int pinum, char *name){
    message_t request;
    request.mtype = MFS_LOOKUP;
    request.inum = pinum;
    int name_len = strlen(name);
    if (name_len > 27 || name_len <=0) {
        // name too long/short
        return -1;
    }
    strcpy(request.name, name);

    int rc = udp_send(&request);
    if (rc < 0) {
        return -1;
    }

    message_t response;
    rc = udp_receive(&response);
    if (rc < 0) {
        return -1;
    }

    if (response.rc < 0) {
        return -1;
    } else {
        return response.inum;
    }
}

int MFS_Stat(int inum, MFS_Stat_t *m){
    message_t request;
    request.inum = inum;
    request.mtype = MFS_STAT;

    int rc = udp_send(&request);
    if (rc < 0) {
        return -1;
    }
    message_t response;
    rc = udp_receive(&response);
    if (rc < 0) {
        return -1;
    }

    if (response.rc < 0) {
        return -1;
    } else {
        m->size = response.size;
        m->type = response.type;
        return 0;
    }
}

int MFS_Write(int inum, char *buffer, int offset, int nbytes){
    if (nbytes <= 0 || nbytes > 4096) {
        // nbytes out of range
        return -1;
    }
    message_t request;
    request.mtype = MFS_WRITE;
    request.inum = inum;
    request.offset = offset;
    request.nbytes = nbytes;

    memcpy(request.buffer, buffer, nbytes);

    int rc = udp_send(&request);
    if (rc < 0) {
        return -1;
    }
    message_t response;
    rc = udp_receive(&response);
    if (rc < 0) {
        return -1;
    }

    return response.rc;
}

int MFS_Read(int inum, char *buffer, int offset, int nbytes){
    if (nbytes <= 0 || nbytes > 4096) {
        // nbytes out of range
        return -1;
    }
    message_t request;
    request.mtype = MFS_READ;
    request.inum = inum;
    request.offset = offset;
    request.nbytes = nbytes;

    int rc = udp_send(&request);
    if (rc < 0) {
        return -1;
    }

    message_t response;
    rc = udp_receive(&response);
    if (rc < 0) {
        return -1;
    }

    if (response.rc < 0) {
        return -1;
    } else {
        memcpy(buffer, response.buffer, nbytes);
        return 0;
    }
}

int MFS_Creat(int pinum, int type, char *name){
    message_t request;
    request.mtype = MFS_CREAT;
    request.inum = pinum;
    request.type = type;

    int name_len = strlen(name);
    if (name_len > 27 || name_len <=0) {
        // name too long/short
        return -1;
    }
    strcpy(request.name, name);

    int rc = udp_send(&request);
    if (rc < 0) {
        return -1;
    }

    message_t response;
    rc = udp_receive(&response);
    if (rc < 0) {
        return -1;
    }

    return response.rc;
}

int MFS_Unlink(int pinum, char *name){
    message_t request;
    request.mtype = MFS_UNLINK;
    request.inum = pinum;

    int name_len = strlen(name);
    if (name_len > 27 || name_len <=0) {
        // name too long/short
        return -1;
    }
    strcpy(request.name, name);

    int rc = udp_send(&request);
    if (rc < 0) {
        return -1;
    }

    message_t response;
    rc = udp_receive(&response);
    if (rc < 0) {
        return -1;
    }

    return response.rc;
}

int MFS_Shutdown(){
    message_t request;
    request.mtype = MFS_SHUTDOWN;

    int rc = udp_send(&request);
    if (rc < 0) {
        return -1;
    }

    UDP_Close(sd);
    return 0;
}