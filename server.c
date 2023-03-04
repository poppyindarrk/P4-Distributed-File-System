#include <stdio.h>
#include <signal.h>
#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include "ufs.h"
#include "udp.h"
#include "mfs.h"


typedef struct {
    unsigned int bits[UFS_BLOCK_SIZE / sizeof(unsigned int)];
} bitmap_t;

typedef struct {
    dir_ent_t entries[128];
} dir_block_t;

int sd;
struct sockaddr_in sockaddr;

void *image;
int image_size;

// super block
super_t *s;
// pointers
bitmap_t *inode_bitmap;
bitmap_t *data_bitmap;
inode_t *itable;

void intHandler(int dummy) {
    UDP_Close(sd);
    exit(130);
}

unsigned int get_bit(unsigned int *bitmap, int position) {
    int index = position / 32;
    int offset = 31 - (position % 32);
    return (bitmap[index] >> offset) & 0x1;
}

void set_bit(unsigned int *bitmap, int position, int value) {
    int index = position / 32;
    int offset = 31 - (position % 32);
    if (value == 1) {
        bitmap[index] |= 0x1 << offset;
    } else if (value == 0) {
        bitmap[index] &= ~(0x1 << offset);
    }
}

// find free inode/data
int get_free_bit(unsigned int *bitmap, int end) {
    for (int i = 0; i < end; i++) {
        if (get_bit(bitmap, i) == 0) {
            return i;
        }
    }
    return -1;
}

// UDP response
int err() {
    message_t response;
    response.rc = -1;
    int rc = UDP_Write(sd, &sockaddr, (char *) &response, sizeof(message_t));
    if (rc < 0) {
	    printf("server:: failed to send\n");
        return -1;
    }
    return 0;
}

int reply_success(message_t *response) {
    response->rc = 0;

    int rc = UDP_Write(sd, &sockaddr, (char *) response, sizeof(message_t));
    if (rc < 0) {
	    printf("server:: failed to send\n");
        return -1;
    }
    return 0;
}

void handle_lookup(int pinum, char *name, char *blocks[]) {
    // if pinum not valid, reply -1
    if (pinum < 0 || pinum >= s->num_inodes) {
        err();
        return;
    }
    if (get_bit(inode_bitmap->bits, pinum) != 1) {
        err();
        return;
    }

    // if parent is not a dir, reply -1
    if (itable[pinum].type != UFS_DIRECTORY) {
        err();
        return;
    }

    int dir_size = itable[pinum].size;
    // if it's an empty dir, reply -1
    if (dir_size < sizeof(dir_ent_t)) {
        err();
        return;
    }

    int data_block_addr = (int)itable[pinum].direct[0];
    if (data_block_addr == -1) {
        err();
        return;
    }

    int data_block_index = data_block_addr - s->data_region_addr;
    // if data block not valid, reply -1
    if (get_bit(data_bitmap->bits, data_block_index) != 1) {
        err();
        return;
    }

    dir_block_t *dir = (dir_block_t*)blocks[data_block_addr];

    for (int i = 0; i < 128; i++) {
        if (dir->entries[i].inum == -1) {
            continue;
        }
        if (strcmp(dir->entries[i].name, name) == 0) {
            // file/dir found, reply inum
            message_t response;
            response.inum = dir->entries[i].inum;
            reply_success(&response);
            return;
        }
    }
    // find/dir not found, reply -1;
    err();
}

void handle_stat(int inum, char *blocks[]) {
    // if inum not valid, reply -1
    if (inum < 0 || inum >= s->num_inodes) {
        err();
        return;
    }
    if (get_bit(inode_bitmap->bits, inum) != 1) {
        err();
        return;
    }
    // reply MFS_Stat
    message_t response;
    response.type = itable[inum].type;
    response.size = itable[inum].size;
    reply_success(&response);
}

void handle_read(int inum, int offset, int nbytes, char *blocks[]) {
    // if inum not valid, reply -1
    if (inum < 0 || inum >= s->num_inodes) {
        err();
        return;
    }
    if (get_bit(inode_bitmap->bits, inum) != 1) {
        err();
        return;
    }

    // if not a file, reply -1
    if (itable[inum].type != UFS_REGULAR_FILE) {
        err();
        return;
    }

    int size = itable[inum].size;

    // check offset
    if (offset < 0 || offset + nbytes > size) {
        err();
        return;
    }

    // get the target block(s)
    int first_block = offset / UFS_BLOCK_SIZE;
    int block_offset = offset % UFS_BLOCK_SIZE;
    int bytes_left = 0;
    int block_num = 1;

    if (block_offset + nbytes > UFS_BLOCK_SIZE) {
        block_num++;
        bytes_left = nbytes - UFS_BLOCK_SIZE + block_offset;
    }

    // get the first block
    int data_block_addr1 = (int)itable[inum].direct[first_block];
    if (data_block_addr1 == -1) {
        err();
        return;
    }
    int data_block_idx1 = data_block_addr1 - s->data_region_addr;
    // if data block not valid, reply -1
    if (get_bit(data_bitmap->bits, data_block_idx1) != 1) {
        err();
        return;
    }

    // read data
    char *data_start = blocks[data_block_addr1] + block_offset;
    if (block_num == 1) {
        message_t response;
        memcpy(response.buffer, data_start, nbytes);
        reply_success(&response);
        return;
    } else {
        // get the second block
        int data_block_addr2 = (int)itable[inum].direct[first_block + 1];
        if (data_block_addr2 == -1) {
            err();
            return;
        }
        int data_block_idx2 = data_block_addr2 - s->data_region_addr;
        // if data block not valid, reply -1
        if (get_bit(data_bitmap->bits, data_block_idx2) != 1) {
            err();
            return;
        }
        message_t response;
        memcpy(response.buffer, data_start, nbytes - bytes_left);
        memcpy(response.buffer + nbytes - bytes_left, blocks[data_block_addr2], bytes_left);
        reply_success(&response);
        return;
    }
}

void handle_write(int inum, char *buffer, int offset, int nbytes, char *blocks[]) {
    // if inum not valid, reply -1
    if (inum < 0 || inum >= s->num_inodes) {
        err();
        return;
    }
    if (get_bit(inode_bitmap->bits, inum) != 1) {
        err();
        return;
    }

    // if not a file, reply -1
    if (itable[inum].type != UFS_REGULAR_FILE) {
        err();
        return;
    }

    int size = itable[inum].size;

    // check offset
    if (offset < 0 || offset > size) {
        err();
        return;
    }

    // get the target block(s)
    int first_block = offset / UFS_BLOCK_SIZE;
    int block_offset = offset % UFS_BLOCK_SIZE;
    int bytes_left = 0;
    int block_num = 1;

    if (block_offset + nbytes > UFS_BLOCK_SIZE) {
        block_num++;
        bytes_left = nbytes - UFS_BLOCK_SIZE + block_offset;
    }

    if (first_block >= DIRECT_PTRS) {
        // not that much blocks
        err();
        return;
    }

    // get the first block
    int data_block_addr1 = (int)itable[inum].direct[first_block];
    int data_block_idx1 = data_block_addr1 - s->data_region_addr;

    if (data_block_addr1 == -1) {
        // create and write in a new block
        int new_block_index = -1;

        new_block_index = get_free_bit(data_bitmap->bits, s->num_data);
        if (new_block_index == -1) {
            // no empty data block
            err();
            return;
        }
        set_bit(data_bitmap->bits, new_block_index, 1);
        data_block_addr1 = new_block_index + s->data_region_addr;
        data_block_idx1 = new_block_index;

        itable[inum].direct[first_block] = data_block_addr1;
    } else {
        // if data block not valid, reply -1
        if (get_bit(data_bitmap->bits, data_block_idx1) != 1) {
            err();
            return;
        }
    }

    // write data
    char *data_start = blocks[data_block_addr1] + block_offset;
    if (block_num == 1) {
        memcpy(data_start, buffer, nbytes);

        // update size
        int new_size = size;
        if (offset + nbytes > size) {
            new_size = offset + nbytes;
        }
        itable[inum].size = new_size;

        // force write to disk
        msync(image, image_size, MS_SYNC);

        message_t response;
        reply_success(&response);
        return;
    } else {
        if (first_block + 1 >= DIRECT_PTRS) {
            // not that much blocks
            err();
            return;
        }
        // get the second block
        int data_block_addr2 = (int)itable[inum].direct[first_block + 1];
        int data_block_idx2 = data_block_addr2 - s->data_region_addr;

        if (data_block_addr2 == -1) {
            // create and write in a new block
            int new_block_index = -1;

            new_block_index = get_free_bit(data_bitmap->bits, s->num_data);
            if (new_block_index == -1) {
                // no empty data block
                err();
                return;
            }
            set_bit(data_bitmap->bits, new_block_index, 1);
            data_block_addr2 = new_block_index + s->data_region_addr;
            data_block_idx2 = new_block_index;
        
            itable[inum].direct[first_block + 1] = data_block_addr2;
        } else {
            // if data block not valid, reply -1
            if (get_bit(data_bitmap->bits, data_block_idx2) != 1) {
                err();
                return;
            }
        }

        memcpy(data_start, buffer, nbytes - bytes_left);
        memcpy(blocks[data_block_addr2], buffer + nbytes - bytes_left, bytes_left);

        // update size
        int new_size = size;
        if (offset + nbytes > size) {
            new_size = offset + nbytes;
        }
        itable[inum].size = new_size;

        // force write to disk
        msync(image, image_size, MS_SYNC);

        message_t response;
        reply_success(&response);
        return;
    }
    err();
}

void handle_creat(int pinum, int type, char *name, char *blocks[]) {
    // if pinum not valid, reply -1
    if (pinum < 0 || pinum >= s->num_inodes) {
        err();
        return;
    }
    if (get_bit(inode_bitmap->bits, pinum) != 1) {
        err();
        return;
    }

    // if parent is not a dir, reply -1
    if (itable[pinum].type != UFS_DIRECTORY) {
        err();
        return;
    }

    int dir_size = itable[pinum].size;
    // if it's an empty dir, reply -1
    if (dir_size < sizeof(dir_ent_t)) {
        err();
        return;
    }

    int data_block_addr = (int)itable[pinum].direct[0];
    if (data_block_addr == -1) {
        err();
        return;
    }
    int data_block_index = data_block_addr - s->data_region_addr;
    // if data block not valid, reply -1
    if (get_bit(data_bitmap->bits, data_block_index) != 1) {
        err();
        return;
    }

    dir_block_t *dir = (dir_block_t*)blocks[data_block_addr];

    for (int i = 0; i < 128; i++) {
        if (dir->entries[i].inum == -1) {
            continue;
        }
        if (strcmp(dir->entries[i].name, name) == 0) {
            if (itable[dir->entries[i].inum].type == type) {
                // file/dir found, reply success
                message_t response;
                reply_success(&response);
                return;
            }
        }
    }

    // create a file
    for (int i = 0; i < 128; i++) {
        if (dir->entries[i].inum != -1) {
            continue;
        }

        // find an empty inode
        int inum = get_free_bit(inode_bitmap->bits, s->num_inodes);

        if (inum == -1) {
            // no empty inode
            err();
            return;
        }
        set_bit(inode_bitmap->bits, inum, 1);

        dir->entries[i].inum = inum;
        strcpy(dir->entries[i].name, name);

        itable[inum].type = type;
        for (int j = 0; j < DIRECT_PTRS; j++) {
            itable[inum].direct[j] = -1;
        }

        if (type == UFS_REGULAR_FILE) {
            itable[inum].size = 0;
        } else if (type == UFS_DIRECTORY) {
            // write out new dir contents to new data block
            int dir_index = get_free_bit(data_bitmap->bits, s->num_data);
            if (dir_index == -1) {
                // no empty datablock
                err();
            }
            int dir_addr = dir_index + s->data_region_addr;

            set_bit(data_bitmap->bits, dir_index, 1);
            dir_block_t *new_dir = (dir_block_t*) blocks[dir_addr];
            
            strcpy(new_dir->entries[0].name, ".");
            new_dir->entries[0].inum = inum;

            strcpy(new_dir->entries[1].name, "..");
            new_dir->entries[1].inum = pinum;

            for (i = 2; i < 128; i++)
            new_dir->entries[i].inum = -1;

            itable[inum].size = 2 * sizeof(dir_ent_t);
            itable[inum].direct[0] = dir_addr;
        }
        itable[pinum].size += sizeof(dir_ent_t);

        // force write to disk
        msync(image, image_size, MS_SYNC);

        message_t response;
        reply_success(&response);
        return;
    }
    // dir is full, reply -1;
    err();
}

void handle_unlink(int pinum, char *name, char *blocks[]) {
    // if pinum not valid, reply -1
    if (pinum < 0 || pinum >= s->num_inodes) {
        err();
        return;
    }
    if (get_bit(inode_bitmap->bits, pinum) != 1) {
        err();
        return;
    }

    // if not a dir, reply -1
    if (itable[pinum].type != UFS_DIRECTORY) {
        err();
        return;
    }

    int data_block_addr = (int)itable[pinum].direct[0];
    if (data_block_addr == -1) {
        err();
        return;
    }
    int data_block_index = data_block_addr - s->data_region_addr;
    // if data block not valid, reply -1
    if (get_bit(data_bitmap->bits, data_block_index) != 1) {
        err();
        return;
    }

    dir_block_t *dir = (dir_block_t*)blocks[data_block_addr];

    int dir_size = itable[pinum].size;
    // if it's an empty dir, reply success
    if (dir_size < sizeof(dir_ent_t)) {
        message_t response;
        reply_success(&response);
        return;
    }

    for (int i = 0; i < 128; i++) {
        if (dir->entries[i].inum == -1) {
            continue;
        }
        if (strcmp(dir->entries[i].name, name) == 0) {
            // file/dir found, unlink
            int file_inum = dir->entries[i].inum;
            int type = itable[file_inum].type;
            int size = itable[file_inum].size;
            if (type == UFS_DIRECTORY) {
                if (size > 2 * sizeof(dir_ent_t)) {
                    // dir not empty
                    err();
                    return;
                }
            }
            int file_block_num = itable[file_inum].size / UFS_BLOCK_SIZE;
            if (file_block_num % UFS_BLOCK_SIZE != 0) {
                file_block_num++;
            }

            dir->entries[i].inum = -1;
            itable[pinum].size -= sizeof(dir_ent_t);

            // clear file data bitmap
            for (int j = 0; j < file_block_num; j++) {
                int file_addr = (int)itable[file_inum].direct[j];
                int file_block_idx = file_addr - s->data_region_addr;
                set_bit(data_bitmap->bits, file_block_idx, 0);
            }

            // clear file inode bitmap
            set_bit(inode_bitmap->bits, file_inum, 0);

            // force write to disk
            msync(image, image_size, MS_SYNC);

            message_t response;
            reply_success(&response);
            return;
        }
    }

    // file not found, reply success;
    message_t response;
    reply_success(&response);
}

// server code
int main(int argc, char *argv[]) {
    if(argc != 3) {
        fprintf(stderr, "usage: server [portnum] [file-system-image]\n");
        exit(1);
    }
    int fd = open(argv[2], O_RDWR);
    if(fd < 0) {
        fprintf(stderr, "image does not exist\n");
        exit(1);
    }

    struct stat sbuf;
    int rc = fstat(fd, &sbuf);
    if(rc < 0) {
        fprintf(stderr, "image does not exist\n");
        exit(1);
    }

    image_size = (int) sbuf.st_size;

    image = mmap(NULL, image_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    assert(image != MAP_FAILED);

    s = (super_t*) image;

    int total_blocks = 1 + s->inode_bitmap_len + s->data_bitmap_len + s->inode_region_len + s->data_region_len;

    // pointers to all blocks
    char *blocks[total_blocks];
    for (int i = 0; i < total_blocks; i++) {
        blocks[i] = (char*)image + i * UFS_BLOCK_SIZE;
    }
    // assign pointers
    inode_bitmap = (bitmap_t*) blocks[s->inode_bitmap_addr];
    data_bitmap = (bitmap_t*) blocks[s->data_bitmap_addr];
    itable = (inode_t*) blocks[s->inode_region_addr];

    signal(SIGINT, intHandler);
    
    int port = atoi(argv[1]);
    sd = UDP_Open(port);
    assert(sd > -1);

    while (1) {
        message_t request;
        // server:: waiting
        int rc = UDP_Read(sd, &sockaddr, (char *) &request, sizeof(message_t));

        if (rc <= 0) {
            continue;
        }
        switch (request.mtype) {

            case MFS_LOOKUP:
                handle_lookup(request.inum, request.name, blocks);
                break;
                
            case MFS_STAT:
                handle_stat(request.inum, blocks);
                break;

            case MFS_WRITE:
                handle_write(request.inum, request.buffer, request.offset, request.nbytes, blocks);
                break;

            case MFS_READ:
                handle_read(request.inum, request.offset, request.nbytes, blocks);
                break;

            case MFS_CREAT:
                handle_creat(request.inum, request.type, request.name, blocks);
                break;

            case MFS_UNLINK:
                handle_unlink(request.inum, request.name, blocks);
                break;
            
            case MFS_SHUTDOWN:
                UDP_Close(sd);
                munmap(image, image_size);
                close(fd);
                exit(0);
                break;

            default:
                err();
                break;
        }
    }
    return 0; 
}