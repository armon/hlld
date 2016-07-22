#include <math.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <stdio.h>
#include <fcntl.h>
#include <strings.h>
#include <sys/mman.h>
#include <errno.h>
#include <sys/stat.h>
#include <syslog.h>
#include "bitmap.h"

/* Static declarations */
static int fill_buffer(int fileno, unsigned char* buf, uint64_t len);
static int flush_all_pages(hlld_bitmap *map);
static int flush_page(hlld_bitmap *map, uint64_t page, uint64_t size, uint64_t max_page);
extern inline int bitmap_getbit(hlld_bitmap *map, uint64_t idx);
extern inline void bitmap_setbit(hlld_bitmap *map, uint64_t idx);

/**
 * Returns a hlld_bitmap pointer from a file handle
 * that is already opened with read/write privileges.
 * @arg fileno The fileno
 * @arg len The length of the bitmap in bytes.
 * @arg mode The mode to use for the bitmap.
 * @arg map The output map. Will be initialized.
 * @return 0 on success. Negative on error.
 */
int bitmap_from_file(int fileno, uint64_t len, bitmap_mode mode, hlld_bitmap *map) {
    // Hack for old kernels and bad length checking
    if (len == 0) {
        return -EINVAL;
    }

    // Check for and clear NEW_BITMAP from the mode
    int new_bitmap = (mode & NEW_BITMAP) ? 1 : 0;
    mode &= ~NEW_BITMAP;

    // Handle each mode
    int flags;
    int newfileno;
    if (mode == SHARED) {
        flags = MAP_SHARED;
        newfileno = dup(fileno);
        if (newfileno < 0) return -errno;

    } else if (mode == PERSISTENT) {
        flags = MAP_ANON | MAP_PRIVATE;
        newfileno = dup(fileno);
        if (newfileno < 0) return -errno;

    } else if (mode == ANONYMOUS) {
        flags = MAP_ANON | MAP_PRIVATE;
        newfileno = -1;

    } else {
        return -1;
    }

    // Perform the map in
    unsigned char* addr = mmap(NULL, len, PROT_READ|PROT_WRITE,
            flags, ((mode == PERSISTENT) ? -1 : newfileno), 0);

    // Check for an error, otherwise return
    if (addr == MAP_FAILED) {
        perror("mmap failed!");
        if (newfileno >= 0) {
            close(newfileno);
        }
        return -errno;
    }

    // Provide some advise on how the memory will be used
    int res;
    if (mode == SHARED) {
        res = madvise(addr, len, MADV_WILLNEED);
        if (res != 0) {
            perror("Failed to call madvise() [MADV_WILLNEED]");
        }
        res = madvise(addr, len, MADV_RANDOM);
        if (res != 0) {
            perror("Failed to call madvise() [MADV_RANDOM]");
        }
    }

    // For the PERSISTENT case, we manually track
    // dirty pages, and need a bit field for this
    if (mode == PERSISTENT) {
        // For existing bitmaps we need to read in the data
        // since we cannot use the kernel to fault it in
        if (!new_bitmap && (res = fill_buffer(newfileno, addr, len))) {
            munmap(addr, len);
            if (newfileno >= 0) close(newfileno);
            return res;
        }
    }

    // Allocate space for the map
    map->mode = mode;
    map->fileno = newfileno;
    map->size = len;
    map->mmap = addr;
    return 0;
}

/*
 * Populates a buffer with the contents of a file
 */
static int fill_buffer(int fileno, unsigned char* buf, uint64_t len) {
    uint64_t total_read = 0;
    ssize_t more;
    while (total_read < len) {
        more = pread(fileno, buf+total_read, len-total_read, total_read);
        if (more == 0)
            break;
        else if (more < 0 && errno != EINTR) {
            perror("Failed to fill the bitmap buffer!");
            return -errno;
        } else
            total_read += more;
    }
    return 0;
}


/**
 * Returns a hlld_bitmap pointer from a filename.
 * Opens the file with read/write privileges. If create
 * is true, then a file will be created if it does not exist.
 * If the file cannot be opened, NULL will be returned.
 * @arg fileno The fileno
 * @arg len The length of the bitmap in bytes.
 * @arg create If 1, then the file will be created if it does not exist.
 * @arg resize If 1, then the file will be expanded to len
 * @arg mode The mode to use for the bitmap.
 * @arg map The output map. Will be initialized.
 * @return 0 on success. Negative on error.
 */
int bitmap_from_filename(char* filename, uint64_t len, int create, bitmap_mode mode, hlld_bitmap *map) {
    // Get the flags
    int flags = O_RDWR;
    if (create) {
        flags |= O_CREAT;
    }

    // Open the file
    int fileno = open(filename, flags, 0644);
    if (fileno == -1) {
        perror("open failed on bitmap!");
        return -errno;
    }

    // Check if we need to resize
    bitmap_mode extra_flags = 0;
    if (create) {
        struct stat buf;
        int res = fstat(fileno, &buf);
        if (res != 0) {
            perror("fstat failed on bitmap!");
            close(fileno);
            return -errno;
        }

        // Only ever truncate a new file, never resize
        // an existing file
        if ((uint64_t)buf.st_size == 0) {
            extra_flags |= NEW_BITMAP;
            res = ftruncate(fileno, len);
            if (res != 0) {
                perror("ftrunctate failed on the bitmap!");
                close(fileno);
                return -errno;
            }

        // Log an error if we are trying to change the
        // size of a file that has non-zero length
        } else if ((uint64_t)buf.st_size != len) {
            syslog(LOG_ERR, "File size does not match length but is already truncated!");
            close(fileno);
            return -1;
        }
    }

    // Use the filehandler mode
    int res = bitmap_from_file(fileno, len, mode | extra_flags, map);

    // Handle is dup'ed, we can close
    close(fileno);

    // Delete the file if we created it and had an error
    if (res && extra_flags & NEW_BITMAP && unlink(filename)) {
        perror("unlink failed!");
        syslog(LOG_ERR, "Failed to unlink new file %s", filename);
    }
    return res;
}


/**
 * Flushes the bitmap back to disk. This is
 * a syncronous operation. It is a no-op for
 * ANONYMOUS bitmaps.
 * @arg map The bitmap
 * @returns 0 on success, negative failure.
 */
int bitmap_flush(hlld_bitmap *map) {
    // Return if there is no map provided
    if (map == NULL) return -EINVAL;

    // Do nothing for anonymous maps
    int res;
    if (map->mode == ANONYMOUS || map->mmap == NULL)
        return 0;

    // For SHARED, we can use an msync and let the kernel deal
    else if (map->mode == SHARED) {
        res = msync(map->mmap, map->size, MS_SYNC);
        if (res == -1) return -errno;

    } else if (map->mode == PERSISTENT) {
        if ((res = flush_all_pages(map)))
            return res;
    }

    // SHARED / PERSISTENT both have a file backing
    res = fsync(map->fileno);
    if (res == -1) return -errno;
    return 0;
}


/**
 * Flushes all the pages of the bitmap.
 */
static int flush_all_pages(hlld_bitmap *map) {
    uint64_t pages = map->size / 4096 + ((map->size % 4096) ? 1 : 0);
    int res = 0;
    for (uint64_t i=0; i < pages; i++) {
        res = flush_page(map, i, map->size, pages - 1);
    }
    return res;
}


/**
 * Flushes out a single page that is dirty
 */
static int flush_page(hlld_bitmap *map, uint64_t page, uint64_t size, uint64_t max_page) {
    int res, total = 0;
    uint64_t offset = page * 4096;

    // The last page may need a write size < 4096
    int should_write = 4096;
    if (page == max_page && size % 4096) {
        should_write = size % 4096;
    }

    while (total < should_write) {
        res = pwrite(map->fileno, map->mmap + offset + total,
                should_write - total, offset + total);
        if (res == -1 && errno != EINTR)
            return -errno;
        else
            total += res;
    }
    return 0;
}


/**
 * Closes and flushes the bitmap. This is
 * a syncronous operation. It is a no-op for
 * ANONYMOUS bitmaps. The caller should free()
 * the structure after.
 * @arg map The bitmap
 * @returns 0 on success, negative on failure.
 */
int bitmap_close(hlld_bitmap *map) {
    // Return if there is no map provided
    if (map == NULL) return -EINVAL;

    // Flush first
    int res = bitmap_flush(map);
    if (res != 0) return res;

    // Unmap the file
    res = munmap(map->mmap, map->size);
    if (res != 0) return -errno;

    // Close the file descriptor if file backed
    if (map->mode != ANONYMOUS) {
       res = close(map->fileno);
       if (res != 0) return -errno;
    }

    // Cleanup
    map->mmap = NULL;
    map->fileno = -1;
    return 0;
}

