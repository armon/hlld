#include <check.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include "bitmap.h"

/*
hlld_bitmap *bitmap_from_file(int fileno, size_t len) {
*/

START_TEST(make_anonymous_bitmap)
{
    // Use -1 for anonymous
    hlld_bitmap map;
    int res = bitmap_from_file(-1, 4096, ANONYMOUS, &map);
    fail_unless(res == 0);
}
END_TEST

START_TEST(make_bitmap_zero_size)
{
    hlld_bitmap map;
    int res = bitmap_from_file(-1, 0, ANONYMOUS, &map);
    fail_unless(res == -EINVAL);
}
END_TEST

START_TEST(make_bitmap_bad_fileno)
{
    hlld_bitmap map;
    int res = bitmap_from_file(500, 4096, SHARED, &map);
    fail_unless(res == -EBADF);
}
END_TEST

START_TEST(make_bitmap_bad_fileno_persistent)
{
    hlld_bitmap map;
    int res = bitmap_from_file(500, 4096, PERSISTENT, &map);
    fail_unless(res == -EBADF);
}
END_TEST


/*
hlld_bitmap *bitmap_from_filename(char* filename, size_t len, int create, int resize) {
*/

START_TEST(make_bitmap_nofile)
{
    hlld_bitmap map;
    int res = bitmap_from_filename("/tmp/asdf123", 4096, 0, SHARED, &map);
    fail_unless(res == -ENOENT);
}
END_TEST

START_TEST(make_bitmap_nofile_persistent)
{
    hlld_bitmap map;
    int res = bitmap_from_filename("/tmp/asdf123", 4096, 0, PERSISTENT, &map);
    fail_unless(res == -ENOENT);
}
END_TEST

START_TEST(make_bitmap_nofile_create)
{
    hlld_bitmap map;
    int res = bitmap_from_filename("/tmp/mmap_nofile_create", 4096, 1, SHARED, &map);
    unlink("/tmp/mmap_nofile_create");
    fail_unless(res == 0);
}
END_TEST

START_TEST(make_bitmap_nofile_create_persistent)
{
    hlld_bitmap map;
    int res = bitmap_from_filename("/tmp/mmap_nofile_create_persist", 4096, 1, PERSISTENT, &map);
    unlink("/tmp/mmap_nofile_create_persist");
    fail_unless(res == 0);
}
END_TEST


/*
 * int bitmap_flush(hlld_bitmap *map) {
 */
START_TEST(flush_bitmap_anonymous)
{
    hlld_bitmap map;
    int res = bitmap_from_file(-1, 4096, ANONYMOUS, &map);
    fail_unless(res == 0);
    fail_unless(bitmap_flush(&map) == 0);
}
END_TEST

START_TEST(flush_bitmap_file)
{
    hlld_bitmap map;
    int res = bitmap_from_filename("/tmp/mmap_flush_bitmap", 8196, 1, SHARED, &map);
    fail_unless(res == 0);
    fail_unless(bitmap_flush(&map) == 0);
    unlink("/tmp/mmap_flush_bitmap");
}
END_TEST

START_TEST(flush_bitmap_file_persistent)
{
    hlld_bitmap map;
    int res = bitmap_from_filename("/tmp/mmap_flush_bitmap_persist", 8196,
            1, PERSISTENT, &map);
    fail_unless(res == 0);
    fail_unless(bitmap_flush(&map) == 0);
    unlink("/tmp/mmap_flush_bitmap_persist");
}
END_TEST

START_TEST(flush_bitmap_null)
{
    fail_unless(bitmap_flush(NULL) == -EINVAL);
}
END_TEST

/*
 * int bitmap_close(hlld_bitmap *map) {
 */

START_TEST(close_bitmap_anonymous)
{
    hlld_bitmap map;
    int res = bitmap_from_file(-1, 4096, ANONYMOUS, &map);
    fail_unless(res == 0);
    fail_unless(bitmap_close(&map) == 0);
    fail_unless(map.mmap == NULL);
}
END_TEST

START_TEST(close_bitmap_file)
{
    hlld_bitmap map;
    int res = bitmap_from_filename("/tmp/mmap_close_bitmap", 8196, 1, SHARED, &map);
    fail_unless(res == 0);
    fail_unless(bitmap_close(&map) == 0);
    fail_unless(map.mmap == NULL);
    unlink("/tmp/mmap_close_bitmap");
}
END_TEST

START_TEST(close_bitmap_file_persistent)
{
    hlld_bitmap map;
    int res = bitmap_from_filename("/tmp/mmap_close_bitmap_persist", 8196, 1,
            PERSISTENT, &map);
    fail_unless(res == 0);
    fail_unless(bitmap_close(&map) == 0);
    fail_unless(map.mmap == NULL);
    unlink("/tmp/mmap_close_bitmap_persist");
}
END_TEST

START_TEST(double_close_bitmap_file)
{
    hlld_bitmap map;
    int res = bitmap_from_filename("/tmp/mmap_close_bitmap", 8196, 1, SHARED, &map);
    fail_unless(res == 0);
    fail_unless(bitmap_close(&map) == 0);
    fail_unless(map.mmap == NULL);
    unlink("/tmp/mmap_close_bitmap");
    fail_unless(bitmap_close(&map) < 0);
}
END_TEST

START_TEST(double_close_bitmap_file_persist)
{
    hlld_bitmap map;
    int res = bitmap_from_filename("/tmp/mmap_close_bitmap_persist",
            8196, 1, PERSISTENT, &map);
    fail_unless(res == 0);
    fail_unless(bitmap_close(&map) == 0);
    fail_unless(map.mmap == NULL);
    unlink("/tmp/mmap_close_bitmap_persist");
    fail_unless(bitmap_close(&map) < 0);
}
END_TEST

START_TEST(close_bitmap_null)
{
    fail_unless(bitmap_close(NULL) == -EINVAL);
}
END_TEST


/*
 *#define BITMAP_GETBIT(map, idx)
 */
START_TEST(getbit_bitmap_anonymous_zero)
{
    hlld_bitmap map;
    bitmap_from_file(-1, 4096, ANONYMOUS, &map);
    for (int idx = 0; idx < 4096*8 ; idx++) {
        fail_unless(bitmap_getbit((&map), idx) == 0);
    }
}
END_TEST

START_TEST(getbit_bitmap_anonymous_one)
{
    hlld_bitmap map;
    bitmap_from_file(-1, 4096, ANONYMOUS, &map);
    memset(map.mmap, 255, 4096);
    for (int idx = 0; idx < 4096*8 ; idx++) {
        fail_unless(bitmap_getbit((&map), idx) == 1);
    }
}
END_TEST

START_TEST(getbit_bitmap_file_zero)
{
    hlld_bitmap map;
    int res = bitmap_from_filename("/tmp/mmap_getbit_zero", 4096, 1, SHARED, &map);
    fail_unless(res == 0);
    for (int idx = 0; idx < 4096*8 ; idx++) {
        fail_unless(bitmap_getbit((&map), idx) == 0);
    }
    unlink("/tmp/mmap_getbit_zero");
}
END_TEST

START_TEST(getbit_bitmap_file_one)
{
    hlld_bitmap map;
    int res = bitmap_from_filename("/tmp/mmap_getbit_one", 4096, 1, SHARED, &map);
    fail_unless(res == 0);
    memset(map.mmap, 255, 4096);
    for (int idx = 0; idx < 4096*8 ; idx++) {
        fail_unless(bitmap_getbit((&map), idx) == 1);
    }
    unlink("/tmp/mmap_getbit_one");
}
END_TEST

START_TEST(getbit_bitmap_file_persist_zero)
{
    hlld_bitmap map;
    int res = bitmap_from_filename("/tmp/persist_getbit_zero", 4096, 1, PERSISTENT, &map);
    fail_unless(res == 0);
    for (int idx = 0; idx < 4096*8 ; idx++) {
        fail_unless(bitmap_getbit((&map), idx) == 0);
    }
    unlink("/tmp/persist_getbit_zero");
}
END_TEST

START_TEST(getbit_bitmap_file_persist_one)
{
    hlld_bitmap map;
    int res = bitmap_from_filename("/tmp/persist_getbit_one", 4096, 1, PERSISTENT, &map);
    fail_unless(res == 0);
    memset(map.mmap, 255, 4096);
    for (int idx = 0; idx < 4096*8 ; idx++) {
        fail_unless(bitmap_getbit((&map), idx) == 1);
    }
    unlink("/tmp/persist_getbit_one");
}
END_TEST

START_TEST(getbit_bitmap_anonymous_one_onebyte)
{
    hlld_bitmap map;
    bitmap_from_file(-1, 4096, ANONYMOUS, &map);
    map.mmap[1] = 128;
    fail_unless(bitmap_getbit((&map), 8) == 1);
}
END_TEST


/*
 *#define BITMAP_SETBIT(map, idx, val)
 */
START_TEST(setbit_bitmap_anonymous_one_byte)
{
    hlld_bitmap map;
    bitmap_from_file(-1, 4096, ANONYMOUS, &map);
    bitmap_setbit((&map), 1);
    fail_unless(map.mmap[0] == 64);
}
END_TEST

START_TEST(setbit_bitmap_anonymous_one_byte_aligned)
{
    hlld_bitmap map;
    bitmap_from_file(-1, 4096, ANONYMOUS, &map);
    bitmap_setbit((&map), 8);
    fail_unless(map.mmap[1] == 128);
}
END_TEST


START_TEST(setbit_bitmap_anonymous_one)
{
    hlld_bitmap map;
    bitmap_from_file(-1, 4096, ANONYMOUS, &map);
    for (int idx = 0; idx < 4096*8 ; idx++) {
        bitmap_setbit((&map), idx);
    }
    for (int idx = 0; idx < 4096; idx++) {
        fail_unless(map.mmap[idx] == 255);
    }
}
END_TEST

START_TEST(setbit_bitmap_file_one)
{
    hlld_bitmap map;
    int res = bitmap_from_filename("/tmp/mmap_setbit_one", 4096, 1, SHARED, &map);
    fail_unless(res == 0);
    for (int idx = 0; idx < 4096*8 ; idx++) {
        bitmap_setbit((&map), idx);
    }
    for (int idx = 0; idx < 4096; idx++) {
        fail_unless(map.mmap[idx] == 255);
    }
    unlink("/tmp/mmap_setbit_one");
}
END_TEST

START_TEST(setbit_bitmap_file_persist_one)
{
    hlld_bitmap map;
    int res = bitmap_from_filename("/tmp/persist_setbit_one", 4096, 1,
            PERSISTENT, &map);
    fail_unless(res == 0);
    for (int idx = 0; idx < 4096*8 ; idx++) {
        bitmap_setbit((&map), idx);
    }
    for (int idx = 0; idx < 4096; idx++) {
        fail_unless(map.mmap[idx] == 255);
    }
    unlink("/tmp/persist_setbit_one");
}
END_TEST


/**
 * Test that flush does indeed write to disk
 */
START_TEST(flush_does_write) {
    hlld_bitmap map;
    int res = bitmap_from_filename("/tmp/mmap_flush_write", 4096, 1, SHARED, &map);
    fchmod(map.fileno, 0777);
    fail_unless(res == 0);
    for (int idx = 0; idx < 4096*8 ; idx++) {
        bitmap_setbit((&map), idx);
    }
    bitmap_flush(&map);

    hlld_bitmap map2;
    res = bitmap_from_filename("/tmp/mmap_flush_write", 4096, 0, SHARED, &map2);
    fail_unless(res == 0);
    for (int idx = 0; idx < 4096; idx++) {
        fail_unless(map2.mmap[idx] == 255);
    }
    unlink("/tmp/mmap_flush_write");
}
END_TEST

START_TEST(close_does_flush) {
    hlld_bitmap map;
    int res = bitmap_from_filename("/tmp/mmap_close_flush", 4096, 1, SHARED, &map);
    fchmod(map.fileno, 0777);
    fail_unless(res == 0);
    for (int idx = 0; idx < 4096*8 ; idx++) {
        bitmap_setbit((&map), idx);
    }
    bitmap_close(&map);

    res = bitmap_from_filename("/tmp/mmap_close_flush", 4096, 0, SHARED, &map);
    fail_unless(res == 0);
    for (int idx = 0; idx < 4096; idx++) {
        fail_unless(map.mmap[idx] == 255);
    }
    unlink("/tmp/mmap_close_flush");
}
END_TEST

START_TEST(flush_does_write_persist) {
    hlld_bitmap map;
    int res = bitmap_from_filename("/tmp/persist_flush_write", 4096, 1, PERSISTENT, &map);
    fchmod(map.fileno, 0777);
    fail_unless(res == 0);
    for (int idx = 0; idx < 4096*8 ; idx++) {
        bitmap_setbit((&map), idx);
    }
    bitmap_flush(&map);

    hlld_bitmap map2;
    res = bitmap_from_filename("/tmp/persist_flush_write", 4096, 0,
            PERSISTENT, &map2);
    fail_unless(res == 0);
    for (int idx = 0; idx < 4096; idx++) {
        fail_unless(map2.mmap[idx] == 255);
    }
    unlink("/tmp/persist_flush_write");
}
END_TEST

START_TEST(close_does_flush_persist) {
    hlld_bitmap map;
    int res = bitmap_from_filename("/tmp/persist_close_flush", 4096, 1,
            PERSISTENT, &map);
    fchmod(map.fileno, 0777);
    fail_unless(res == 0);
    for (int idx = 0; idx < 4096*8 ; idx++) {
        bitmap_setbit((&map), idx);
    }
    bitmap_close(&map);

    res = bitmap_from_filename("/tmp/persist_close_flush", 4096, 0,
            PERSISTENT, &map);
    fail_unless(res == 0);
    for (int idx = 0; idx < 4096; idx++) {
        fail_unless(map.mmap[idx] == 255);
    }
    unlink("/tmp/persist_close_flush");
}
END_TEST

