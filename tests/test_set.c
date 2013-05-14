#include <check.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <dirent.h>
#include "config.h"
#include "set.h"

static int set_out_special(const struct dirent *d) {
    const char *name = d->d_name;
    if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) {
        return 0;
    }
    return 1;
}

static int delete_dir(char *path) {
    // Delete the files
    struct dirent **namelist = NULL;
    int num;

    // set only data dirs, in sorted order
    num = scandir(path, &namelist, set_out_special, NULL);
    if (num == -1) return 0;

    // Free the memory associated with scandir
    for (int i=0; i < num; i++) {
        char *file_path = join_path(path, namelist[i]->d_name);
        if (unlink(file_path)) {
            printf("Failed to delete: %s. %s\n", file_path, strerror(errno));
        }
        free(file_path);
    }

    // Free the memory associated with scandir
    for (int i=0; i < num; i++) {
        free(namelist[i]);
    }
    if (namelist != NULL) free(namelist);

    // Delete the directory
    if (rmdir(path)) {
        printf("Failed to delete dir: %s. %s\n", path, strerror(errno));
    }
    return num;
}

START_TEST(test_set_init_destroy)
{
    hlld_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    hlld_set *set = NULL;
    res = init_set(&config, "test_set", 0, &set);
    fail_unless(res == 0);

    res = destroy_set(set);
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_set_init_discover_destroy)
{
    hlld_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    hlld_set *set = NULL;
    res = init_set(&config, "test_set", 1, &set);
    fail_unless(res == 0);
    fail_unless(hset_is_proxied(set) == 0);

    res = destroy_set(set);
    fail_unless(res == 0);
    fail_unless(delete_dir("/tmp/hlld/hlld.test_set") == 2);
}
END_TEST

START_TEST(test_set_init_discover_delete)
{
    hlld_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    hlld_set *set = NULL;
    res = init_set(&config, "test_set2", 1, &set);
    fail_unless(res == 0);
    fail_unless(hset_is_proxied(set) == 0);

    res = hset_delete(set);
    fail_unless(res == 0);

    res = destroy_set(set);
    fail_unless(res == 0);
    fail_unless(delete_dir("/tmp/hlld/hlld.test_set2") == 0);
}
END_TEST

START_TEST(test_set_init_proxied)
{
    hlld_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    hlld_set *set = NULL;
    res = init_set(&config, "test_set3", 0, &set);
    fail_unless(res == 0);

    set_counters *counters = hset_counters(set);
    fail_unless(counters->sets== 0);
    fail_unless(counters->page_ins == 0);
    fail_unless(counters->page_outs == 0);

    fail_unless(hset_is_proxied(set) == 1);
    fail_unless(hset_byte_size(set) == 3280);
    fail_unless(hset_size(set) == 0);

    res = destroy_set(set);
    fail_unless(res == 0);
    fail_unless(delete_dir("/tmp/hlld/hlld.test_set3") == 0);
}
END_TEST

START_TEST(test_set_add)
{
    hlld_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    hlld_set *set = NULL;
    res = init_set(&config, "test_set4", 0, &set);
    fail_unless(res == 0);

    set_counters *counters = hset_counters(set);

    // Check all the keys get added
    char buf[100];
    for (int i=0;i<10000;i++) {
        snprintf((char*)&buf, 100, "foobar%d", i);
        res = hset_add(set, (char*)&buf);
        fail_unless(res == 0);
    }

    fail_unless(hset_size(set) > 9800 && hset_size(set) < 10200);
    fail_unless(hset_byte_size(set) == 3280);
    fail_unless(counters->sets == 10000);

    res = destroy_set(set);
    fail_unless(res == 0);
    fail_unless(delete_dir("/tmp/hlld/hlld.test_set4") == 2);
}
END_TEST

START_TEST(test_set_restore)
{
    hlld_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    hlld_set *set = NULL;
    res = init_set(&config, "test_set5", 0, &set);
    fail_unless(res == 0);

    // Check all the keys get added
    char buf[100];
    for (int i=0;i<10000;i++) {
        snprintf((char*)&buf, 100, "foobar%d", i);
        res = hset_add(set, (char*)&buf);
        fail_unless(res == 0);
    }

    // Get the size
    uint64_t size = hset_size(set);

    // Destroy the set
    res = destroy_set(set);
    fail_unless(res == 0);

    // Remake the set
    res = init_set(&config, "test_set5", 1, &set);
    fail_unless(res == 0);

    // Re-check
    fail_unless(hset_size(set) == size);
    fail_unless(hset_byte_size(set) == 3280);

    res = destroy_set(set);
    fail_unless(res == 0);
    fail_unless(delete_dir("/tmp/hlld/hlld.test_set5") == 2);
}
END_TEST

START_TEST(test_set_flush)
{
    hlld_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    hlld_set *set = NULL;
    res = init_set(&config, "test_set6", 0, &set);
    fail_unless(res == 0);

    // Check all the keys get added
    char buf[100];
    for (int i=0;i<10000;i++) {
        snprintf((char*)&buf, 100, "foobar%d", i);
        res = hset_add(set, (char*)&buf);
        fail_unless(res == 0);
    }

    // Flush
    fail_unless(hset_flush(set) == 0);

    // Remake the set
    hlld_set *set2 = NULL;
    res = init_set(&config, "test_set6", 1, &set2);
    fail_unless(res == 0);

    // Re-check
    fail_unless(hset_size(set2) == hset_size(set));
    fail_unless(hset_byte_size(set2) == hset_byte_size(set));

    // Destroy the set
    res = destroy_set(set);
    fail_unless(res == 0);

    res = destroy_set(set2);
    fail_unless(res == 0);
    fail_unless(delete_dir("/tmp/hlld/hlld.test_set6") == 2);
}
END_TEST

START_TEST(test_set_add_in_mem)
{
    hlld_config config;
    int res = config_from_filename(NULL, &config);
    config.in_memory = 1;
    fail_unless(res == 0);

    hlld_set *set = NULL;
    res = init_set(&config, "test_set7", 0, &set);
    fail_unless(res == 0);

    set_counters *counters = hset_counters(set);

    // Check all the keys get added
    char buf[100];
    for (int i=0;i<10000;i++) {
        snprintf((char*)&buf, 100, "foobar%d", i);
        res = hset_add(set, (char*)&buf);
        fail_unless(res == 0);
    }

    fail_unless(hset_size(set) > 9800 && hset_size(set) < 10200);
    fail_unless(hset_byte_size(set) == 3280);
    fail_unless(counters->sets == 10000);

    res = destroy_set(set);
    fail_unless(res == 0);
    fail_unless(delete_dir("/tmp/hlld/hlld.test_set7") == 1);
}
END_TEST

START_TEST(test_set_page_out)
{
    hlld_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    hlld_set *set = NULL;
    res = init_set(&config, "test_set10", 0, &set);
    fail_unless(res == 0);

    set_counters *counters = hset_counters(set);

    // Check all the keys get added
    char buf[100];
    for (int i=0;i<10000;i++) {
        snprintf((char*)&buf, 100, "foobar%d", i);
        res = hset_add(set, (char*)&buf);
        fail_unless(res == 0);
    }

    uint64_t size = hset_size(set);
    fail_unless(size > 9800 && size < 10200);
    fail_unless(hset_close(set) == 0);
    fail_unless(counters->page_outs == 1);
    fail_unless(counters->page_ins == 0);

    // Force fault in with another add
    res = hset_add(set, (char*)&buf);
    fail_unless(res == 0);

    // Check the size again
    fail_unless(hset_size(set) == size);
    fail_unless(counters->sets == 10001);
    fail_unless(counters->page_outs == 1);
    fail_unless(counters->page_ins == 1);

    res = destroy_set(set);
    fail_unless(res == 0);
    fail_unless(delete_dir("/tmp/hlld/hlld.test_set10") == 2);
}
END_TEST

