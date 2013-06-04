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
#include "set_manager.h"

START_TEST(test_mgr_init_destroy)
{
    hlld_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    hlld_setmgr *mgr;
    res = init_set_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = destroy_set_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_mgr_create_drop)
{
    hlld_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    hlld_setmgr *mgr;
    res = init_set_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = setmgr_create_set(mgr, "foo1", NULL);
    fail_unless(res == 0);

    res = setmgr_drop_set(mgr, "foo1");
    fail_unless(res == 0);

    res = destroy_set_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_mgr_create_double_drop)
{
    hlld_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    hlld_setmgr *mgr;
    res = init_set_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = setmgr_create_set(mgr, "dub1", NULL);
    fail_unless(res == 0);

    res = setmgr_drop_set(mgr, "dub1");
    fail_unless(res == 0);

    res = setmgr_drop_set(mgr, "dub1");
    fail_unless(res == -1);

    res = destroy_set_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_mgr_list)
{
    hlld_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    hlld_setmgr *mgr;
    res = init_set_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = setmgr_create_set(mgr, "bar1", NULL);
    fail_unless(res == 0);
    res = setmgr_create_set(mgr, "bar2", NULL);
    fail_unless(res == 0);

    hlld_set_list_head *head;
    res = setmgr_list_sets(mgr, NULL, &head);
    fail_unless(res == 0);
    fail_unless(head->size == 2);

    int has_bar1 = 0;
    int has_bar2 = 0;

    hlld_set_list *node = head->head;
    while (node) {
        if (strcmp(node->set_name, "bar1") == 0)
            has_bar1 = 1;
        else if (strcmp(node->set_name, "bar2") == 0)
            has_bar2 = 1;
        node = node->next;
    }
    fail_unless(has_bar1);
    fail_unless(has_bar2);

    res = setmgr_drop_set(mgr, "bar1");
    fail_unless(res == 0);
    res = setmgr_drop_set(mgr, "bar2");
    fail_unless(res == 0);

    setmgr_cleanup_list(head);

    res = destroy_set_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_mgr_list_prefix)
{
    hlld_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    hlld_setmgr *mgr;
    res = init_set_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = setmgr_create_set(mgr, "bar1", NULL);
    fail_unless(res == 0);
    res = setmgr_create_set(mgr, "bar2", NULL);
    fail_unless(res == 0);
    res = setmgr_create_set(mgr, "junk1", NULL);
    fail_unless(res == 0);

    hlld_set_list_head *head;
    res = setmgr_list_sets(mgr, "bar", &head);
    fail_unless(res == 0);
    fail_unless(head->size == 2);

    int has_bar1 = 0;
    int has_bar2 = 0;

    hlld_set_list *node = head->head;
    while (node) {
        if (strcmp(node->set_name, "bar1") == 0)
            has_bar1 = 1;
        else if (strcmp(node->set_name, "bar2") == 0)
            has_bar2 = 1;
        node = node->next;
    }
    fail_unless(has_bar1);
    fail_unless(has_bar2);

    res = setmgr_drop_set(mgr, "bar1");
    fail_unless(res == 0);
    res = setmgr_drop_set(mgr, "bar2");
    fail_unless(res == 0);
    res = setmgr_drop_set(mgr, "junk1");
    fail_unless(res == 0);

    setmgr_cleanup_list(head);

    res = destroy_set_manager(mgr);
    fail_unless(res == 0);
}
END_TEST


START_TEST(test_mgr_list_no_sets)
{
    hlld_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    hlld_setmgr *mgr;
    res = init_set_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    hlld_set_list_head *head;
    res = setmgr_list_sets(mgr, NULL, &head);
    fail_unless(res == 0);
    fail_unless(head->size == 0);
    setmgr_cleanup_list(head);

    res = destroy_set_manager(mgr);
    fail_unless(res == 0);
}
END_TEST


START_TEST(test_mgr_add_keys)
{
    hlld_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    hlld_setmgr *mgr;
    res = init_set_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = setmgr_create_set(mgr, "zab1", NULL);
    fail_unless(res == 0);

    char *keys[] = {"hey","there","person"};
    res = setmgr_set_keys(mgr, "zab1", (char**)&keys, 3);
    fail_unless(res == 0);

    res = setmgr_drop_set(mgr, "zab1");
    fail_unless(res == 0);

    res = destroy_set_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_mgr_add_no_set)
{
    hlld_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    hlld_setmgr *mgr;
    res = init_set_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    char *keys[] = {"hey","there","person"};
    res = setmgr_set_keys(mgr, "noop1", (char**)&keys, 3);
    fail_unless(res == -1);

    res = destroy_set_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

/* Flush */
START_TEST(test_mgr_flush_no_set)
{
    hlld_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    hlld_setmgr *mgr;
    res = init_set_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = setmgr_flush_set(mgr, "noop1");
    fail_unless(res == -1);

    res = destroy_set_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_mgr_flush)
{
    hlld_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    hlld_setmgr *mgr;
    res = init_set_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = setmgr_create_set(mgr, "zab3", NULL);
    fail_unless(res == 0);

    res = setmgr_flush_set(mgr, "zab3");
    fail_unless(res == 0);

    res = setmgr_drop_set(mgr, "zab3");
    fail_unless(res == 0);

    res = destroy_set_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

/* Unmap */
START_TEST(test_mgr_unmap_no_set)
{
    hlld_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    hlld_setmgr *mgr;
    res = init_set_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = setmgr_unmap_set(mgr, "noop2");
    fail_unless(res == -1);

    res = destroy_set_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_mgr_unmap)
{
    hlld_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    hlld_setmgr *mgr;
    res = init_set_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = setmgr_create_set(mgr, "zab4", NULL);
    fail_unless(res == 0);

    res = setmgr_unmap_set(mgr, "zab4");
    fail_unless(res == 0);

    res = setmgr_drop_set(mgr, "zab4");
    fail_unless(res == 0);

    res = destroy_set_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_mgr_unmap_add_keys)
{
    hlld_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    hlld_setmgr *mgr;
    res = init_set_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = setmgr_create_set(mgr, "zab5", NULL);
    fail_unless(res == 0);

    res = setmgr_unmap_set(mgr, "zab5");
    fail_unless(res == 0);

    // Try to add keys now
    char *keys[] = {"hey","there","person"};
    res = setmgr_set_keys(mgr, "zab5", (char**)&keys, 3);
    fail_unless(res == 0);

    res = setmgr_drop_set(mgr, "zab5");
    fail_unless(res == 0);

    res = destroy_set_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

/* Clear command */
START_TEST(test_mgr_clear_no_set)
{
    hlld_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    hlld_setmgr *mgr;
    res = init_set_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = setmgr_clear_set(mgr, "noop2");
    fail_unless(res == -1);

    res = destroy_set_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_mgr_clear_not_proxied)
{
    hlld_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    hlld_setmgr *mgr;
    res = init_set_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = setmgr_create_set(mgr, "dub1", NULL);
    fail_unless(res == 0);

    // Should be not proxied still
    res = setmgr_clear_set(mgr, "dub1");
    fail_unless(res == -2);

    res = setmgr_drop_set(mgr, "dub1");
    fail_unless(res == 0);

    res = destroy_set_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_mgr_clear)
{
    hlld_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    hlld_setmgr *mgr;
    res = init_set_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = setmgr_create_set(mgr, "dub2", NULL);
    fail_unless(res == 0);

    res = setmgr_unmap_set(mgr, "dub2");
    fail_unless(res == 0);

    // Should be not proxied still
    res = setmgr_clear_set(mgr, "dub2");
    fail_unless(res == 0);

    // Force a vacuum
    setmgr_vacuum(mgr);

    res = setmgr_create_set(mgr, "dub2", NULL);
    fail_unless(res == 0);

    res = setmgr_drop_set(mgr, "dub2");
    fail_unless(res == 0);

    res = destroy_set_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_mgr_clear_reload)
{
    hlld_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    hlld_setmgr *mgr;
    res = init_set_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = setmgr_create_set(mgr, "zab9", NULL);
    fail_unless(res == 0);

    // Try to add keys now
    char *keys[] = {"hey","there","person"};
    res = setmgr_set_keys(mgr, "zab9", (char**)&keys, 3);
    fail_unless(res == 0);

    res = setmgr_unmap_set(mgr, "zab9");
    fail_unless(res == 0);

    res = setmgr_clear_set(mgr, "zab9");
    fail_unless(res == 0);

    // Force a vacuum
    setmgr_vacuum(mgr);

    // This should rediscover
    res = setmgr_create_set(mgr, "zab9", NULL);
    fail_unless(res == 0);

    // Try to check keys now
    uint64_t size;
    res = setmgr_set_size(mgr, "zab9", &size);
    fail_unless(res == 0);
    fail_unless(size == 3);

    res = setmgr_drop_set(mgr, "zab9");
    fail_unless(res == 0);

    res = destroy_set_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

/* List Cold */
START_TEST(test_mgr_list_cold_no_sets)
{
    hlld_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    hlld_setmgr *mgr;
    res = init_set_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    hlld_set_list_head *head;
    res = setmgr_list_cold_sets(mgr, &head);
    fail_unless(res == 0);
    fail_unless(head->size == 0);
    setmgr_cleanup_list(head);

    res = destroy_set_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_mgr_list_cold)
{
    hlld_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    hlld_setmgr *mgr;
    res = init_set_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = setmgr_create_set(mgr, "zab6", NULL);
    fail_unless(res == 0);
    res = setmgr_create_set(mgr, "zab7", NULL);
    fail_unless(res == 0);

    // Force vacuum so that these are noticed by the cold list
    setmgr_vacuum(mgr);

    hlld_set_list_head *head;
    res = setmgr_list_cold_sets(mgr, &head);
    fail_unless(res == 0);
    fail_unless(head->size == 0);

    // Check the keys in one, so that it stays hot
    char *keys[] = {"hey","there","person"};
    res = setmgr_set_keys(mgr, "zab6", (char**)&keys, 3);
    fail_unless(res == 0);

    // Check cold again
    res = setmgr_list_cold_sets(mgr, &head);
    fail_unless(res == 0);
    fail_unless(head->size == 1);

    int has_zab6 = 0;
    int has_zab7 = 0;

    hlld_set_list *node = head->head;
    while (node) {
        if (strcmp(node->set_name, "zab6") == 0)
            has_zab6 = 1;
        else if (strcmp(node->set_name, "zab7") == 0)
            has_zab7 = 1;
        node = node->next;
    }
    fail_unless(!has_zab6);
    fail_unless(has_zab7);

    res = setmgr_drop_set(mgr, "zab6");
    fail_unless(res == 0);
    res = setmgr_drop_set(mgr, "zab7");
    fail_unless(res == 0);
    setmgr_cleanup_list(head);

    res = destroy_set_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

/* Unmap in memory */
START_TEST(test_mgr_unmap_in_mem)
{
    hlld_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);
    config.in_memory = 1;

    hlld_setmgr *mgr;
    res = init_set_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = setmgr_create_set(mgr, "mem1", NULL);
    fail_unless(res == 0);

    // Try to add keys now
    char *keys[] = {"hey","there","person"};
    res = setmgr_set_keys(mgr, "mem1", (char**)&keys, 3);
    fail_unless(res == 0);

    res = setmgr_unmap_set(mgr, "mem1");
    fail_unless(res == 0);

    // Try to check keys now
    uint64_t size;
    res = setmgr_set_size(mgr, "mem1", &size);
    fail_unless(res == 0);
    fail_unless(size == 3);

    res = setmgr_drop_set(mgr, "mem1");
    fail_unless(res == 0);

    res = destroy_set_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

/* Custom config */
START_TEST(test_mgr_create_custom_config)
{
    hlld_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    hlld_setmgr *mgr;
    res = init_set_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    // Custom config
    hlld_config *custom = malloc(sizeof(hlld_config));
    memcpy(custom, &config, sizeof(hlld_config));
    custom->in_memory = 1;

    res = setmgr_create_set(mgr, "custom1", custom);
    fail_unless(res == 0);

    res = setmgr_drop_set(mgr, "custom1");
    fail_unless(res == 0);

    res = destroy_set_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

/* Close & Restore */

START_TEST(test_mgr_restore)
{
    hlld_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    hlld_setmgr *mgr;
    res = init_set_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = setmgr_create_set(mgr, "zab8", NULL);
    fail_unless(res == 0);

    char *keys[] = {"hey","there","person"};
    res = setmgr_set_keys(mgr, "zab8", (char**)&keys, 3);
    fail_unless(res == 0);

    // Shutdown
    res = destroy_set_manager(mgr);
    fail_unless(res == 0);

   // Restrore
    res = init_set_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    // Try to check keys now
    uint64_t size;
    res = setmgr_set_size(mgr, "zab8", &size);
    fail_unless(res == 0);
    fail_unless(size == 3);

    res = setmgr_drop_set(mgr, "zab8");
    fail_unless(res == 0);

    res = destroy_set_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

void test_mgr_cb(void *data, char *set_name, hlld_set* set) {
    (void)set_name;
    (void)set;
    int *out = data;
    *out = 1;
}

START_TEST(test_mgr_callback)
{
    hlld_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);
    config.in_memory = 1;

    hlld_setmgr *mgr;
    res = init_set_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = setmgr_create_set(mgr, "cb1", NULL);
    fail_unless(res == 0);

    int val = 0;
    res = setmgr_set_cb(mgr, "cb1", test_mgr_cb, &val);
    fail_unless(val == 1);

    res = setmgr_drop_set(mgr, "cb1");
    fail_unless(res == 0);

    res = destroy_set_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

