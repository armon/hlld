#include <check.h>
#include <stdio.h>
#include <syslog.h>
#include "test_config.c"
#include "test_hll.c"
#include "test_bitmap.c"
#include "test_set.c"
#include "test_setmgr.c"
#include "test_art.c"

int main(void)
{
    setlogmask(LOG_UPTO(LOG_DEBUG));

    Suite *s1 = suite_create("hlld");
    TCase *tc1 = tcase_create("config");
    TCase *tc3 = tcase_create("bitmap");
    TCase *tc4 = tcase_create("hll");
    TCase *tc5 = tcase_create("set");
    TCase *tc6 = tcase_create("manager");
    TCase *tc7 = tcase_create("art");
    SRunner *sr = srunner_create(s1);
    int nf;

    // Add the config tests
    suite_add_tcase(s1, tc1);
    tcase_add_test(tc1, test_config_get_default);
    tcase_add_test(tc1, test_config_bad_file);
    tcase_add_test(tc1, test_config_empty_file);
    tcase_add_test(tc1, test_config_basic_config);
    tcase_add_test(tc1, test_config_basic_config_precision);
    tcase_add_test(tc1, test_validate_default_config);
    tcase_add_test(tc1, test_validate_bad_config);
    tcase_add_test(tc1, test_join_path_no_slash);
    tcase_add_test(tc1, test_join_path_with_slash);
    tcase_add_test(tc1, test_sane_log_level);
    tcase_add_test(tc1, test_sane_default_eps);
    tcase_add_test(tc1, test_sane_default_precision);
    tcase_add_test(tc1, test_sane_flush_interval);
    tcase_add_test(tc1, test_sane_cold_interval);
    tcase_add_test(tc1, test_sane_in_memory);
    tcase_add_test(tc1, test_sane_use_mmap);
    tcase_add_test(tc1, test_sane_worker_threads);
    tcase_add_test(tc1, test_set_config_bad_file);
    tcase_add_test(tc1, test_set_config_empty_file);
    tcase_add_test(tc1, test_set_config_basic_config);
    tcase_add_test(tc1, test_update_filename_from_set_config);

    // Add the bitmap tests
    suite_add_tcase(s1, tc3);
    tcase_set_timeout(tc3, 3);
    tcase_add_test(tc3, make_anonymous_bitmap);
    tcase_add_test(tc3, make_bitmap_zero_size);
    tcase_add_test(tc3, make_bitmap_bad_fileno);
    tcase_add_test(tc3, make_bitmap_bad_fileno_persistent);
    tcase_add_test(tc3, make_bitmap_nofile);
    tcase_add_test(tc3, make_bitmap_nofile_persistent);
    tcase_add_test(tc3, make_bitmap_nofile_create);
    tcase_add_test(tc3, make_bitmap_nofile_create_persistent);

    // Add the hll tests
    suite_add_tcase(s1, tc4);
    tcase_add_test(tc4, test_hll_init_bad);
    tcase_add_test(tc4, test_hll_init_and_destroy);
    tcase_add_test(tc4, test_hll_add);
    tcase_add_test(tc4, test_hll_add_hash);
    tcase_add_test(tc4, test_hll_add_size);
    tcase_add_test(tc4, test_hll_add_size_bitmap);
    tcase_add_test(tc4, test_hll_size);
    tcase_add_test(tc4, test_hll_error_bound);
    tcase_add_test(tc4, test_hll_precision_for_error);
    tcase_add_test(tc4, test_hll_error_for_precision);
    tcase_add_test(tc4, test_hll_bytes_for_precision);

    // Add the set tests
    suite_add_tcase(s1, tc5);
    tcase_set_timeout(tc5, 3);
    tcase_add_test(tc5, test_set_init_destroy);
    tcase_add_test(tc5, test_set_init_discover_destroy);
    tcase_add_test(tc5, test_set_init_discover_delete);
    tcase_add_test(tc5, test_set_init_proxied);
    tcase_add_test(tc5, test_set_add);
    tcase_add_test(tc5, test_set_restore);
    tcase_add_test(tc5, test_set_flush);
    tcase_add_test(tc5, test_set_add_in_mem);
    tcase_add_test(tc5, test_set_page_out);

    // Add the filter tests
    suite_add_tcase(s1, tc6);
    tcase_set_timeout(tc6, 3);
    tcase_add_test(tc6, test_mgr_init_destroy);
    tcase_add_test(tc6, test_mgr_create_drop);
    tcase_add_test(tc6, test_mgr_create_double_drop);
    tcase_add_test(tc6, test_mgr_list);
    tcase_add_test(tc6, test_mgr_list_prefix);
    tcase_add_test(tc6, test_mgr_list_no_sets);
    tcase_add_test(tc6, test_mgr_add_keys);
    tcase_add_test(tc6, test_mgr_add_no_set);
    tcase_add_test(tc6, test_mgr_flush_no_set);
    tcase_add_test(tc6, test_mgr_flush);
    tcase_add_test(tc6, test_mgr_unmap_no_set);
    tcase_add_test(tc6, test_mgr_unmap);
    tcase_add_test(tc6, test_mgr_unmap_add_keys);
    tcase_add_test(tc6, test_mgr_clear_no_set);
    tcase_add_test(tc6, test_mgr_clear_not_proxied);
    tcase_add_test(tc6, test_mgr_clear);
    tcase_add_test(tc6, test_mgr_clear_reload);
    tcase_add_test(tc6, test_mgr_list_cold_no_sets);
    tcase_add_test(tc6, test_mgr_list_cold);
    tcase_add_test(tc6, test_mgr_unmap_in_mem);
    tcase_add_test(tc6, test_mgr_create_custom_config);
    tcase_add_test(tc6, test_mgr_restore);
    tcase_add_test(tc6, test_mgr_callback);

    // Add the art tests
    suite_add_tcase(s1, tc7);
    tcase_add_test(tc7, test_art_init_and_destroy);
    tcase_add_test(tc7, test_art_insert);
    tcase_add_test(tc7, test_art_insert_verylong);
    tcase_add_test(tc7, test_art_insert_search);
    tcase_add_test(tc7, test_art_insert_delete);
    tcase_add_test(tc7, test_art_insert_iter);
    tcase_add_test(tc7, test_art_iter_prefix);
    tcase_add_test(tc7, test_art_insert_copy_delete);

    srunner_run_all(sr, CK_ENV);
    nf = srunner_ntests_failed(sr);
    srunner_free(sr);

    return nf == 0 ? 0 : 1;
}

