#include <check.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include "hll.h"

START_TEST(test_hll_init_bad)
{
    hll_t h;
    fail_unless(hll_init(HLL_MIN_PRECISION-1, &h) == -1);
    fail_unless(hll_init(HLL_MAX_PRECISION+1, &h) == -1);

    fail_unless(hll_init(HLL_MIN_PRECISION, &h) == 0);
    fail_unless(hll_destroy(&h) == 0);

    fail_unless(hll_init(HLL_MAX_PRECISION, &h) == 0);
    fail_unless(hll_destroy(&h) == 0);
}
END_TEST


START_TEST(test_hll_init_and_destroy)
{
    hll_t h;
    fail_unless(hll_init(10, &h) == 0);
    fail_unless(hll_destroy(&h) == 0);
}
END_TEST

START_TEST(test_hll_add)
{
    hll_t h;
    fail_unless(hll_init(10, &h) == 0);

    char buf[100];
    for (int i=0; i < 100; i++) {
        fail_unless(sprintf((char*)&buf, "test%d", i));
        hll_add(&h, (char*)&buf);
    }

    fail_unless(hll_destroy(&h) == 0);
}
END_TEST

START_TEST(test_hll_add_hash)
{
    hll_t h;
    fail_unless(hll_init(10, &h) == 0);

    for (uint64_t i=0; i < 100; i++) {
        hll_add_hash(&h, i ^ rand());
    }

    fail_unless(hll_destroy(&h) == 0);
}
END_TEST

START_TEST(test_hll_add_size)
{
    hll_t h;
    fail_unless(hll_init(10, &h) == 0);

    char buf[100];
    for (int i=0; i < 100; i++) {
        fail_unless(sprintf((char*)&buf, "test%d", i));
        hll_add(&h, (char*)&buf);
    }

    double s = hll_size(&h);
    fail_unless(s > 95 && s < 105);

    fail_unless(hll_destroy(&h) == 0);
}
END_TEST

START_TEST(test_hll_add_size_bitmap)
{
    hlld_bitmap bm;
    uint64_t bytes = hll_bytes_for_precision(10);
    fail_unless(bitmap_from_file(-1, bytes, ANONYMOUS, &bm) == 0);

    hll_t h;
    fail_unless(hll_init_from_bitmap(10, &bm, &h) == 0);

    char buf[100];
    for (int i=0; i < 100; i++) {
        fail_unless(sprintf((char*)&buf, "test%d", i));
        hll_add(&h, (char*)&buf);
    }

    double s = hll_size(&h);
    fail_unless(s > 95 && s < 105);

    fail_unless(hll_destroy(&h) == 0);
}
END_TEST

START_TEST(test_hll_size)
{
    hll_t h;
    fail_unless(hll_init(10, &h) == 0);

    double s = hll_size(&h);
    fail_unless(s == 0);

    fail_unless(hll_destroy(&h) == 0);
}
END_TEST

START_TEST(test_hll_error_bound)
{
    // Precision 14 -> variance of 1%
    hll_t h;
    fail_unless(hll_init(14, &h) == 0);

    char buf[100];
    for (int i=0; i < 10000; i++) {
        fail_unless(sprintf((char*)&buf, "test%d", i));
        hll_add(&h, (char*)&buf);
    }

    // Should be within 1%
    double s = hll_size(&h);
    fail_unless(s > 9900 && s < 10100);

    fail_unless(hll_destroy(&h) == 0);
}
END_TEST

START_TEST(test_hll_precision_for_error)
{
    fail_unless(hll_precision_for_error(1.0) == -1);
    fail_unless(hll_precision_for_error(0.0) == -1);
    fail_unless(hll_precision_for_error(0.02) == 12);
    fail_unless(hll_precision_for_error(0.01) == 14);
    fail_unless(hll_precision_for_error(0.005) == 16);
}
END_TEST


START_TEST(test_hll_error_for_precision)
{
    fail_unless(hll_error_for_precision(3) == 0);
    fail_unless(hll_error_for_precision(20) == 0);
    fail_unless(hll_error_for_precision(12) == .01625);
    fail_unless(hll_error_for_precision(10) == .0325);
    fail_unless(hll_error_for_precision(16) == .0040625);
}
END_TEST

START_TEST(test_hll_bytes_for_precision)
{
    fail_unless(hll_bytes_for_precision(3) == 0);
    fail_unless(hll_bytes_for_precision(20) == 0);
    fail_unless(hll_bytes_for_precision(12) == 3280);
    fail_unless(hll_bytes_for_precision(10) == 820);
    fail_unless(hll_bytes_for_precision(16) == 52432);
}
END_TEST

