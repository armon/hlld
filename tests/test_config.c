#include <check.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include "config.h"

START_TEST(test_config_get_default)
{
    hlld_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);
    fail_unless(config.tcp_port == 4553);
    fail_unless(config.udp_port == 4554);
    fail_unless(strcmp(config.data_dir, "/tmp/hlld") == 0);
    fail_unless(strcmp(config.log_level, "INFO") == 0);
    fail_unless(config.syslog_log_level == LOG_INFO);
    fail_unless(config.default_eps == 0.01625);
    fail_unless(config.default_precision == 12);
    fail_unless(config.flush_interval == 60);
    fail_unless(config.cold_interval == 3600);
    fail_unless(config.in_memory == 0);
    fail_unless(config.worker_threads == 1);
    fail_unless(config.use_mmap == 0);
}
END_TEST

START_TEST(test_config_bad_file)
{
    hlld_config config;
    int res = config_from_filename("/tmp/does_not_exist", &config);
    fail_unless(res == -ENOENT);

    // Should get the defaults...
    fail_unless(config.tcp_port == 4553);
    fail_unless(config.udp_port == 4554);
    fail_unless(strcmp(config.data_dir, "/tmp/hlld") == 0);
    fail_unless(strcmp(config.log_level, "INFO") == 0);
    fail_unless(config.syslog_log_level == LOG_INFO);
    fail_unless(config.default_eps == 0.01625);
    fail_unless(config.default_precision == 12);
    fail_unless(config.flush_interval == 60);
    fail_unless(config.cold_interval == 3600);
    fail_unless(config.in_memory == 0);
    fail_unless(config.worker_threads == 1);
    fail_unless(config.use_mmap == 0);
}
END_TEST

START_TEST(test_config_empty_file)
{
    int fh = open("/tmp/zero_file", O_CREAT|O_RDWR, 0777);
    fchmod(fh, 777);
    close(fh);

    hlld_config config;
    int res = config_from_filename("/tmp/zero_file", &config);
    fail_unless(res == 0);

    // Should get the defaults...
    fail_unless(config.tcp_port == 4553);
    fail_unless(config.udp_port == 4554);
    fail_unless(strcmp(config.data_dir, "/tmp/hlld") == 0);
    fail_unless(strcmp(config.log_level, "INFO") == 0);
    fail_unless(config.syslog_log_level == LOG_INFO);
    fail_unless(config.default_eps == 0.01625);
    fail_unless(config.default_precision == 12);
    fail_unless(config.flush_interval == 60);
    fail_unless(config.cold_interval == 3600);
    fail_unless(config.in_memory == 0);
    fail_unless(config.worker_threads == 1);
    fail_unless(config.use_mmap == 0);

    unlink("/tmp/zero_file");
}
END_TEST

START_TEST(test_config_basic_config)
{
    int fh = open("/tmp/basic_config", O_CREAT|O_RDWR, 0777);
    char *buf = "[hlld]\n\
port = 10000\n\
udp_port = 10001\n\
flush_interval = 120\n\
cold_interval = 12000\n\
in_memory = 1\n\
default_eps = 0.05\n\
data_dir = /tmp/test\n\
workers = 2\n\
use_mmap = 1\n\
log_level = INFO\n";
    write(fh, buf, strlen(buf));
    fchmod(fh, 777);
    close(fh);

    hlld_config config;
    int res = config_from_filename("/tmp/basic_config", &config);
    fail_unless(res == 0);

    // Should get the config
    fail_unless(config.tcp_port == 10000);
    fail_unless(config.udp_port == 10001);
    fail_unless(strcmp(config.data_dir, "/tmp/test") == 0);
    fail_unless(strcmp(config.log_level, "INFO") == 0);
    fail_unless(config.default_eps - 0.045961941 < 0.0001, "EPS %f", config.default_eps);
    fail_unless(config.default_precision == 9, "PREC %d", config.default_precision);
    fail_unless(config.flush_interval == 120);
    fail_unless(config.cold_interval == 12000);
    fail_unless(config.in_memory == 1);
    fail_unless(config.worker_threads == 2);
    fail_unless(config.use_mmap == 1);

    unlink("/tmp/basic_config");
}
END_TEST

START_TEST(test_config_basic_config_precision)
{
    int fh = open("/tmp/basic_config_prec", O_CREAT|O_RDWR, 0777);
    char *buf = "[hlld]\n\
port = 10000\n\
udp_port = 10001\n\
flush_interval = 120\n\
cold_interval = 12000\n\
in_memory = 1\n\
default_precision = 14\n\
data_dir = /tmp/test\n\
workers = 2\n\
use_mmap = 1\n\
log_level = INFO\n";
    write(fh, buf, strlen(buf));
    fchmod(fh, 777);
    close(fh);

    hlld_config config;
    int res = config_from_filename("/tmp/basic_config_prec", &config);
    fail_unless(res == 0);

    // Should get the config
    fail_unless(config.tcp_port == 10000);
    fail_unless(config.udp_port == 10001);
    fail_unless(strcmp(config.data_dir, "/tmp/test") == 0);
    fail_unless(strcmp(config.log_level, "INFO") == 0);
    fail_unless(config.default_precision == 14, "PREC %d", config.default_precision);
    fail_unless(config.default_eps == .008125, "EPS %f", config.default_eps);
    fail_unless(config.flush_interval == 120);
    fail_unless(config.cold_interval == 12000);
    fail_unless(config.in_memory == 1);
    fail_unless(config.worker_threads == 2);
    fail_unless(config.use_mmap == 1);

    unlink("/tmp/basic_config_prec");
}
END_TEST



START_TEST(test_validate_default_config)
{
    hlld_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    res = validate_config(&config);
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_validate_bad_config)
{
    hlld_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    // Set an absurd probability, should fail
    config.default_eps = 1.0;

    res = validate_config(&config);
    fail_unless(res == 1);
}
END_TEST

START_TEST(test_join_path_no_slash)
{
    char *s1 = "/tmp/path";
    char *s2 = "file";
    char *s3 = join_path(s1, s2);
    fail_unless(strcmp(s3, "/tmp/path/file") == 0);
}
END_TEST

START_TEST(test_join_path_with_slash)
{
    char *s1 = "/tmp/path/";
    char *s2 = "file";
    char *s3 = join_path(s1, s2);
    fail_unless(strcmp(s3, "/tmp/path/file") == 0);
}
END_TEST

START_TEST(test_sane_log_level)
{
    int log_lvl;
    fail_unless(sane_log_level("DEBUG", &log_lvl) == 0);
    fail_unless(sane_log_level("debug", &log_lvl) == 0);
    fail_unless(sane_log_level("INFO", &log_lvl) == 0);
    fail_unless(sane_log_level("info", &log_lvl) == 0);
    fail_unless(sane_log_level("WARN", &log_lvl) == 0);
    fail_unless(sane_log_level("warn", &log_lvl) == 0);
    fail_unless(sane_log_level("ERROR", &log_lvl) == 0);
    fail_unless(sane_log_level("error", &log_lvl) == 0);
    fail_unless(sane_log_level("CRITICAL", &log_lvl) == 0);
    fail_unless(sane_log_level("critical", &log_lvl) == 0);
    fail_unless(sane_log_level("foo", &log_lvl) == 1);
    fail_unless(sane_log_level("BAR", &log_lvl) == 1);
}
END_TEST

START_TEST(test_sane_default_eps)
{
    fail_unless(sane_default_eps(1) == 1);
    fail_unless(sane_default_eps(0) == 1);
    fail_unless(sane_default_eps(0.01) == 0);
    fail_unless(sane_default_eps(0.25) == 0);
    fail_unless(sane_default_eps(0.005) == 0);
    fail_unless(sane_default_eps(0.3) == 1);
    fail_unless(sane_default_eps(0.002) == 1);
}
END_TEST

START_TEST(test_sane_default_precision)
{
    fail_unless(sane_default_precision(0) == 1);
    fail_unless(sane_default_precision(20) == 1);
    fail_unless(sane_default_precision(4) == 0);
    fail_unless(sane_default_precision(18) == 0);
    fail_unless(sane_default_precision(12) == 0);
}
END_TEST

START_TEST(test_sane_flush_interval)
{
    fail_unless(sane_flush_interval(-1) == 1);
    fail_unless(sane_flush_interval(0) == 0);
    fail_unless(sane_flush_interval(60) == 0);
    fail_unless(sane_flush_interval(120) == 0);
    fail_unless(sane_flush_interval(86400) == 0);
}
END_TEST

START_TEST(test_sane_cold_interval)
{
    fail_unless(sane_cold_interval(-1) == 1);
    fail_unless(sane_cold_interval(0) == 0);
    fail_unless(sane_cold_interval(60) == 0);
    fail_unless(sane_cold_interval(120) == 0);
    fail_unless(sane_cold_interval(3600) == 0);
    fail_unless(sane_cold_interval(86400) == 0);
}
END_TEST

START_TEST(test_sane_in_memory)
{
    fail_unless(sane_in_memory(-1) == 1);
    fail_unless(sane_in_memory(0) == 0);
    fail_unless(sane_in_memory(1) == 0);
    fail_unless(sane_in_memory(2) == 1);
}
END_TEST

START_TEST(test_sane_use_mmap)
{
    fail_unless(sane_use_mmap(-1) == 1);
    fail_unless(sane_use_mmap(0) == 0);
    fail_unless(sane_use_mmap(1) == 0);
    fail_unless(sane_use_mmap(2) == 1);
}
END_TEST

START_TEST(test_sane_worker_threads)
{
    fail_unless(sane_worker_threads(-1) == 1);
    fail_unless(sane_worker_threads(0) == 1);
    fail_unless(sane_worker_threads(1) == 0);
    fail_unless(sane_worker_threads(2) == 0);
    fail_unless(sane_worker_threads(16) == 0);
}
END_TEST

START_TEST(test_set_config_bad_file)
{
    hlld_set_config config;
    memset(&config, '\0', sizeof(config));
    int res = set_config_from_filename("/tmp/does_not_exist", &config);
    fail_unless(res == -ENOENT);

    fail_unless(config.default_eps == 0);
    fail_unless(config.default_precision == 0);
    fail_unless(config.in_memory == 0);
    fail_unless(config.size == 0);
}
END_TEST

START_TEST(test_set_config_empty_file)
{
    int fh = open("/tmp/zero_file", O_CREAT|O_RDWR, 0777);
    fchmod(fh, 777);
    close(fh);

    hlld_set_config config;
    memset(&config, '\0', sizeof(config));
    int res = set_config_from_filename("/tmp/zero_file", &config);
    fail_unless(res == 0);

    fail_unless(config.default_eps == 0);
    fail_unless(config.default_precision == 0);
    fail_unless(config.in_memory == 0);
    fail_unless(config.size == 0);

    unlink("/tmp/zero_file");
}
END_TEST

START_TEST(test_set_config_basic_config)
{
    int fh = open("/tmp/set_basic_config", O_CREAT|O_RDWR, 0777);
    char *buf = "[hlld]\n\
size = 1024\n\
in_memory = 1\n\
default_eps = 0.01625\n\
default_precision = 12\n";
    write(fh, buf, strlen(buf));
    fchmod(fh, 777);
    close(fh);

    hlld_set_config config;
    memset(&config, '\0', sizeof(config));
    int res = set_config_from_filename("/tmp/set_basic_config", &config);
    fail_unless(res == 0);

    // Should get the config
    fail_unless(config.size == 1024);
    fail_unless(config.default_eps == 0.01625);
    fail_unless(config.default_precision == 12);
    fail_unless(config.in_memory == 1);

    unlink("/tmp/set_basic_config");
}
END_TEST

START_TEST(test_update_filename_from_set_config)
{
    hlld_set_config config;
    config.default_eps = 0.01625;
    config.default_precision = 12;
    config.in_memory = 1;
    config.size = 4096;

    int res = update_filename_from_set_config("/tmp/update_filter", &config);
    chmod("/tmp/update_filter", 777);
    fail_unless(res == 0);

    // Should get the config
    hlld_set_config config2;
    memset(&config2, '\0', sizeof(config2));
    res = set_config_from_filename("/tmp/update_filter", &config2);
    fail_unless(res == 0);

    fail_unless(config2.default_eps == 0.01625);
    fail_unless(config2.default_precision == 12);
    fail_unless(config2.in_memory == 1);
    fail_unless(config2.size == 4096);

    unlink("/tmp/update_filter");
}
END_TEST

