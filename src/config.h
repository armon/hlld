#ifndef CONFIG_H
#define CONFIG_H
#include <stdint.h>
#include <syslog.h>

/**
 * Stores our configuration
 */
typedef struct {
    int tcp_port;
    int udp_port;
    char *bind_address;
    char *data_dir;
    char *log_level;
    int syslog_log_level;
    double default_eps;
    int default_precision;
    int flush_interval;
    int cold_interval;
    int in_memory;
    int worker_threads;
    int use_mmap;
} hlld_config;

/**
 * This structure is used to persist
 * set specific settings to an INI file.
 */
typedef struct {
    double default_eps;
    int default_precision;
    int in_memory;
    uint64_t size;
} hlld_set_config;


/**
 * Initializes the configuration from a filename.
 * Reads the file as an INI configuration, and sets up the
 * config object.
 * @arg filename The name of the file to read. NULL for defaults.
 * @arg config Output. The config object to initialize.
 * @return 0 on success, negative on error.
 */
int config_from_filename(char *filename, hlld_config *config);

/**
 * Updates the configuration from a filename.
 * Reads the file as an INI configuration and updates the config.
 * @arg filename The name of the file to read.
 * @arg config Output. The config object to update. Does not initialize!
 * @return 0 on success, negative on error.
 */
int set_config_from_filename(char *filename, hlld_set_config *config);

/**
 * Writes the configuration to a filename.
 * Writes the file as an INI configuration
 * @arg filename The name of the file to write.
 * @arg config The config object to write out.
 * @return 0 on success, negative on error.
 */
int update_filename_from_set_config(char *filename, hlld_set_config *config);

/**
 * Validates the configuration
 * @arg config The config object to validate.
 * @return 0 on success, negative on error.
 */
int validate_config(hlld_config *config);

// Configuration validation methods
int sane_data_dir(char *data_dir);
int sane_log_level(char *log_level, int *syslog_level);
int sane_default_eps(double prob);
int sane_default_precision(int precision);
int sane_flush_interval(int intv);
int sane_cold_interval(int intv);
int sane_in_memory(int in_mem);
int sane_use_mmap(int use_mmap);
int sane_worker_threads(int threads);

/**
 * Joins two strings as part of a path,
 * and adds a separating slash if needed.
 * @param path Part one of the path
 * @param part2 The second part of the path
 * @return A new string, that uses a malloc()'d buffer.
 */
char* join_path(char *path, char *part2);

#endif
