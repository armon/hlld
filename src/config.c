#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <syslog.h>
#include <unistd.h>
#include "hll.h"
#include "config.h"
#include "ini.h"

/**
 * Default hlld_config values. Should create
 * sets that are about 300KB initially, and suited
 * to grow quickly.
 */
static const hlld_config DEFAULT_CONFIG = {
    4553,               // TCP defaults to 8673
    4554,               // UDP on 8674
    "0.0.0.0",          // Default listen on 0.0.0.0
    "/tmp/hlld",        // Tmp data dir, until configured
    "INFO",             // INFO level
    LOG_INFO,
    .01625,             // Default 1.625% error == precision 12
    12,                 // Default 12 precision (4096 registers)
    60,                 // Flush once a minute
    3600,               // Cold after an hour
    0,                  // Persist to disk by default
    1,                  // Only a single worker thread by default
    0                   // Do NOT use mmap by default
};

/**
 * Attempts to convert a string to an integer,
 * and write the value out.
 * @arg val The string value
 * @arg result The destination for the result
 * @return 1 on success, 0 on error.
 */
static int value_to_int(const char *val, int *result) {
    long res = strtol(val, NULL, 10);
    if (res == 0 && errno == EINVAL) {
        return 0;
    }
    *result = res;
    return 1;
}

/**
 * Attempts to convert a string to an integer (64bit),
 * and write the value out.
 * @arg val The string value
 * @arg result The destination for the result
 * @return 0 on success, 0 on error.
 */
static int value_to_int64(const char *val, uint64_t *result) {
    long long res = strtoll(val, NULL, 10);
    if (res == 0 && errno == EINVAL) {
        return 0;
    }
    *result = res;
    return 1;
}

/**
 * Attempts to convert a string to a double,
 * and write the value out.
 * @arg val The string value
 * @arg result The destination for the result
 * @return 0 on success, -EINVAL on error.
 */
static int value_to_double(const char *val, double *result) {
    double res = strtod(val, NULL);
    if (res == 0) {
        return 0;
    }
    *result = res;
    return 0;
}

/**
 * Callback function to use with INI-H.
 * @arg user Opaque user value. We use the hlld_config pointer
 * @arg section The INI seciton
 * @arg name The config name
 * @arg value The config value
 * @return 1 on success.
 */
static int config_callback(void* user, const char* section, const char* name, const char* value) {
    // Ignore any non-hlld sections
    if (strcasecmp("hlld", section) != 0) {
        return 0;
    }

    // Cast the user handle
    hlld_config *config = (hlld_config*)user;

    // Switch on the config
#define NAME_MATCH(param) (strcasecmp(param, name) == 0)

    // Handle the int cases
    if (NAME_MATCH("port")) {
        return value_to_int(value, &config->tcp_port);
    } else if (NAME_MATCH("tcp_port")) {
        return value_to_int(value, &config->tcp_port);
    } else if (NAME_MATCH("udp_port")) {
        return value_to_int(value, &config->udp_port);
    } else if (NAME_MATCH("flush_interval")) {
        return value_to_int(value, &config->flush_interval);
    } else if (NAME_MATCH("cold_interval")) {
        return value_to_int(value, &config->cold_interval);
    } else if (NAME_MATCH("in_memory")) {
        return value_to_int(value, &config->in_memory);
    } else if (NAME_MATCH("use_mmap")) {
        return value_to_int(value, &config->use_mmap);
    } else if (NAME_MATCH("workers")) {
        return value_to_int(value, &config->worker_threads);
    } else if (NAME_MATCH("default_precision")) {
        int res = value_to_int(value, &config->default_precision);
        // Compute expected error given precision
        config->default_eps = hll_error_for_precision(config->default_precision);
        return res;

        // Handle the double cases
    } else if (NAME_MATCH("default_eps")) {
        int res = value_to_double(value, &config->default_eps);
        // Compute required precision given error
        config->default_precision = hll_precision_for_error(config->default_eps);

        // Compute error given precision. This is kinda strange but it is done
        // since its not possible to hit all epsilons perfectly, but we try to get
        // the eps provided to be the upper bound. This value is the actual eps.
        config->default_eps = hll_error_for_precision(config->default_precision);
        return res;

        // Copy the string values
    } else if (NAME_MATCH("data_dir")) {
        config->data_dir = strdup(value);
    } else if (NAME_MATCH("log_level")) {
        config->log_level = strdup(value);
    } else if (NAME_MATCH("bind_address")) {
        config->bind_address = strdup(value);

        // Unknown parameter?
    } else {
        // Log it, but ignore
        syslog(LOG_NOTICE, "Unrecognized config parameter: %s", value);
    }

    // Success
    return 1;
}

/**
 * Initializes the configuration from a filename.
 * Reads the file as an INI configuration, and sets up the
 * config object.
 * @arg filename The name of the file to read. NULL for defaults.
 * @arg config Output. The config object to initialize.
 * @return 0 on success, negative on error.
 */
int config_from_filename(char *filename, hlld_config *config) {
    // Initialize to the default values
    memcpy(config, &DEFAULT_CONFIG, sizeof(hlld_config));

    // If there is no filename, return now
    if (filename == NULL)
        return 0;

    // Try to open the file
    int res = ini_parse(filename, config_callback, config);
    if (res == -1) {
        return -ENOENT;
    }

    return 0;
}

/**
 * Joins two strings as part of a path,
 * and adds a separating slash if needed.
 * @param path Part one of the path
 * @param part2 The second part of the path
 * @return A new string, that uses a malloc()'d buffer.
 */
char* join_path(char *path, char *part2) {
    // Check for the end slash
    int len = strlen(path);
    int has_end_slash = path[len-1] == '/';

    // Use the proper format string
    char *buf;
    int res;
    if (has_end_slash)
        res = asprintf(&buf, "%s%s", path, part2);
    else
        res = asprintf(&buf, "%s/%s", path, part2);
    assert(res != -1);

    // Return the new buffer
    return buf;
}

int sane_data_dir(char *data_dir) {
    // Check if the path exists, and it is not a dir
    struct stat buf;
    int res = stat(data_dir, &buf);
    if (res == 0) {
        if ((buf.st_mode & S_IFDIR) == 0) {
            syslog(LOG_ERR,
                    "Provided data directory exists and is not a directory!");
            return 1;
        }
    } else  {
        // Try to make the directory
        res = mkdir(data_dir, 0775);
        if (res != 0) {
            syslog(LOG_ERR,
                    "Failed to make the data directory! Err: %s", strerror(errno));
            return 1;
        }
    }

    // Try to test we have permissions to write
    char *test_path = join_path(data_dir, "PERMTEST");
    int fh = open(test_path, O_CREAT|O_RDWR, 0644);

    // Cleanup
    if (fh != -1) close(fh);
    unlink(test_path);
    free(test_path);

    // If we failed to open the file, error
    if (fh == -1) {
        syslog(LOG_ERR,
                "Failed to write to data directory! Err: %s", strerror(errno));
        return 1;
    }

    return 0;
}

int sane_log_level(char *log_level, int *syslog_level) {
#define LOG_MATCH(lvl) (strcasecmp(lvl, log_level) == 0)
    if (LOG_MATCH("DEBUG")) {
        *syslog_level = LOG_UPTO(LOG_DEBUG);
    } else if (LOG_MATCH("INFO")) {
        *syslog_level = LOG_UPTO(LOG_INFO);
    } else if (LOG_MATCH("WARN")) {
        *syslog_level = LOG_UPTO(LOG_WARNING);
    } else if (LOG_MATCH("ERROR")) {
        *syslog_level = LOG_UPTO(LOG_ERR);
    } else if (LOG_MATCH("CRITICAL")) {
        *syslog_level = LOG_UPTO(LOG_CRIT);
    } else {
        syslog(LOG_ERR, "Unknown log level!");
        return 1;
    }
    return 0;
}

int sane_default_eps(double eps) {
    if (eps > hll_error_for_precision(HLL_MIN_PRECISION)) {
        syslog(LOG_ERR,
                "Epsilon cannot be greater than %f!", hll_error_for_precision(HLL_MIN_PRECISION));
        return 1;

    } else if (eps < hll_error_for_precision(HLL_MAX_PRECISION)) {
        syslog(LOG_ERR, "Epsilon cannot be less than %f!", hll_error_for_precision(HLL_MAX_PRECISION));
        return 1;

    } else if (eps < 0.005) {
        syslog(LOG_WARNING, "Epsilon very low, could cause high memory usage!");
    }
    return 0;
}

int sane_default_precision(int precision) {
    if (precision < HLL_MIN_PRECISION) {
        syslog(LOG_ERR,
                "Precision cannot be less than %d!", HLL_MIN_PRECISION);
        return 1;
    } else if (precision > HLL_MAX_PRECISION) {
        syslog(LOG_ERR, "Precision cannot be more than %d!", HLL_MAX_PRECISION);
        return 1;

    } else if (precision > 15) {
        syslog(LOG_WARNING, "Precision very high, could cause high memory usage!");
    }

    return 0;
}

int sane_flush_interval(int intv) {
    if (intv == 0) {
        syslog(LOG_WARNING,
                "Flushing is disabled! Increased risk of data loss.");
    } else if (intv < 0) {
        syslog(LOG_ERR, "Flush interval cannot be negative!");
        return 1;
    } else if (intv >= 600)  {
        syslog(LOG_WARNING,
                "Flushing set to be very infrequent! Increased risk of data loss.");
    }
    return 0;
}

int sane_cold_interval(int intv) {
    if (intv == 0) {
        syslog(LOG_WARNING,
                "Cold data unmounting is disabled! Memory usage may be high.");
    } else if (intv < 0) {
        syslog(LOG_ERR, "Cold interval cannot be negative!");
        return 1;
    } else if (intv < 300) {
        syslog(LOG_ERR, "Cold interval is less than 5 minutes. \
                This may cause excessive unmapping to occur.");
    }

    return 0;
}

int sane_in_memory(int in_mem) {
    if (in_mem != 0) {
        syslog(LOG_WARNING,
                "Default sets are in-memory only! Sets not persisted by default.");
    }
    if (in_mem != 0 && in_mem != 1) {
        syslog(LOG_ERR,
                "Illegal value for in-memory. Must be 0 or 1.");
        return 1;
    }

    return 0;
}

int sane_use_mmap(int use_mmap) {
    if (use_mmap != 1) {
        syslog(LOG_WARNING,
                "Without use_mmap, a crash of hlld can result in data loss.");
    }
    if (use_mmap != 0 && use_mmap != 1) {
        syslog(LOG_ERR,
                "Illegal value for use_mmap. Must be 0 or 1.");
        return 1;
    }
    return 0;
}

int sane_worker_threads(int threads) {
    if (threads <= 0) {
        syslog(LOG_ERR,
                "Cannot have fewer than one worker thread!");
        return 1;
    }
    return 0;
}


/**
 * Validates the configuration
 * @arg config The config object to validate.
 * @return 0 on success.
 */
int validate_config(hlld_config *config) {
    int res = 0;

    res |= sane_data_dir(config->data_dir);
    res |= sane_log_level(config->log_level, &config->syslog_log_level);
    res |= sane_default_eps(config->default_eps);
    res |= sane_default_precision(config->default_precision);
    res |= sane_flush_interval(config->flush_interval);
    res |= sane_cold_interval(config->cold_interval);
    res |= sane_in_memory(config->in_memory);
    res |= sane_use_mmap(config->use_mmap);
    res |= sane_worker_threads(config->worker_threads);

    return res;
}

/**
 * Callback function to use with INI-H.
 * @arg user Opaque user value. We use the hlld_config pointer
 * @arg section The INI seciton
 * @arg name The config name
 * @arg value The config value
 * @return 1 on success.
 */
static int set_config_callback(void* user, const char* section, const char* name, const char* value) {
    // Ignore any non-hlld sections
    if (strcasecmp("hlld", section) != 0) {
        return 0;
    }

    // Cast the user handle
    hlld_set_config *config = (hlld_set_config*)user;

    // Switch on the config
#define NAME_MATCH(param) (strcasecmp(param, name) == 0)

    // Handle the int cases
    if (NAME_MATCH("in_memory")) {
        return value_to_int(value, &config->in_memory);
    } else if (NAME_MATCH("default_precision")) {
        return value_to_int(value, &config->default_precision);

        // Handle big int
    } else if (NAME_MATCH("size")) {
        return value_to_int64(value, &config->size);

        // Handle the double cases
    } else if (NAME_MATCH("default_eps")) {
        return value_to_double(value, &config->default_eps);

        // Unknown parameter?
    } else {
        // Log it, but ignore
        syslog(LOG_NOTICE, "Unrecognized set config parameter: %s", value);
    }

    // Success
    return 1;
}

/**
 * Updates the configuration from a filename.
 * Reads the file as an INI configuration and updates the config.
 * @arg filename The name of the file to read.
 * @arg config Output. The config object to update. Does not initialize!
 * @return 0 on success, negative on error.
 */
int set_config_from_filename(char *filename, hlld_set_config *config) {
    // If there is no filename, return now
    if (filename == NULL)
        return 0;

    // Try to open the file
    int res = ini_parse(filename, set_config_callback, config);
    if (res == -1) {
        return -ENOENT;
    }

    return 0;
}

/**
 * Writes the configuration to a filename.
 * Writes the file as an INI configuration
 * @arg filename The name of the file to write.
 * @arg config The config object to write out.
 * @return 0 on success, negative on error.
 */
int update_filename_from_set_config(char *filename, hlld_set_config *config) {
    // Try to open the file
    FILE* f = fopen(filename, "w+");
    if (!f) return -errno;

    // Write out
    fprintf(f, "[hlld]\n\
size = %llu\n\
default_eps = %f\n\
default_precision = %d\n\
in_memory = %d\n", (unsigned long long)config->size,
            config->default_eps,
            config->default_precision,
            config->in_memory
           );

    // Close
    fclose(f);
    return 0;
}

