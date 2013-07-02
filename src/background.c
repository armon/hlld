#include <unistd.h>
#include <sys/time.h>
#include "background.h"

/**
 * This defines how log we sleep between loop iterations
 * in microseconds
 */
#define PERIODIC_TIME_USEC 250000

/**
 * Based on the PERIODIC_TIME_USEC, this should
 * convert seconds to tick counts. One tick occurs
 * each PERIODIC_TIME_USEC interval
 */
#define SEC_TO_TICKS(sec) ((sec * 4))

/**
 * After how many background operations should we force a client
 * checkpoint. This allows the vacuum thread to make progress even
 * if we have a very slow background task
 */
#define PERIODIC_CHECKPOINT 64

static int timediff_msec(struct timeval *t1, struct timeval *t2);
static void* flush_thread_main(void *in);
static void* unmap_thread_main(void *in);
typedef struct {
    hlld_config *config;
    hlld_setmgr *mgr;
    int *should_run;
} background_thread_args;

/**
 * Helper macro to pack and unpack the arguments
 * to the thread, and free the memory.
 */
# define PACK_ARGS() {                  \
    args = malloc(sizeof(background_thread_args));  \
    args->config = config;              \
    args->mgr = mgr;                    \
    args->should_run = should_run;      \
}
# define UNPACK_ARGS() {                \
    background_thread_args *args = in;  \
    config = args->config;              \
    mgr = args->mgr;                    \
    should_run = args->should_run;      \
    free(args);                         \
}

/**
 * Starts a flushing thread which on every
 * configured flush interval, flushes all the sets.
 * @arg config The configuration
 * @arg mgr The manager to use
 * @arg should_run Pointer to an integer that is set to 0 to
 * indicate the thread should exit.
 * @arg t The output thread
 * @return 1 if the thread was started
 */
int start_flush_thread(hlld_config *config, hlld_setmgr *mgr, int *should_run, pthread_t *t) {
    // Return if we are not scheduled
    if(config->flush_interval <= 0) {
        return 0;
    }

    // Start thread
    background_thread_args *args;
    PACK_ARGS();
    pthread_create(t, NULL, flush_thread_main, args);
    return 1;
}

/**
 * Starts a cold unmap thread which on every
 * cold interval unamps cold sets.
 * @arg config The configuration
 * @arg mgr The manager to use
 * @arg should_run Pointer to an integer that is set to 0 to
 * indicate the thread should exit.
 * @arg t The output thread
 * @return 1 if the thread was started
 */
int start_cold_unmap_thread(hlld_config *config, hlld_setmgr *mgr, int *should_run, pthread_t *t) {
    // Return if we are not scheduled
    if(config->cold_interval <= 0) {
        return 0;
    }

    // Start thread
    background_thread_args *args;
    PACK_ARGS();
    pthread_create(t, NULL, unmap_thread_main, args);
    return 1;
}


static void* flush_thread_main(void *in) {
    hlld_config *config;
    hlld_setmgr *mgr;
    int *should_run;
    UNPACK_ARGS();

    // Perform the initial checkpoint with the manager
    setmgr_client_checkpoint(mgr);

    syslog(LOG_INFO, "Flush thread started. Interval: %d seconds.", config->flush_interval);
    unsigned int ticks = 0;
    while (*should_run) {
        usleep(PERIODIC_TIME_USEC);
        setmgr_client_checkpoint(mgr);
        if ((++ticks % SEC_TO_TICKS(config->flush_interval)) == 0 && *should_run) {
            // Time how long this takes
            struct timeval start, end;
            gettimeofday(&start, NULL);

            // List all the sets
            syslog(LOG_INFO, "Scheduled flush started.");
            hlld_set_list_head *head;
            int res = setmgr_list_sets(mgr, NULL, &head);
            if (res != 0) {
                syslog(LOG_WARNING, "Failed to list sets for flushing!");
                continue;
            }

            // Flush all, ignore errors since
            // sets might get deleted in the process
            hlld_set_list *node = head->head;
            unsigned int cmds = 0;
            while (node) {
                setmgr_flush_set(mgr, node->set_name);
                if (!(++cmds % PERIODIC_CHECKPOINT)) setmgr_client_checkpoint(mgr);
                node = node->next;
            }

            // Compute the elapsed time
            gettimeofday(&end, NULL);
            syslog(LOG_INFO, "Flushed %d sets in %d msecs", head->size, timediff_msec(&start, &end));

            // Cleanup
            setmgr_cleanup_list(head);
        }
    }
    return NULL;
}

static void* unmap_thread_main(void *in) {
    hlld_config *config;
    hlld_setmgr *mgr;
    int *should_run;
    UNPACK_ARGS();

    // Perform the initial checkpoint with the manager
    setmgr_client_checkpoint(mgr);

    syslog(LOG_INFO, "Cold unmap thread started. Interval: %d seconds.", config->cold_interval);
    unsigned int ticks = 0;
    while (*should_run) {
        usleep(PERIODIC_TIME_USEC);
        setmgr_client_checkpoint(mgr);
        if ((++ticks % SEC_TO_TICKS(config->cold_interval)) == 0 && *should_run) {
            // Time how long this takes
            struct timeval start, end;
            gettimeofday(&start, NULL);

            // List the cold sets
            syslog(LOG_INFO, "Cold unmap started.");
            hlld_set_list_head *head;
            int res = setmgr_list_cold_sets(mgr, &head);
            if (res != 0) {
                continue;
            }

            // Close the sets, save memory
            hlld_set_list *node = head->head;
            unsigned int cmds = 0;
            while (node) {
                syslog(LOG_DEBUG, "Unmapping set '%s' for being cold.", node->set_name);
                setmgr_unmap_set(mgr, node->set_name);
                if (!(++cmds % PERIODIC_CHECKPOINT)) setmgr_client_checkpoint(mgr);
                node = node->next;
            }

            // Compute the elapsed time
            gettimeofday(&end, NULL);
            syslog(LOG_INFO, "Unmapped %d sets in %d msecs", head->size, timediff_msec(&start, &end));

            // Cleanup
            setmgr_cleanup_list(head);
        }
    }
    return NULL;
}

/**
 * Computes the difference in time in milliseconds
 * between two timeval structures.
 */
static int timediff_msec(struct timeval *t1, struct timeval *t2) {
    uint64_t micro1 = t1->tv_sec * 1000000 + t1->tv_usec;
    uint64_t micro2= t2->tv_sec * 1000000 + t2->tv_usec;
    return (micro2-micro1) / 1000;
}

