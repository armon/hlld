#ifndef BACKGROUND_H
#define BACKGROUND_H
#include <pthread.h>
#include "config.h"
#include "set_manager.h"

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
int start_flush_thread(hlld_config *config, hlld_setmgr *mgr, int *should_run, pthread_t *t);

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
int start_cold_unmap_thread(hlld_config *config, hlld_setmgr *mgr, int *should_run, pthread_t *);

#endif
