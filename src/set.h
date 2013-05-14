#ifndef SET_H
#define SET_H
#include <pthread.h>
#include "config.h"
#include "spinlock.h"
#include "hll.h"

/*
 * Functions are NOT thread safe unless explicitly documented
 */

/**
 * These are the counters that are maintained for each set.
 */
typedef struct {
    uint64_t sets;
    uint64_t page_ins;
    uint64_t page_outs;
} set_counters;

/**
 * Representation of a hyperloglog set
 */
typedef struct hlld_set {
    hlld_config *config;           // hlld configuration
    hlld_set_config set_config;    // Set-specific config

    char *set_name;                 // The name of the set
    char *full_path;                // Path to our data

    char is_proxied;                // Is the bitmap available
    pthread_mutex_t hll_lock;       // Protects faulting in the HLL

    char is_dirty;                  // Has a write happened
    hlld_bitmap bm;                 // Bitmap for the HLL
    hll_t hll;                      // Underlying HLL
    hlld_spinlock hll_update;       // Protect the updates

    set_counters counters;         // Counters
} hlld_set;

/**
 * Initializes a set wrapper.
 * @arg config The configuration to use
 * @arg set_name The name of the set
 * @arg discover Should existing data files be discovered. Otherwise
 * they will be faulted in on-demand.
 * @arg set Output parameter, the new set
 * @return 0 on success
 */
int init_set(hlld_config *config, char *set_name, int discover, hlld_set **set);

/**
 * Destroys a set
 * @arg set The set to destroy
 * @return 0 on success
 */
int destroy_set(hlld_set *set);

/**
 * Gets the counters that belong to a set
 * @notes Thread safe, but may be inconsistent.
 * @arg set The set
 * @return A reference to the counters of a set
 */
set_counters* hset_counters(hlld_set *set);

/**
 * Checks if a set is currectly mapped into
 * memory or if it is proxied.
 * @notes Thread safe.
 * @return 0 if in-memory, 1 if proxied.
 */
int hset_is_proxied(hlld_set *set);

/**
 * Flushes the set. Idempotent if the
 * set is proxied or not dirty.
 * @arg set The set to close
 * @return 0 on success.
 */
int hset_flush(hlld_set *set);

/**
 * Gracefully closes a set.
 * @arg set The set to close
 * @return 0 on success.
 */
int hset_close(hlld_set *set);

/**
 * Deletes the set with extreme prejudice.
 * @arg set The set to delete
 * @return 0 on success.
 */
int hset_delete(hlld_set *set);

/**
 * Adds a key to the given set
 * @arg set The set to add to
 * @arg key The key to add
 * @return 0 on success.
 */
int hset_add(hlld_set *set, char *key);

/**
 * Gets the size of the set
 * @note Thread safe.
 * @arg set The set to check
 * @return The estimated size of the set
 */
uint64_t hset_size(hlld_set *set);

/**
 * Gets the byte size of the set
 * @note Thread safe.
 * @arg set The set
 * @return The total byte size of the set
 */
uint64_t hset_byte_size(hlld_set *set);

#endif
