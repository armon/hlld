#include <stdlib.h>
#include <stdio.h>
#include <syslog.h>
#include <pthread.h>
#include <dirent.h>
#include <string.h>
#include "spinlock.h"
#include "set_manager.h"
#include "art.h"
#include "set.h"
#include "type_compat.h"

/**
 * This defines how log we sleep between vacuum poll
 * iterations in microseconds
 */
#define VACUUM_POLL_USEC 500000

/**
 * Wraps a hlld_set to ensure only a single
 * writer access it at a time. Tracks the outstanding
 * references, to allow a sane close to take place.
 */
typedef struct {
    volatile int is_active;         // Set to 0 when we are trying to delete it
    volatile int is_hot;            // Used to mark a set as hot
    volatile int should_delete;     // Used to control deletion

    hlld_set *set;    // The actual set object
    pthread_rwlock_t rwlock; // Protects the set
    hlld_config *custom;   // Custom config to cleanup
} hlld_set_wrapper;

/**
 * We use a linked list of setmgr_client
 * structs to track any clients of the set manager.
 * Each client maintains a thread ID as well as the
 * last known version they used. The vacuum thread
 * uses this information to safely garbage collect
 * old versions.
 */
typedef struct setmgr_client {
    pthread_t id;
    unsigned long long vsn;
    struct setmgr_client *next;
} setmgr_client;

// Enum of possible delta updates
typedef enum {
    CREATE,
    DELETE,
    BARRIER
} delta_type;

// Simple linked list of set wrappers
typedef struct set_list {
    unsigned long long vsn;
    delta_type type;
    hlld_set_wrapper *set;
    struct set_list *next;
} set_list;

/**
 * We use a a simple form of Multi-Version Concurrency Controll (MVCC)
 * to prevent locking on access to the map of set name -> hlld_set_wrapper.
 *
 * The way it works is we have 2 ART trees, the "primary" and an alternate.
 * All the clients of the set manager have reads go through the primary
 * without any locking. We also maintain a delta list of new and deleted sets,
 * which is a simple linked list.
 *
 * We use a separate vacuum thread to merge changes from the delta lists
 * into the alternate tree, and then do a pointer swap to rotate the
 * primary tree for the alternate.
 *
 * This mechanism ensures we have at most 2 ART trees, reads are lock-free,
 * and performance does not degrade with the number of sets.
 *
 */
struct hlld_setmgr {
    hlld_config *config;

    int should_run;  // Used to stop the vacuum thread
    pthread_t vacuum_thread;

    /*
     * To support vacuuming of old versions, we require that
     * workers 'periodically' checkpoint. This just updates an
     * index to match the current version. The vacuum thread
     * can scan for the minimum seen version and clean all older
     * versions.
     */
    setmgr_client *clients;
    hlld_spinlock clients_lock;

    // This is the current version. Should be used
    // under the write lock.
    unsigned long long vsn;
    pthread_mutex_t write_lock; // Serializes destructive operations

    // Maps key names -> hlld_set_wrapper
    unsigned long long primary_vsn; // This is the version that set_map represents
    art_tree *set_map;
    art_tree *alt_set_map;

    /**
     * List of pending deletes. This is necessary
     * because the set_map may reflect that a delete has
     * taken place, while the vacuum thread has not yet performed the
     * delete. This allows create to return a "Delete in progress".
     */
    hlld_set_list *pending_deletes;
    hlld_spinlock pending_lock;

    // Delta lists for non-merged operations
    set_list *delta;
};

/**
 * We warn if there are this many outstanding versions
 * that cannot be vacuumed
 */
#define WARN_THRESHOLD 32

/*
 * Static declarations
 */
static const char FOLDER_PREFIX[] = "hlld.";
static const int FOLDER_PREFIX_LEN = sizeof(FOLDER_PREFIX) - 1;

static hlld_set_wrapper* find_set(hlld_setmgr *mgr, char *set_name);
static hlld_set_wrapper* take_set(hlld_setmgr *mgr, char *set_name);
static void delete_set(hlld_set_wrapper *set);
static int add_set(hlld_setmgr *mgr, char *set_name, hlld_config *config, int is_hot, int delta);
static int set_map_list_cb(void *data, const unsigned char *key, uint32_t key_len, void *value);
static int set_map_list_cold_cb(void *data, const unsigned char *key, uint32_t key_len, void *value);
static int set_map_delete_cb(void *data, const unsigned char *key, uint32_t key_len, void *value);
static int load_existing_sets(hlld_setmgr *mgr);
static unsigned long long create_delta_update(hlld_setmgr *mgr, delta_type type, hlld_set_wrapper *set);
static void* setmgr_thread_main(void *in);

/**
 * Initializer
 * @arg config The configuration
 * @arg vacuum Should vacuuming be enabled. True unless in a
 * test or embedded environment using setmgr_vacuum()
 * @arg mgr Output, resulting manager.
 * @return 0 on success.
 */
int init_set_manager(hlld_config *config, int vacuum, hlld_setmgr **mgr) {
    // Allocate a new object
    hlld_setmgr *m = *mgr = calloc(1, sizeof(hlld_setmgr));

    // Copy the config
    m->config = config;

    // Initialize the locks
    pthread_mutex_init(&m->write_lock, NULL);
    INIT_HLLD_SPIN(&m->clients_lock);
    INIT_HLLD_SPIN(&m->pending_lock);

    // Allocate storage for the art trees
    art_tree *trees = calloc(2, sizeof(art_tree));
    m->set_map = trees;
    m->alt_set_map = trees+1;

    // Allocate the initial art tree
    int res = init_art_tree(m->set_map);
    if (res) {
        syslog(LOG_ERR, "Failed to allocate set map!");
        free(m);
        return -1;
    }

    // Discover existing sets
    load_existing_sets(m);

    // Initialize the alternate map
    res = art_copy(m->alt_set_map, m->set_map);
    if (res) {
        syslog(LOG_ERR, "Failed to copy set map to alternate!");
        destroy_set_manager(m);
        return -1;
    }

    // Start the vacuum thread
    m->should_run = vacuum;
    if (vacuum && pthread_create(&m->vacuum_thread, NULL, setmgr_thread_main, m)) {
        perror("Failed to start vacuum thread!");
        destroy_set_manager(m);
        return 1;
    }

    // Done
    return 0;
}

/**
 * Cleanup
 * @arg mgr The manager to destroy
 * @return 0 on success.
 */
int destroy_set_manager(hlld_setmgr *mgr) {
    // Stop the vacuum thread
    mgr->should_run = 0;
    if (mgr->vacuum_thread) pthread_join(mgr->vacuum_thread, NULL);

    // Nuke all the keys in the current version.
    art_iter(mgr->set_map, set_map_delete_cb, mgr);

    // Handle any delta operations
    set_list *next, *current = mgr->delta;
    while (current) {
        // Only delete pending creates, pending
        // deletes are still in the primary tree
        if (current->type == CREATE)
            delete_set(current->set);
        next = current->next;
        free(current);
        current = next;
    }

    // Free the clients
    setmgr_client *cl_next, *cl = mgr->clients;
    while (cl) {
        cl_next = cl->next;
        free(cl);
        cl = cl_next;
    }

    // Destroy the ART trees
    destroy_art_tree(mgr->set_map);
    destroy_art_tree(mgr->alt_set_map);
    free((mgr->set_map < mgr->alt_set_map) ? mgr->set_map : mgr->alt_set_map);

    // Free the manager
    free(mgr);
    return 0;
}

/**
 * Should be invoked periodically by client threads to allow
 * the vacuum thread to cleanup garbage state. It should also
 * be called before making other calls into the set manager
 * so that it is aware of a client making use of the current
 * state.
 * @arg mgr The manager
 */
void setmgr_client_checkpoint(hlld_setmgr *mgr) {
    // Get a reference to ourself
    pthread_t id = pthread_self();

    // Look for our ID, and update the version
    // This is O(n), but N is small and its done infrequently
    setmgr_client *cl = mgr->clients;
    while (cl) {
        if (cl->id == id) {
            cl->vsn = mgr->vsn;
            return;
        }
        cl = cl->next;
    }

    // If we make it here, we are not a client yet
    // so we need to safely add ourself
    cl = malloc(sizeof(setmgr_client));
    cl->id = id;
    cl->vsn = mgr->vsn;

    // Critical section for the flip
    LOCK_HLLD_SPIN(&mgr->clients_lock);

    cl->next = mgr->clients;
    mgr->clients = cl;

    UNLOCK_HLLD_SPIN(&mgr->clients_lock);
}

/**
 * Should be invoked by clients when they no longer
 * need to make use of the set manager. This
 * allows the vacuum thread to cleanup garbage state.
 * @arg mgr The manager
 */
void setmgr_client_leave(hlld_setmgr *mgr) {
    // Get a reference to ourself
    pthread_t id = pthread_self();

    // Critical section
    LOCK_HLLD_SPIN(&mgr->clients_lock);

    // Look for our ID, and update the version
    // This is O(n), but N is small and its done infrequently
    setmgr_client **last_next = &mgr->clients;
    setmgr_client *cl = mgr->clients;
    while (cl) {
        if (cl->id == id) {
            // Set the last prev pointer to skip the current entry
            *last_next = cl->next;

            // Cleanup the memory associated
            free(cl);
            break;
        }
        last_next = &cl->next;
        cl = cl->next;
    }
    UNLOCK_HLLD_SPIN(&mgr->clients_lock);
}

/**
 * Flushes the set with the given name
 * @arg set_name The name of the set to flush
 * @return 0 on success. -1 if the set does not exist.
 */
int setmgr_flush_set(hlld_setmgr *mgr, char *set_name) {
    // Get the set
    hlld_set_wrapper *set = take_set(mgr, set_name);
    if (!set) return -1;

    // Acquire the READ lock. We use the read lock
    // since clients might inspect the hll, which
    // should not be cleared in the mean time
    pthread_rwlock_rdlock(&set->rwlock);

    // Flush
    hset_flush(set->set);

    // Release the lock
    pthread_rwlock_unlock(&set->rwlock);
    return 0;
}

/**
 * Sets keys in a given set
 * @arg set_name The name of the set
 * @arg keys A list of points to character arrays to add
 * @arg num_keys The number of keys to add
 * * @return 0 on success, -1 if the set does not exist.
 * -2 on internal error.
 */
int setmgr_set_keys(hlld_setmgr *mgr, char *set_name, char **keys, int num_keys) {
    // Get the set
    hlld_set_wrapper *set = take_set(mgr, set_name);
    if (!set) return -1;

    // Acquire the READ lock. We use the read lock
    // since we can handle concurrent writes.
    pthread_rwlock_rdlock(&set->rwlock);

    // Set the keys, store the results
    int res = 0;
    for (int i=0; i<num_keys; i++) {
        res = hset_add(set->set, keys[i]);
        if (res) break;
    }

    // Mark as hot
    set->is_hot = 1;

    // Release the lock
    pthread_rwlock_unlock(&set->rwlock);
    return (res == -1) ? -2 : 0;
}

/**
 * Estimates the size of a set
 * @arg set_name The name of the set
 * @arg est Output pointer, the estimate on success.
 * @return 0 on success, -1 if the set does not exist.
 */
int setmgr_set_size(hlld_setmgr *mgr, char *set_name, uint64_t *est) {
    // Get the set
    hlld_set_wrapper *set = take_set(mgr, set_name);
    if (!set) return -1;

    // Acquire the READ lock. We use the read lock
    // since we can handle concurrent read/writes.
    pthread_rwlock_rdlock(&set->rwlock);

    // Get the size
    *est = hset_size(set->set);

    // Release the lock
    pthread_rwlock_unlock(&set->rwlock);
    return 0;
}

/**
 * Creates a new set of the given name and parameters.
 * @arg set_name The name of the set
 * @arg custom_config Optional, can be null. Configs that override the defaults.
 * @return 0 on success, -1 if the set already exists.
 * -2 for internal error. -3 if there is a pending delete.
 */
int setmgr_create_set(hlld_setmgr *mgr, char *set_name, hlld_config *custom_config) {
    int res = 0;
    pthread_mutex_lock(&mgr->write_lock);

    /*
     * Bail if the set already exists.
     * -1 if the set is active
     * -3 if delete is pending
     */
    hlld_set_wrapper *set = find_set(mgr, set_name);
    if (set) {
        res = (set->is_active) ? -1 : -3;
        goto LEAVE;
    }

    // Scan the pending delete queue
    LOCK_HLLD_SPIN(&mgr->pending_lock);
    if (mgr->pending_deletes) {
        hlld_set_list *node = mgr->pending_deletes;
        while (node) {
            if (!strcmp(node->set_name, set_name)) {
                res = -3; // Pending delete
                UNLOCK_HLLD_SPIN(&mgr->pending_lock);
                goto LEAVE;
            }
            node = node->next;
        }
    }
    UNLOCK_HLLD_SPIN(&mgr->pending_lock);

    // Use a custom config if provided, else the default
    hlld_config *config = (custom_config) ? custom_config : mgr->config;

    // Add the set
    if (add_set(mgr, set_name, config, 1, 1)) {
        res = -2; // Internal error
    }

LEAVE:
    pthread_mutex_unlock(&mgr->write_lock);
    return res;
}

/**
 * Deletes the set entirely. This removes it from the set
 * manager and deletes it from disk. This is a permanent operation.
 * @arg set_name The name of the set to delete
 * @return 0 on success, -1 if the set does not exist.
 */
int setmgr_drop_set(hlld_setmgr *mgr, char *set_name) {
    // Lock the deletion
    int res = 0;
    pthread_mutex_lock(&mgr->write_lock);

    // Get the set
    hlld_set_wrapper *set = take_set(mgr, set_name);
    if (!set) {
        res = -1;
        goto LEAVE;
    }

    // Set the set to be non-active and mark for deletion
    set->is_active = 0;
    set->should_delete = 1;
    create_delta_update(mgr, DELETE, set);

LEAVE:
    pthread_mutex_unlock(&mgr->write_lock);
    return res;
}


/**
 * Clears the set from the internal data stores. This can only
 * be performed if the set is proxied.
 * @arg set_name The name of the set to delete
 * @return 0 on success, -1 if the set does not exist, -2
 * if the set is not proxied.
 */
int setmgr_clear_set(hlld_setmgr *mgr, char *set_name) {
    int res = 0;
    pthread_mutex_lock(&mgr->write_lock);

    // Get the set
    hlld_set_wrapper *set = take_set(mgr, set_name);
    if (!set) {
        res = -1;
        goto LEAVE;
    }

    // Check if the set is proxied
    if (!hset_is_proxied(set->set)) {
        res = -2;
        goto LEAVE;
    }

    // This is critical, as it prevents it from
    // being deleted. Instead, it is merely closed.
    set->is_active = 0;
    set->should_delete = 0;
    create_delta_update(mgr, DELETE, set);

LEAVE:
    pthread_mutex_unlock(&mgr->write_lock);
    return res;
}


/**
 * Unmaps the set from memory, but leaves it
 * registered in the set manager. This is rarely invoked
 * by a client, as it can be handled automatically by hlld,
 * but particular clients with specific needs may use it as an
 * optimization.
 * @arg set_name The name of the set to delete
 * @return 0 on success, -1 if the set does not exist.
 */
int setmgr_unmap_set(hlld_setmgr *mgr, char *set_name) {
    // Get the set
    hlld_set_wrapper *set = take_set(mgr, set_name);
    if (!set) return -1;

    // Bail if we are in memory only
    if (set->set->set_config.in_memory)
        goto LEAVE;

    // Acquire the write lock
    pthread_rwlock_wrlock(&set->rwlock);

    // Close the set
    hset_close(set->set);

    // Release the lock
    pthread_rwlock_unlock(&set->rwlock);

LEAVE:
    return 0;
}


/**
 * Allocates space for and returns a linked
 * list of all the sets.
 * @arg mgr The manager to list from
 * @arg prefix The prefix to match on or NULL
 * @arg head Output, sets to the address of the list header
 * @return 0 on success.
 */
int setmgr_list_sets(hlld_setmgr *mgr, char *prefix, hlld_set_list_head **head) {
    // Allocate the head
    hlld_set_list_head *h = *head = calloc(1, sizeof(hlld_set_list_head));

    // Check if we should use the prefix
    int prefix_len = 0;
    if (prefix) {
        prefix_len = strlen(prefix);
        art_iter_prefix(mgr->set_map, (unsigned char*)prefix, prefix_len, set_map_list_cb, h);
    } else
        art_iter(mgr->set_map, set_map_list_cb, h);

    // Joy... we have to potentially handle the delta updates
    if (mgr->primary_vsn == mgr->vsn) return 0;

    set_list *current = mgr->delta;
    hlld_set_wrapper *s;
    while (current) {
        // Check if this is a match (potential prefix)
        if (current->type == CREATE) {
            s = current->set;
            if (!prefix_len || !strncmp(s->set->set_name, prefix, prefix_len)) {
                s = current->set;
                set_map_list_cb(h, (unsigned char*)s->set->set_name, 0, s);
            }
        }

        // Don't seek past what the primary set map incorporates
        if (current->vsn == mgr->primary_vsn + 1)
            break;
        current = current->next;
    }

    return 0;
}


/**
 * Allocates space for and returns a linked
 * list of all the cold sets. This has the side effect
 * of clearing the list of cold sets!
 * @arg mgr The manager to list from
 * @arg head Output, sets to the address of the list header
 * @return 0 on success.
 */
int setmgr_list_cold_sets(hlld_setmgr *mgr, hlld_set_list_head **head) {
    // Allocate the head of a new hashmap
    hlld_set_list_head *h = *head = calloc(1, sizeof(hlld_set_list_head));

    // Scan for the cold sets. Ignore deltas, since they are either
    // new (e.g. hot), or being deleted anyways.
    art_iter(mgr->set_map, set_map_list_cold_cb, h);
    return 0;
}


/**
 * This method allows a callback function to be invoked with hlld set.
 * The purpose of this is to ensure that a hlld set is not deleted or
 * otherwise destroyed while being referenced. The set is not locked
 * so clients should under no circumstance use this to read/write to the set.
 * It should be used to read metrics, size information, etc.
 * @return 0 on success, -1 if the set does not exist.
 */
int setmgr_set_cb(hlld_setmgr *mgr, char *set_name, set_cb cb, void* data) {
    // Get the set
    hlld_set_wrapper *set = take_set(mgr, set_name);
    if (!set) return -1;

    // Callback
    cb(data, set_name, set->set);
    return 0;
}


/**
 * Convenience method to cleanup a set list.
 */
void setmgr_cleanup_list(hlld_set_list_head *head) {
    hlld_set_list *next, *current = head->head;
    while (current) {
        next = current->next;
        free(current->set_name);
        free(current);
        current = next;
    }
    free(head);
}


/**
 * Searches the primary tree and the delta list for a set
 */
static hlld_set_wrapper* find_set(hlld_setmgr *mgr, char *set_name) {
    // Search the tree first
    hlld_set_wrapper *set = art_search(mgr->set_map, (unsigned char*)set_name, strlen(set_name)+1);

    // If we found the set, check if it is active
    if (set) return set;

    // Check if the primary has all delta changes
    if (mgr->primary_vsn == mgr->vsn) return NULL;

    // Search the delta list
    set_list *current = mgr->delta;
    while (current) {
        // Check if this is a match
        if (current->type != BARRIER &&
            strcmp(current->set->set->set_name, set_name) == 0) {
            return current->set;
        }

        // Don't seek past what the primary set map incorporates
        if (current->vsn == mgr->primary_vsn + 1)
            break;
        current = current->next;
    }

    // Not found
    return NULL;
}


/**
 * Gets the hlld set in a thread safe way.
 */
static hlld_set_wrapper* take_set(hlld_setmgr *mgr, char *set_name) {
    hlld_set_wrapper *set = find_set(mgr, set_name);
    return (set && set->is_active) ? set : NULL;
}

/**
 * Invoked to cleanup a set once we
 * have hit 0 remaining references.
 */
static void delete_set(hlld_set_wrapper *set) {
    // Delete or Close the set
    if (set->should_delete)
        hset_delete(set->set);
    else
        hset_close(set->set);

    // Cleanup the set
    destroy_set(set->set);

    // Release any custom configs
    if (set->custom) {
        free(set->custom);
    }

    // Release the struct
    free(set);
    return;
}

/**
 * Creates a new set and adds it to the set set.
 * @arg mgr The manager to add to
 * @arg set_name The name of the set
 * @arg config The configuration for the set
 * @arg is_hot Is the set hot. False for existing.
 * @arg delta Should a delta entry be added, or the primary tree updated.
 * This is usually 0, except during initialization when it is safe to update
 * the primary tree.
 * @return 0 on success, -1 on error
 */
static int add_set(hlld_setmgr *mgr, char *set_name, hlld_config *config, int is_hot, int delta) {
    // Create the set
    hlld_set_wrapper *set = calloc(1, sizeof(hlld_set_wrapper));
    set->is_active = 1;
    set->is_hot = is_hot;
    set->should_delete = 0;
    pthread_rwlock_init(&set->rwlock, NULL);

    // Set the custom set if its not the same
    if (mgr->config != config) {
        set->custom = config;
    }

    // Try to create the underlying set. Only discover if it is hot.
    int res = init_set(config, set_name, is_hot, &set->set);
    if (res != 0) {
        free(set);
        return -1;
    }

    // Check if we are adding a delta value or directly updating ART tree
    if (delta)
        create_delta_update(mgr, CREATE, set);
    else
        art_insert(mgr->set_map, (unsigned char*)set_name, strlen(set_name)+1, set);

    return 0;
}

/**
 * Called as part of the hashmap callback
 * to list all the sets. Only works if value is
 * not NULL.
 */
static int set_map_list_cb(void *data, const unsigned char *key, uint32_t key_len, void *value) {
    (void)key_len;
    // set out the non-active nodes
    hlld_set_wrapper *set = value;
    if (!set->is_active) return 0;

    // Cast the inputs
    hlld_set_list_head *head = data;

    // Allocate a new entry
    hlld_set_list *node = malloc(sizeof(hlld_set_list));

    // Setup
    node->set_name = strdup((char*)key);
    node->next = NULL;

    // Inject at head if first node
    if (!head->head) {
        head->head = node;
        head->tail = node;

    // Inject at tail
    } else {
        head->tail->next = node;
        head->tail = node;
    }
    head->size++;
    return 0;
}

/**
 * Called as part of the hashmap callback
 * to list cold sets. Only works if value is
 * not NULL.
 */
static int set_map_list_cold_cb(void *data, const unsigned char *key, uint32_t key_len, void *value) {
    (void)key_len;
    // Cast the inputs
    hlld_set_list_head *head = data;
    hlld_set_wrapper *set = value;

    // Check if hot, turn off and skip
    if (set->is_hot) {
        set->is_hot = 0;
        return 0;
    }

    // Check if proxied
    if (hset_is_proxied(set->set)) {
        return 0;
    }

    // Allocate a new entry
    hlld_set_list *node = malloc(sizeof(hlld_set_list));

    // Setup
    node->set_name = strdup((char*)key);
    node->next = head->head;

    // Inject
    head->head = node;
    head->size++;
    return 0;
}

/**
 * Called as part of the hashmap callback
 * to cleanup the sets.
 */
static int set_map_delete_cb(void *data, const unsigned char *key, uint32_t key_len, void *value) {
    (void)data;
    (void)key;
    (void)key_len;
    hlld_set_wrapper *set = value;
    delete_set(set);
    return 0;
}

/**
 * Works with scandir to set out non-hlld folders.
 */
static int set_hlld_folders(CONST_DIRENT_T *d) {
    // Get the file name
    char *name = (char*)d->d_name;

    // Look if it ends in ".data"
    int name_len = strlen(name);

    // Too short
    if (name_len <= FOLDER_PREFIX_LEN) return 0;

    // Compare the prefix
    if (strncmp(name, FOLDER_PREFIX, FOLDER_PREFIX_LEN) == 0) {
        return 1;
    }

    // Do not store
    return 0;
}

/**
 * Loads the existing sets. This is not thread
 * safe and assumes that we are being initialized.
 */
static int load_existing_sets(hlld_setmgr *mgr) {
    struct dirent **namelist;
    int num;

    num = scandir(mgr->config->data_dir, &namelist, set_hlld_folders, NULL);
    if (num == -1) {
        syslog(LOG_ERR, "Failed to scan files for existing sets!");
        return -1;
    }
    syslog(LOG_INFO, "Found %d existing sets", num);

    // Add all the sets
    for (int i=0; i< num; i++) {
        char *folder_name = namelist[i]->d_name;
        char *set_name = folder_name + FOLDER_PREFIX_LEN;
        if (add_set(mgr, set_name, mgr->config, 0, 0)) {
            syslog(LOG_ERR, "Failed to load set '%s'!", set_name);
        }
    }

    for (int i=0; i < num; i++) free(namelist[i]);
    free(namelist);
    return 0;
}


/**
 * Creates a new delta update and adds to the head of the list.
 * This must be invoked with the write lock as it is unsafe.
 * @arg mgr The manager
 * @arg type The type of delta
 * @arg set The set that is affected
 * @return The new version we created
 */
static unsigned long long create_delta_update(hlld_setmgr *mgr, delta_type type, hlld_set_wrapper *set) {
    set_list *delta = malloc(sizeof(set_list));
    delta->vsn = ++mgr->vsn;
    delta->type = type;
    delta->set = set;
    delta->next = mgr->delta;
    mgr->delta = delta;
    return delta->vsn;
}

/**
 * Merges changes into the alternate tree from the delta lists
 * Safety: Safe ONLY if no other thread is using alt_set_map
 */
static void merge_old_versions(hlld_setmgr *mgr, set_list *delta, unsigned long long min_vsn) {
    // Handle older delta first (bottom up)
    if (delta->next) merge_old_versions(mgr, delta->next, min_vsn);

    // Check if we should skip this update
    if (delta->vsn > min_vsn) return;

    // Handle current update
    hlld_set_wrapper *s = delta->set;
    switch (delta->type) {
        case CREATE:
            art_insert(mgr->alt_set_map, (unsigned char*)s->set->set_name, strlen(s->set->set_name)+1, s);
            break;
        case DELETE:
            art_delete(mgr->alt_set_map, (unsigned char*)s->set->set_name, strlen(s->set->set_name)+1);
            break;
        case BARRIER:
            // Ignore the barrier...
            break;
    }
}

/**
 * Updates the pending deletes list with a list of pending deletes
 */
static void mark_pending_deletes(hlld_setmgr *mgr, unsigned long long min_vsn) {
    hlld_set_list *tmp, *pending = NULL;

    // Add each delete
    set_list *delta = mgr->delta;
    while (delta) {
        if (delta->vsn <= min_vsn && delta->type == DELETE) {
            tmp = malloc(sizeof(hlld_set_list));
            tmp->set_name = strdup(delta->set->set->set_name);
            tmp->next = pending;
            pending = tmp;
        }
        delta = delta->next;
    }

    LOCK_HLLD_SPIN(&mgr->pending_lock);
    mgr->pending_deletes = pending;
    UNLOCK_HLLD_SPIN(&mgr->pending_lock);
}

/**
 * Clears the pending deletes
 */
static void clear_pending_deletes(hlld_setmgr *mgr) {
    // Get a reference to the head
    hlld_set_list *pending = mgr->pending_deletes;
    if (!pending) return;

    // Set the pending list to NULL safely
    LOCK_HLLD_SPIN(&mgr->pending_lock);
    mgr->pending_deletes = NULL;
    UNLOCK_HLLD_SPIN(&mgr->pending_lock);

    // Free the nodes
    hlld_set_list *next;
    while (pending) {
        free(pending->set_name);
        next = pending->next;
        free(pending);
        pending = next;
    }
}

/**
 * Swap the alternate / primary maps, sets the primary_vsn
 * This is always safe, since its just a pointer swap.
 */
static void swap_set_maps(hlld_setmgr *mgr, unsigned long long primary_vsn) {
    art_tree *tmp = mgr->set_map;
    mgr->set_map = mgr->alt_set_map;
    mgr->alt_set_map = tmp;
    mgr->primary_vsn = primary_vsn;
}

/*
 * Scans a set_list* until it finds an entry with a version
 * less than min_vsn. It NULLs the pointer to that version
 * and returns a pointer to that node.
 *
 * Safety: This is ONLY safe if the minimum client version
 * and the primary_vsn is strictly greater than the min_vsn argument.
 * This ensures access to older delta entries will not happen.
 */
static set_list* remove_delta_versions(set_list *init, set_list **ref, unsigned long long min_vsn) {
    set_list *current = init;
    set_list **prev = ref;
    while (current && current->vsn > min_vsn) {
        prev = &current->next;
        current = current->next;
    }

    // NULL out the reference pointer to current node if any
    if (current) *prev = NULL;
    return current;
}

/**
 * Deletes old versions from the delta lists, and calls
 * delete_set on the sets in the destroyed list.
 *
 * Safety: Same as remove_delta_versions
 */
static void delete_old_versions(hlld_setmgr *mgr, unsigned long long min_vsn) {
    // Get the merged in pending ops, lock to avoid a race
    pthread_mutex_lock(&mgr->write_lock);
    set_list *old = remove_delta_versions(mgr->delta, &mgr->delta, min_vsn);
    pthread_mutex_unlock(&mgr->write_lock);

    // Delete the sets now that we have merged into both trees
    set_list *next, *current = old;
    while (current) {
        if (current->type == DELETE) delete_set(current->set);
        next = current->next;
        free(current);
        current = next;
    }
}

/**
 * Determines the minimum visible version from the client list
 * Safety: Always safe
 */
static unsigned long long client_min_vsn(hlld_setmgr *mgr) {
    // Determine the minimum version
    unsigned long long thread_vsn, min_vsn = mgr->vsn;
    for (setmgr_client *cl=mgr->clients; cl != NULL; cl=cl->next) {
        thread_vsn = cl->vsn;
        if (thread_vsn < min_vsn) min_vsn = thread_vsn;
    }
    return min_vsn;
}

/**
 * Creates a barrier that is implicit by adding a
 * new version, and waiting for all clients to reach
 * that version. This can be used as a non-locking
 * syncronization mechanism.
 */
static void version_barrier(hlld_setmgr *mgr) {
    // Create a new delta
    pthread_mutex_lock(&mgr->write_lock);
    unsigned long long vsn = create_delta_update(mgr, BARRIER, NULL);
    pthread_mutex_unlock(&mgr->write_lock);

    // Wait until we converge on the version
    while (mgr->should_run && client_min_vsn(mgr) < vsn)
        usleep(VACUUM_POLL_USEC);
}

/**
 * This thread is started after initialization to maintain
 * the state of the set manager. It's current use is to
 * cleanup the garbage created by our MVCC model. We do this
 * by making use of periodic 'checkpoints'. Our worker threads
 * report the version they are currently using, and we are always
 * able to delete versions that are strictly less than the minimum.
 */
static void* setmgr_thread_main(void *in) {
    // Extract our arguments
    hlld_setmgr *mgr = in;
    unsigned long long min_vsn, mgr_vsn;
    while (mgr->should_run) {
        // Do nothing if there is no changes
        if (mgr->vsn == mgr->primary_vsn) {
            usleep(VACUUM_POLL_USEC);
            continue;
        }

        /*
         * Because we use a version barrier, we always
         * end up creating a new version when we try to
         * apply delta updates. We need to handle the special case
         * where we are 1 version behind and the only delta is
         * a barrier. Do this by just updating primary_vsn.
         */
        mgr_vsn = mgr->vsn;
        if ((mgr_vsn - mgr->primary_vsn) == 1) {
            pthread_mutex_lock(&mgr->write_lock);

            // Ensure no version created in the mean time
            int should_continue = 0;
            if (mgr_vsn == mgr->vsn && mgr->delta->type == BARRIER) {
                mgr->primary_vsn = mgr_vsn;
                should_continue = 1;
            }

            // Release the lock and see if we should loop back
            pthread_mutex_unlock(&mgr->write_lock);
            if (should_continue) {
                syslog(LOG_INFO, "All updates applied. (vsn: %llu)", mgr_vsn);
                continue;
            }
        }

        // Determine the minimum version
        min_vsn = client_min_vsn(mgr);

        // Warn if there are a lot of outstanding deltas
        if (mgr->vsn - min_vsn > WARN_THRESHOLD) {
            syslog(LOG_WARNING, "Many delta versions detected! min: %llu (vsn: %llu)",
                    min_vsn, mgr->vsn);
        } else {
            syslog(LOG_DEBUG, "Applying delta update up to: %llu (vsn: %llu)",
                    min_vsn, mgr->vsn);
        }

        // Merge the old versions into the alternate three
        merge_old_versions(mgr, mgr->delta, min_vsn);

        /*
         * Mark any pending deletes so that create does not allow
         * a set to be created before we manage to call delete_old_versions.
         * There is an unfortunate race that can happen if a client
         * does a create/drop/create cycle, where the create/drop are
         * reflected in the set_map, and thus the second create is allowed
         * BEFORE we have had a chance to actually handle the delete.
         */
        mark_pending_deletes(mgr, min_vsn);

        // Swap the maps
        swap_set_maps(mgr, min_vsn);

        // Wait on a barrier until nobody is using the old tree
        version_barrier(mgr);

        // Merge the changes into the other tree now that its safe
        merge_old_versions(mgr, mgr->delta, min_vsn);

        // Both trees have the changes incorporated, safe to delete
        delete_old_versions(mgr, min_vsn);

        // Clear the pending delete list, since delete_old_versions() will
        // block untill all deletes are completed.
        clear_pending_deletes(mgr);

        // Log that we finished
        syslog(LOG_INFO, "Finished delta updates up to: %llu (vsn: %llu)",
                min_vsn, mgr->vsn);
    }
    return NULL;
}


/**
 * This method is used to force a vacuum up to the current
 * version. It is generally unsafe to use in hlld,
 * but can be used in an embeded or test environment.
 */
void setmgr_vacuum(hlld_setmgr *mgr) {
    unsigned long long vsn = mgr->vsn;
    merge_old_versions(mgr, mgr->delta, vsn);
    swap_set_maps(mgr, vsn);
    merge_old_versions(mgr, mgr->delta, vsn);
    delete_old_versions(mgr, vsn);
}

