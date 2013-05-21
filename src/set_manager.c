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
 * We use a linked list of setmgr_vsn structs
 * as a simple form of Multi-Version Concurrency Controll (MVCC).
 * The latest version is always the head of the list, and older
 * versions are maintained as a linked list. A separate vacuum thread
 * is used to clean out the old version. This allows reads against the
 * head to be non-blocking.
 */
typedef struct setmgr_vsn {
    unsigned long long vsn;

    // Maps key names -> hlld_set_wrapper
    art_tree set_map;

    // Holds a reference to the deleted set, since
    // it is no longer in the hash map
    hlld_set_wrapper *deleted;
    struct setmgr_vsn *prev;
} setmgr_vsn;

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

struct hlld_setmgr {
    hlld_config *config;

    setmgr_vsn *latest;
    pthread_mutex_t write_lock; // Serializes destructive operations
    pthread_mutex_t vacuum_lock; // Held while vacuum is happening

    volatile int should_run;  // Used to stop the vacuum thread
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

static hlld_set_wrapper* take_set(setmgr_vsn *vsn, char *set_name);
static void delete_set(hlld_set_wrapper *set);
static int add_set(hlld_setmgr *mgr, setmgr_vsn *vsn, char *set_name, hlld_config *config, int is_hot);
static int set_map_list_cb(void *data, const char *key, uint32_t key_len, void *value);
static int set_map_list_cold_cb(void *data, const char *key, uint32_t key_len, void *value);
static int set_map_delete_cb(void *data, const char *key, uint32_t key_len, void *value);
static int load_existing_sets(hlld_setmgr *mgr);
static setmgr_vsn* create_new_version(hlld_setmgr *mgr);
static void destroy_version(setmgr_vsn *vsn);
static void* setmgr_thread_main(void *in);

/**
 * Initializer
 * @arg config The configuration
 * @arg mgr Output, resulting manager.
 * @return 0 on success.
 */
int init_set_manager(hlld_config *config, hlld_setmgr **mgr) {
    // Allocate a new object
    hlld_setmgr *m = *mgr = calloc(1, sizeof(hlld_setmgr));

    // Copy the config
    m->config = config;

    // Initialize the locks
    pthread_mutex_init(&m->write_lock, NULL);
    pthread_mutex_init(&m->vacuum_lock, NULL);
    INIT_HLLD_SPIN(&m->clients_lock);

    // Allocate the initial version and hash table
    setmgr_vsn *vsn = calloc(1, sizeof(setmgr_vsn));
    m->latest = vsn;
    int res = init_art_tree(&vsn->set_map);
    if (res) {
        syslog(LOG_ERR, "Failed to allocate set map!");
        free(m);
        return -1;
    }

    // Discover existing sets
    load_existing_sets(m);

    // Start the vacuum thread
    m->should_run = 1;
    if (pthread_create(&m->vacuum_thread, NULL, setmgr_thread_main, m)) {
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

    // Nuke all the keys in the current version
    setmgr_vsn *current = mgr->latest;
    art_iter(&current->set_map, set_map_delete_cb, mgr);

    // Destroy the versions
    setmgr_vsn *next, *vsn = mgr->latest;
    while (vsn) {
        // Handle any lingering deletes
        if (vsn->deleted) delete_set(vsn->deleted);
        next = vsn->prev;
        destroy_version(vsn);
        vsn = next;
    }

    // Free the clients
    setmgr_client *cl_next, *cl = mgr->clients;
    while (cl) {
        cl_next = cl->next;
        free(cl);
        cl = cl_next;
    }

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
            cl->vsn = mgr->latest->vsn;
            return;
        }
        cl = cl->next;
    }

    // If we make it here, we are not a client yet
    // so we need to safely add ourself
    cl = malloc(sizeof(setmgr_client));
    cl->id = id;
    cl->vsn = mgr->latest->vsn;

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
    setmgr_vsn *current = mgr->latest;
    hlld_set_wrapper *set = take_set(current, set_name);
    if (!set) return -1;

    // Flush
    hset_flush(set->set);
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
    setmgr_vsn *current = mgr->latest;
    hlld_set_wrapper *set = take_set(current, set_name);
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
    setmgr_vsn *current = mgr->latest;
    hlld_set_wrapper *set = take_set(current, set_name);
    if (!set) return -1;

    // Acquire the READ lock. We use the read lock
    // since we can handle concurrent read/writes.
    pthread_rwlock_rdlock(&set->rwlock);

    // Get the size
    *est = hset_size(set->set);

    // Mark as hot
    set->is_hot = 1;

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
    // Lock the creation
    pthread_mutex_lock(&mgr->write_lock);

    // Bail if the set already exists
    hlld_set_wrapper *set = NULL;
    setmgr_vsn *latest = mgr->latest;
    set = art_search(&latest->set_map, set_name, strlen(set_name)+1);
    if (set) {
        pthread_mutex_unlock(&mgr->write_lock);
        return -1;
    }

    // Scan for a pending delete
    pthread_mutex_lock(&mgr->vacuum_lock);
    setmgr_vsn *vsn = mgr->latest->prev;
    while (vsn) {
        if (vsn->deleted && strcmp(vsn->deleted->set->set_name, set_name) == 0) {
            syslog(LOG_WARNING, "Tried to create set '%s' with a pending delete!", set_name);
            pthread_mutex_unlock(&mgr->vacuum_lock);
            pthread_mutex_unlock(&mgr->write_lock);
            return -3;
        }
        vsn = vsn->prev;
    }
    pthread_mutex_unlock(&mgr->vacuum_lock);

    // Create a new version
    setmgr_vsn *new_vsn = create_new_version(mgr);

    // Use a custom config if provided, else the default
    hlld_config *config = (custom_config) ? custom_config : mgr->config;

    // Add the set to the new version
    int res = add_set(mgr, new_vsn, set_name, config, 1);
    if (res != 0) {
        destroy_version(new_vsn);
        res = -2; // Internal error
    } else {
        // Install the new version
        mgr->latest = new_vsn;
    }

    // Release the lock
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
    pthread_mutex_lock(&mgr->write_lock);

    // Get the set
    setmgr_vsn *current = mgr->latest;
    hlld_set_wrapper *set = take_set(current, set_name);
    if (!set) {
        pthread_mutex_unlock(&mgr->write_lock);
        return -1;
    }

    // Set the set to be non-active and mark for deletion
    set->is_active = 0;
    set->should_delete = 1;

    // Create a new version without this set
    setmgr_vsn *new_vsn = create_new_version(mgr);
    art_delete(&new_vsn->set_map, set_name, strlen(set_name)+1);
    current->deleted = set;

    // Install the new version
    mgr->latest = new_vsn;

    // Unlock
    pthread_mutex_unlock(&mgr->write_lock);
    return 0;
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
    setmgr_vsn *current = mgr->latest;
    hlld_set_wrapper *set = take_set(current, set_name);
    if (!set) return -1;

    // Only do it if we are not in memory
    if (!set->set->set_config.in_memory) {
        // Acquire the write lock
        pthread_rwlock_wrlock(&set->rwlock);

        // Close the set
        hset_close(set->set);

        // Release the lock
        pthread_rwlock_unlock(&set->rwlock);
    }

    return 0;
}


/**
 * Clears the set from the internal data stores. This can only
 * be performed if the set is proxied.
 * @arg set_name The name of the set to delete
 * @return 0 on success, -1 if the set does not exist, -2
 * if the set is not proxied.
 */
int setmgr_clear_set(hlld_setmgr *mgr, char *set_name) {
    // Lock the deletion
    pthread_mutex_lock(&mgr->write_lock);

    // Get the set
    setmgr_vsn *current = mgr->latest;
    hlld_set_wrapper *set = take_set(current, set_name);
    if (!set) {
        pthread_mutex_unlock(&mgr->write_lock);
        return -1;
    }

    // Check if the set is proxied
    if (!hset_is_proxied(set->set)) {
        pthread_mutex_unlock(&mgr->write_lock);
        return -2;
    }

    // This is critical, as it prevents it from
    // being deleted. Instead, it is merely closed.
    set->is_active = 0;
    set->should_delete = 0;

    // Create a new version without this set
    setmgr_vsn *new_vsn = create_new_version(mgr);
    art_delete(&new_vsn->set_map, set_name, strlen(set_name)+1);
    current->deleted = set;

    // Install the new version
    mgr->latest = new_vsn;

    // Unlock
    pthread_mutex_unlock(&mgr->write_lock);
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

    // Iterate through a callback to append
    setmgr_vsn *current = mgr->latest;

    // Check if we should use the prefix
    if (prefix)
        art_iter_prefix(&current->set_map, prefix, strlen(prefix), set_map_list_cb, h);
    else
        art_iter(&current->set_map, set_map_list_cb, h);
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

    // Scan for the cold sets
    setmgr_vsn *current = mgr->latest;
    art_iter(&current->set_map, set_map_list_cold_cb, h);
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
    setmgr_vsn *current = mgr->latest;
    hlld_set_wrapper *set = take_set(current, set_name);
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
 * Gets the hlld set in a thread safe way.
 */
static hlld_set_wrapper* take_set(setmgr_vsn *vsn, char *set_name) {
    hlld_set_wrapper *set = art_search(&vsn->set_map, set_name, strlen(set_name)+1);
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
 * @arg vsn The version to add to
 * @arg set_name The name of the set
 * @arg config The configuration for the set
 * @arg is_hot Is the set hot. False for existing.
 * @return 0 on success, -1 on error
 */
static int add_set(hlld_setmgr *mgr, setmgr_vsn *vsn, char *set_name, hlld_config *config, int is_hot) {
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

    // Add to the hash map
    art_insert(&vsn->set_map, set_name, strlen(set_name)+1, set);
    return 0;
}

/**
 * Called as part of the hashmap callback
 * to list all the sets. Only works if value is
 * not NULL.
 */
static int set_map_list_cb(void *data, const char *key, uint32_t key_len, void *value) {
    (void)key_len;
    // set out the non-active nodes
    hlld_set_wrapper *set = value;
    if (!set->is_active) return 0;

    // Cast the inputs
    hlld_set_list_head *head = data;

    // Allocate a new entry
    hlld_set_list *node = malloc(sizeof(hlld_set_list));

    // Setup
    node->set_name = strdup(key);
    node->next = head->head;

    // Inject
    head->head = node;
    head->size++;
    return 0;
}

/**
 * Called as part of the hashmap callback
 * to list cold sets. Only works if value is
 * not NULL.
 */
static int set_map_list_cold_cb(void *data, const char *key, uint32_t key_len, void *value) {
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
    node->set_name = strdup(key);
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
static int set_map_delete_cb(void *data, const char *key, uint32_t key_len, void *value) {
    (void)data;
    (void)key;
    (void)key_len;

    // Cast the inputs
    hlld_set_wrapper *set = value;

    // Delete, but not the underlying files
    set->should_delete = 0;
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
        if (add_set(mgr, mgr->latest, set_name, mgr->config, 0)) {
            syslog(LOG_ERR, "Failed to load set '%s'!", set_name);
        }
    }

    for (int i=0; i < num; i++) free(namelist[i]);
    free(namelist);
    return 0;
}


/**
 * Creates a new version struct from the current version.
 * Does not install the new version in place. This should
 * be guarded by the write lock to prevent conflicting versions.
 */
static setmgr_vsn* create_new_version(hlld_setmgr *mgr) {
    // Create a new blank version
    setmgr_vsn *vsn = calloc(1, sizeof(setmgr_vsn));

    // Increment the version number
    setmgr_vsn *current = mgr->latest;
    vsn->vsn = mgr->latest->vsn + 1;

    // Set the previous pointer
    vsn->prev = current;

    // Initialize the map
    int res = art_copy(&vsn->set_map, &current->set_map);
    if (res) {
        syslog(LOG_ERR, "Failed to copy set map!");
        free(vsn);
        return NULL;
    }

    // Return the new version
    syslog(LOG_DEBUG, "(setmgr) Created new version %llu", vsn->vsn);
    return vsn;
}

// Destroys a version. Does basic cleanup.
static void destroy_version(setmgr_vsn *vsn) {
    destroy_art_tree(&vsn->set_map);
    free(vsn);
}

// Recursively waits and cleans up old versions
static int clean_old_versions(setmgr_vsn *v, unsigned long long min_vsn) {
    // Recurse if possible
    if (v->prev && clean_old_versions(v->prev, min_vsn))
        v->prev = NULL;

    // Abort if this version cannot be cleaned
    if (v->vsn >= min_vsn) return 0;

    // Log about the cleanup
    syslog(LOG_DEBUG, "(setmgr) Destroying version %llu", v->vsn);

    // Handle the cleanup
    if (v->deleted) {
        delete_set(v->deleted);
    }

    // Destroy this version
    destroy_version(v);
    return 1;
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

    // Store the oldest version
    setmgr_vsn *current;
    while (mgr->should_run) {
        sleep(1);
        if (!mgr->should_run) break;

        // Do nothing if there is no older versions
        current = mgr->latest;
        if (!current->prev) continue;

        // Determine the minimum version
        unsigned long long thread_vsn, min_vsn = current->vsn;
        for (setmgr_client *cl=mgr->clients; cl != NULL; cl=cl->next) {
            thread_vsn = cl->vsn;
            if (thread_vsn < min_vsn) {
                min_vsn = thread_vsn;
            }
        }

        // Check if it is too old
        if (current->vsn - min_vsn > WARN_THRESHOLD) {
            syslog(LOG_WARNING, "Many concurrent versions detected! Either slow operations, or too many changes! Current: %llu, Minimum: %llu", current->vsn, min_vsn);
        }

        // Cleanup the old versions
        pthread_mutex_lock(&mgr->vacuum_lock);
        clean_old_versions(current, min_vsn);
        pthread_mutex_unlock(&mgr->vacuum_lock);
    }
    return NULL;
}


/**
 * This method is used to force a vacuum up to the current
 * version. It is generally unsafe to use in hlld,
 * but can be used in an embeded or test environment.
 */
void setmgr_vacuum(hlld_setmgr *mgr) {
    // Cleanup the old versions
    pthread_mutex_lock(&mgr->vacuum_lock);
    clean_old_versions(mgr->latest, mgr->latest->vsn);
    pthread_mutex_unlock(&mgr->vacuum_lock);
}

