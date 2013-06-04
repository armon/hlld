#ifndef SET_MANAGER_H
#define SET_MANAGER_H
#include <pthread.h>
#include "config.h"
#include "set.h"

/**
 * Opaque handle to the set manager
 */
typedef struct hlld_setmgr hlld_setmgr;

/**
 * Lists of sets
 */
typedef struct hlld_set_list {
    char *set_name;
    struct hlld_set_list *next;
} hlld_set_list;

typedef struct {
   int size;
   hlld_set_list *head;
   hlld_set_list *tail;
} hlld_set_list_head;

/**
 * Initializer
 * @arg config The configuration
 * @arg vacuum Should vacuuming be enabled. True unless in a
 * test or embedded environment using setmgr_vacuum()
 * @arg mgr Output, resulting manager.
 * @return 0 on success.
 */
int init_set_manager(hlld_config *config, int vacuum, hlld_setmgr **mgr);

/**
 * Cleanup
 * @arg mgr The manager to destroy
 * @return 0 on success.
 */
int destroy_set_manager(hlld_setmgr *mgr);

/**
 * Should be invoked periodically by client threads to allow
 * the vacuum thread to cleanup garbage state. It should also
 * be called before making other calls into the set manager
 * so that it is aware of a client making use of the current
 * state.
 * @arg mgr The manager
 */
void setmgr_client_checkpoint(hlld_setmgr *mgr);

/**
 * Should be invoked by clients when they no longer
 * need to make use of the set manager. This
 * allows the vacuum thread to cleanup garbage state.
 * @arg mgr The manager
 */
void setmgr_client_leave(hlld_setmgr *mgr);

/**
 * Flushes the set with the given name
 * @arg set_name The name of the set to flush
 * @return 0 on success. -1 if the set does not exist.
 */
int setmgr_flush_set(hlld_setmgr *mgr, char *set_name);

/**
 * Sets keys in a given set
 * @arg set_name The name of the set
 * @arg keys A list of points to character arrays to add
 * @arg num_keys The number of keys to add
 * @return 0 on success, -1 if the set does not exist.
 * -2 on internal error.
 */
int setmgr_set_keys(hlld_setmgr *mgr, char *set_name, char **keys, int num_keys);

/**
 * Estimates the size of a set
 * @arg set_name The name of the set
 * @arg est Output pointer, the estimate on success.
 * @return 0 on success, -1 if the set does not exist.
 */
int setmgr_set_size(hlld_setmgr *mgr, char *set_name, uint64_t *est);

/**
 * Creates a new set of the given name and parameters.
 * @arg set_name The name of the set
 * @arg custom_config Optional, can be null. Configs that override the defaults.
 * @return 0 on success, -1 if the set already exists.
 * -2 for internal error.
 */
int setmgr_create_set(hlld_setmgr *mgr, char *set_name, hlld_config *custom_config);

/**
 * Deletes the set entirely. This removes it from the set
 * manager and deletes it from disk. This is a permanent operation.
 * @arg set_name The name of the set to delete
 * @return 0 on success, -1 if the set does not exist.
 */
int setmgr_drop_set(hlld_setmgr *mgr, char *set_name);

/**
 * Unmaps the set from memory, but leaves it
 * registered in the set manager. This is rarely invoked
 * by a client, as it can be handled automatically by hlld,
 * but particular clients with specific needs may use it as an
 * optimization.
 * @arg set_name The name of the set to delete
 * @return 0 on success, -1 if the set does not exist.
 */
int setmgr_unmap_set(hlld_setmgr *mgr, char *set_name);

/**
 * Clears the set from the internal data stores. This can only
 * be performed if the set is proxied.
 * @arg set_name The name of the set to delete
 * @return 0 on success, -1 if the set does not exist, -2
 * if the set is not proxied.
 */
int setmgr_clear_set(hlld_setmgr *mgr, char *set_name);

/**
 * Allocates space for and returns a linked
 * list of all the sets. The memory should be free'd by
 * the caller.
 * @arg mgr The manager to list from
 * @arg prefix The prefix to match on or NULL
 * @arg head Output, sets to the address of the list header
 * @return 0 on success.
 */
int setmgr_list_sets(hlld_setmgr *mgr, char *prefix, hlld_set_list_head **head);

/**
 * Allocates space for and returns a linked
 * list of all the cold sets. This has the side effect
 * of clearing the list of cold sets! The memory should
 * be free'd by the caller.
 * @arg mgr The manager to list from
 * @arg head Output, sets to the address of the list header
 * @return 0 on success.
 */
int setmgr_list_cold_sets(hlld_setmgr *mgr, hlld_set_list_head **head);

/**
 * Convenience method to cleanup a set list.
 */
void setmgr_cleanup_list(hlld_set_list_head *head);

/**
 * This method allows a callback function to be invoked with hlld set.
 * The purpose of this is to ensure that a hlld set is not deleted or
 * otherwise destroyed while being referenced. The set is not locked
 * so clients should under no circumstance use this to read/write to the set.
 * It should be used to read metrics, size information, etc.
 * @return 0 on success, -1 if the set does not exist.
 */
typedef void(*set_cb)(void* in, char *set_name, hlld_set *set);
int setmgr_set_cb(hlld_setmgr *mgr, char *set_name, set_cb cb, void* data);

/**
 * This method is used to force a vacuum up to the current
 * version. It is generally unsafe to use in hlld,
 * but can be used in an embeded or test environment.
 */
void setmgr_vacuum(hlld_setmgr *mgr);

#endif
