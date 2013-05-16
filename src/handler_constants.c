#include <regex.h>

/*
 * Various messages and responses
 */
static const char CLIENT_ERR[] = "Client Error: ";
static const int CLIENT_ERR_LEN = sizeof(CLIENT_ERR) - 1;

static const char CMD_NOT_SUP[] = "Command not supported";
static const int CMD_NOT_SUP_LEN = sizeof(CMD_NOT_SUP) - 1;

static const char BAD_ARGS[] = "Bad arguments";
static const int BAD_ARGS_LEN= sizeof(BAD_ARGS) - 1;

static const char UNEXPECTED_ARGS[] = "Unexpected arguments";
static const int UNEXPECTED_ARGS_LEN = sizeof(UNEXPECTED_ARGS) - 1;

static const char SET_KEY_NEEDED[] = "Must provide set name and key";
static const int SET_KEY_NEEDED_LEN = sizeof(SET_KEY_NEEDED) - 1;

static const char SET_NEEDED[] = "Must provide set name";
static const int SET_NEEDED_LEN = sizeof(SET_NEEDED) - 1;

static const char BAD_SET_NAME[] = "Bad set name";
static const int BAD_SET_NAME_LEN = sizeof(BAD_SET_NAME) - 1;

static const char INTERNAL_ERR[] = "Internal Error\n";
static const int INTERNAL_ERR_LEN = sizeof(INTERNAL_ERR) - 1;

static const char SET_NOT_EXIST[] = "Set does not exist\n";
static const int SET_NOT_EXIST_LEN = sizeof(SET_NOT_EXIST) - 1;

static const char SET_NOT_PROXIED[] = "Set is not proxied. Close it first.\n";
static const int SET_NOT_PROXIED_LEN = sizeof(SET_NOT_PROXIED) - 1;

static const char DELETE_IN_PROGRESS[] = "Delete in progress\n";
static const int DELETE_IN_PROGRESS_LEN = sizeof(DELETE_IN_PROGRESS) - 1;

static const char DONE_RESP[] = "Done\n";
static const int DONE_RESP_LEN = sizeof(DONE_RESP) - 1;

static const char EXISTS_RESP[] = "Exists\n";
static const int EXISTS_RESP_LEN = sizeof(EXISTS_RESP) - 1;

static const char NEW_LINE[] = "\n";
static const int NEW_LINE_LEN = sizeof(NEW_LINE) - 1;

static const char START_RESP[] = "START\n";
static const int START_RESP_LEN = sizeof(START_RESP) - 1;

static const char END_RESP[] = "END\n";
static const int END_RESP_LEN = sizeof(END_RESP) - 1;

typedef enum {
    UNKNOWN = 0,    // Unrecognized command
    SET,            // Set a single key
    SET_MULTI,      // Set multiple space-seperated keys
    LIST,           // List sets
    INFO,           // Info about a set
    CREATE,         // Creates a set
    DROP,           // Drop a set
    CLOSE,          // Close a set
    CLEAR,          // Clears a set from the internals
    FLUSH,          // Force flush a set
} conn_cmd_type;

/* Static regexes */
static regex_t VALID_SET_NAMES_RE;
static const char *VALID_SET_NAMES_PATTERN = "^[^ \t\n\r]{1,200}$";

