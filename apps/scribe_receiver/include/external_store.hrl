-ifndef(_external_store_included).
-define(_external_store_included, yeah).

-define(ROTATE_DAILY, daily).
-define(ROTATE_HOURLY, hourly).
-define(ROTATE_OTHER, other).
-define(ROTATE_NEVER, never).

-define(META_LOGFILE_PREFIX, "scribe_meta<new_logfile>: ").

-record(baseConfig, {category, multi_category, readable, targetWriteSize = 16384, maxWriteInterval = 10}).
-record(queueState, {currentSize=0, messages = []}).

-endif.
