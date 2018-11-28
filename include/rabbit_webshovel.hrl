
-define(DEFAULT_PREFETCH_COUNT, 1000).
-define(DEFAULT_RECONNECT_DELAY, 5).
-define(ACKMODE_ALLOWED_VALUES, [no_ack,on_publish,on_confirm]).
-define(DEST_PROTOCOL_ALLOWED_VALUE, [http, https]).
-define(DEFAULT_DEST_PROTOCOL, http).
-define(HTTP_METHOD_ALLOWED_VALUE, [head, get, put, post, trace,
				    options, delete, patch]).
-define(DEFAULT_HTTP_METHOD, post).
-define(DEFAULT_CONTENT_TYPE,	"application/octet-stream").
