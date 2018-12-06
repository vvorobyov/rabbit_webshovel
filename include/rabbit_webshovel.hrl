-define(CHANNEL_LIMIT, 5).

-define(DEFAULT_PREFETCH_COUNT, 1000).
-define(DEFAULT_RECONNECT_DELAY, 5).
-define(DEFAULT_DEST_PROTOCOL, http).
-define(DEFAULT_HTTP_METHOD, post).
-define(DEFAULT_ENDPOINT_PUBLISH_HEADER, true).
-define(DEFAULT_ENDPOINT_PUBLISH_PROPERTIES, all).
-define(DEFAULT_CONTENT_TYPE,	"application/octet-stream").
-define(DEFAULT_ACK_MODE(RESPONSE), case RESPONSE of
                                        true  -> on_broker_confirm;
                                        false -> on_broker_confirm
                                    end).

-define(SRC_PROTOCOL_ALLOWED_VALUES, [amqp091, amqp10]).
-define(ACKMODE_ALLOWED_VALUES, [no_ack,
                                 on_endpoint_publish,on_endpoint_confirm,
                                 on_broker_publish,on_broker_confirm]).
-define(DEST_PROTOCOL_ALLOWED_VALUE, [http, https]).
-define(HTTP_METHOD_ALLOWED_VALUE, [head, get, put, post, trace,
                                    options, delete, patch]).


-record(src, {protocol,
              amqp_params,
              reconnect_delay}).


-record(endpoint, {protocol,
                   uri,
                   method,
                   publish_properties,
                   publish_headers}).

-record(response, {}).

-record(dst, {name,
              queue= <<"">>,
              prefetch_count,
              ack_mode,
              endpoint = #endpoint{},
              response}).

-record(webshovel, {name,
                    source = #src{},
                    destinations=[]}).

