%%%-------------------------------------------------------------------
%%% @author Vladislav Vorobyov <vlad@erldev>
%%% @copyright (C) 2018, Vladislav Vorobyov
%%% @doc
%%%
%%% @end
%%% Created : 23 Nov 2018 by Vladislav Vorobyov <vlad@erldev>
%%%-------------------------------------------------------------------
-module(rabbit_webshovel_utils).

-include("rabbit_webshovel.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
%% API
-export([parse/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @spec
%% @end
%%--------------------------------------------------------------------
-spec parse(WSName :: atom(), WSConfig :: list()) -> {ok, map()}.
parse(WSName, WSConfig)->
    try
        validate(WSConfig),
        parse_current(WSName, WSConfig)
    catch
        throw:{error, Reason} ->
            {error, {invalid_webshovel_configuration, WSName, Reason}};
        throw:Reason ->
            {error, {invalid_webshovel_configuration, WSName, Reason}}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%%================= Parse function ==================================

parse_current(WSName, WSConfig)->
    {source, Source0} = proplists:lookup(source, WSConfig),
    Source = parse_source(Source0),
    {destinations, Dest0} = proplists:lookup(destinations, WSConfig),
    Dest = parse_destinations(Dest0),
    {ok,  #webshovel{name = WSName,
                     source = Source,
                     destinations = Dest}}.
%% Разбор раздела source конфигурации
parse_source(SrcConfig) ->
    validate(SrcConfig),
    {protocol, Protocol0} = proplists:lookup(protocol, SrcConfig),
    Protocol = validate_parameter(
                 protocol,
                 fun validate_allowed_value/1,
                 {Protocol0, ?SRC_PROTOCOL_ALLOWED_VALUES}),
    {uris, URIs} = proplists:lookup(uris, SrcConfig),
    AMQPParams = validate_parameter(
                   uris,
                   fun validate_src_uris/1,
                   URIs),
    ReconnectDelay = validate_parameter(
                       reconnect_delay,
                       fun valid_non_negative_integer/1,
                       proplists:get_value(reconnect_delay,
                                           SrcConfig,
                                           ?DEFAULT_RECONNECT_DELAY)),
    #src{protocol = Protocol,
         amqp_params=AMQPParams,
         reconnect_delay=ReconnectDelay}.

parse_destinations(DestConfig) ->
    validate(DestConfig),
    Fun = fun({Name,Config}, Acc)->
		  validate(Config),
		  [parse_destination(Name, Config)|Acc]
	  end,
    lists:foldl(Fun, [], DestConfig).


parse_destination(Name, Config)->
    {queue, Queue0} = proplists:lookup(queue, Config),
    Queue = validate_parameter(queue,
                               fun valid_binary/1,
                               Queue0),
    PrefCount0 = proplists:get_value(prefetch_count,
                                         Config,
                                         ?DEFAULT_PREFETCH_COUNT),
    PrefCount = validate_parameter(prefetch_count,
                                   fun valid_non_negative_integer/1,
                                   PrefCount0),
    AckMode0 = proplists:get_value(
                 ack_mode,
                 Config,
                 ?DEFAULT_ACK_MODE(proplists:is_defined(response, Config))),
    AckMode = validate_parameter(
                ack_mode,
                fun validate_allowed_value/1,
                {AckMode0, ?ACKMODE_ALLOWED_VALUES}),
    {endpoint, EndPoint0} = proplists:lookup(endpoint, Config),
    EndPoint = parse_endpoint(EndPoint0),
    #dst{name = Name,
         queue = Queue,
         prefetch_count = PrefCount,
         ack_mode = AckMode,
         endpoint = EndPoint}.

parse_endpoint(Config) ->
    validate(Config),
    Protocol0 = proplists:get_value(protocol, Config,
                                    ?DEFAULT_DEST_PROTOCOL),
    Protocol = validate_parameter(protocol,
                                  fun validate_allowed_value/1,
                                  {Protocol0, ?DEST_PROTOCOL_ALLOWED_VALUE}),
    {uri, URI0} = proplists:lookup(uri, Config),
    URI = validate_parameter(uri, fun validate_dest_uri/1, URI0),
    Method0 = proplists:get_value(method, Config,
                                  ?DEFAULT_HTTP_METHOD),
    Method = validate_parameter(method,
                                fun validate_allowed_value/1,
                                {Method0, ?HTTP_METHOD_ALLOWED_VALUE}),
    PubHeaders0 = proplists:get_value(publish_headers, Config,
                                      ?DEFAULT_ENDPOINT_PUBLISH_HEADER),
    PubHeaders = validate_parameter(publish_headers,
                                    fun valid_boolean/1,
                                    PubHeaders0),
    PubProperties0 = proplists:get_value(publish_properties, Config,
                                         ?DEFAULT_ENDPOINT_PUBLISH_PROPERTIES),
    PubProperties = validate_publish_properties(PubProperties0),
    #endpoint{protocol = Protocol,
              uri = URI,
              method = Method,
              publish_headers = PubHeaders,
              publish_properties = PubProperties}.




%%% ================= Validate function ==============================

validate(Config) ->
    validate_proplist(Config),
    validate_duplicates(Config).

validate_proplist(Config) when is_list (Config) ->
    PropsFilterFun = fun ({_, _}) -> false;
                         (_) -> true
                     end,
    case lists:filter(PropsFilterFun, Config) of
        [] -> ok;
        Invalid ->
            throw({invalid_parameters, Invalid})
    end;
validate_proplist(X) ->
    throw({require_list, X}).

validate_duplicates(Config) ->
    case duplicate_keys(Config) of
        [] -> ok;
        Invalid ->
            throw({duplicate_parameters, Invalid})
    end.

duplicate_keys(PropList) when is_list(PropList) ->
    proplists:get_keys(
        lists:foldl(fun (K, L) -> lists:keydelete(K, 1, L) end, PropList,
            proplists:get_keys(PropList))).

validate_publish_properties(none) ->
    none;
validate_publish_properties(all) ->
    all;
validate_publish_properties(List) when is_list(List) ->
    DublOrNotProps = list:foldl(
                       fun (Item, Acc)->
                               lists:delete(Item, Acc) end,
                       List,
                       record_info(fields, 'P_basic')),
    case DublOrNotProps of
        [] -> List;
        Value -> throw({error, {incorrect_property_values, Value}})
    end;
validate_publish_properties(_Other) ->
    throw({error, {waiting_for_one_of,['atom none','atom  all', list]}}).


validate_parameter(Param, Fun, Value) ->
    try
        Fun(Value)
    catch
        _:{error, Err} ->
            throw({error,{invalid_parameter_value, Param, Err}})
    end.

valid_non_negative_integer(V) when is_integer(V) andalso V >= 0 ->
    V;
valid_non_negative_integer(V) ->
    throw({error, {require_non_negative_integer, V}}).

valid_binary(V) when is_binary(V) ->
    V;
valid_binary(NotBin) ->
    throw({error, {require_binary, NotBin}}).

valid_boolean(V) when is_boolean(V) ->
    V;
valid_boolean(NotBool) ->
    throw({error, {require_boolean, NotBool}}).



validate_dest_uri(V) ->
    case http_uri:parse(V) of
	{ok, _} ->
	    V;
	Error ->
	    throw(Error)
    end.

validate_allowed_value({Value, List}) ->
    case lists:member(Value, List) of
        true ->
            Value;
        false ->
            throw({error, {waiting_for_one_of,List}})
    end.

validate_src_uris(URIs)->
    Fun = fun (URI, Acc) ->
                  case amqp_uri:parse(URI) of
                      {ok, AMQPParams0} ->
                          [AMQPParams0|Acc];
                      {error, Reason, _} ->
                          throw({invalid_source_uri, {URI, Reason}})
                  end
          end,
    lists:reverse(lists:foldl(Fun, [], URIs)).
