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
    validate(Source0),
    Source1 = parse_source(Source0),
    {destinations, Dest0} = proplists:lookup(destinations, WSConfig),
    validate(Dest0),
    Dest = parse_destinations(Dest0),
    Source = Source1#{destinations => Dest},
    {ok,  #{name => WSName,
            config => Source}}.

parse_source(SrcConfig) ->
    Protocol = proplists:get_value(protocol, SrcConfig, amqp091),
    {uris, URIs} = proplists:lookup(uris, SrcConfig),
    Fun = fun (URI, Acc) ->
            case amqp_uri:parse(URI) of
                {ok, AMQPParams0} -> 
		    [AMQPParams0|Acc];
                {error, Reason, _} -> 
		    throw({invalid_source_uri, {URI, Reason}})
            end
          end,
    AMQPParams = lists:foldl(Fun, [], URIs),
    ReconnectDelay = validate_parameter(
		       reconnect_delay,
		       fun valid_non_negative_integer/1,
		       proplists:get_value(reconnect_delay,
					   SrcConfig,
					   ?DEFAULT_RECONNECT_DELAY)),
    #{protocol => Protocol,
      amqp_params => AMQPParams,
      reconnect_delay => ReconnectDelay}.

parse_destinations(DestConfig) ->
    Fun = fun({Name,Config}, Acc)->
		  validate(Config),
		  [parse_destination(Name, Config)|Acc]
	  end, 
    lists:foldl(Fun, [], DestConfig).


parse_destination(Name, Config)->
    {queue,Queue0} = proplists:lookup(queue, Config),
    Queue = validate_parameter(queue, 
			       fun valid_binary/1, 
			       Queue0),
    PrefetchCount = validate_parameter(
		      prefetch_count,
		      fun valid_non_negative_integer/1,
		      proplists:get_value(prefetch_count, 
					  Config, 
					  ?DEFAULT_PREFETCH_COUNT)),
    #{name => Name, 
      config => #{queue => Queue, 
		 prefetch_count => PrefetchCount}}.


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
valid_binary(V) ->
    throw({error, {require_binary, V}}).
