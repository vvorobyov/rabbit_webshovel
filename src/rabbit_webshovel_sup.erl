%%%-------------------------------------------------------------------
%%% @author Vladislav Vorobyov <vlad@erldev>
%%% @copyright (C) 2018, Vladislav Vorobyov
%%% @doc
%%%
%%% @end
%%% Created : 19 Nov 2018 by Vladislav Vorobyov <vlad@erldev>
%%%-------------------------------------------------------------------
-module(rabbit_webshovel_sup).

-behaviour(mirrored_supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%% @end
%%--------------------------------------------------------------------
start_link() ->
    case parse_configuration(get_config()) of
	{ok, Configuration} ->
	    mirrored_supervisor:start_link({local, ?SERVER}, ?MODULE,
					   fun rabbit_misc:execute_mnesia_transaction/1,
					   ?MODULE, [Configuration]);
	Error ->
	    Error
    end.

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================
init([Configurations]) ->
    Len = maps:size(Configurations),
    SupFlags = { one_for_one, Len*2, 2},
    _StatusSpec =  #{id => rabbit_webshovel_status,
		     start => {rabbit_webshovel_status, start_link, []},
		     restart => transient,
		     shutdown => 16#ffffffff,
		     type => supervisor,
		     modules => [rabbit_webshovel_status]},
    _DynSSupSpec =  #{id => rabbit_webshovel_dyn_worker_sup_sup,
		      start => {rabbit_webshovel_dyn_worker_sup_sup, start_link, []},
		      restart => transient,
		      shutdown => 16#ffffffff,
		      type => supervisor,
		      modules => [rabbit_webshovel_dyn_worker_sup_sup]},
    ChildSpecs = make_child_specs(Configurations),
    {ok, {SupFlags, ChildSpecs}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

make_child_specs(Configurations)->
    Fun = fun(WSName, WSConfig, AccIn) ->
			ChildSpec =  { WSName,
                           {rabbit_webshovel_connection_sup, 
			    start_link, 
			    [WSConfig]},
                           permanent,
                           16#ffffffff,
                           supervisor,
                           [rabbit_webshovel_connection_sup]},
			[ChildSpec|AccIn]
		end,
	maps:fold(Fun, [], Configurations).

get_config() ->
    case catch file:consult("/home/vlad/rabbit_webshovel/include/config.erl") of
	{ok, [Value]} -> {ok,Value};
	{error, _}  -> 
	    io:format("~n!!! Error load config!!!~n"),
	    {ok,[]}
    end.
    
parse_configuration({ok, Env}) ->
    parse_configuration(Env, #{}).

parse_configuration([], Acc) ->
    {ok, Acc};
parse_configuration([{WSName, WSConfig}| Env], Acc) when is_atom(WSName) andalso is_list(WSConfig)->
    case maps:is_key(WSName, Acc) of
	true  -> {error, {duplicate_webshovel_definition, WSName}};
	false -> case validate_webshovel_config(WSName, WSConfig) of
		     {ok, WebShovel} ->
			 Acc2 = maps:put(WSName, WebShovel, Acc),
			 parse_configuration(Env, Acc2);
		     Error ->
			 Error
		 end
    end;

parse_configuration( _Other, _Acc) ->
    {error, require_list_of_webshovel_configurations}.

validate_webshovel_config(WSName, WSConfig) ->
    rabbit_webshovel_utils:parse(WSName, WSConfig).

