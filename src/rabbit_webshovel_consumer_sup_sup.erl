%%%-------------------------------------------------------------------
%%% @author Vladislav Vorobyov <vlad@erldev>
%%% @copyright (C) 2018, Vladislav Vorobyov
%%% @doc
%%%
%%% @end
%%% Created : 23 Nov 2018 by Vladislav Vorobyov <vlad@erldev>
%%%-------------------------------------------------------------------
-module(rabbit_webshovel_consumer_sup_sup).

-behaviour(supervisor2).

%% API
-export([start_link/3]).

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
start_link(WSName, Connection, Config) ->
    supervisor:start_link(?MODULE, [WSName, Connection, Config]).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart intensity, and child
%% specifications.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
		  {ok, {SupFlags :: supervisor:sup_flags(),
			[ChildSpec :: supervisor:child_spec()]}} |
		  ignore.
init(Config) ->
    SupFlags = {one_for_one, 1, 5},
    ChildSpecs = make_child_specs(Config),
    io:format("~n~p~n",[ChildSpecs]),
    {ok, {SupFlags, ChildSpecs}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
make_child_specs([WSName, Connection, Config])->
    Fun = fun(DstConfig = #{name :=Name}, AccIn) ->
		  ChildSpec =  {Name,
				 {rabbit_webshovel_consumer_sup,
				  start_link, 
				  [WSName, Connection,DstConfig]},
				 permanent,
				 16#ffffffff,
				 supervisor,
				 [rabbit_webshovel_consumer_sup]},
		  [ChildSpec|AccIn]
		end,
    lists:foldl(Fun, [], Config).
