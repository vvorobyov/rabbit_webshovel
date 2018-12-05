%%%-------------------------------------------------------------------
%%% @author Vladislav Vorobyov <vlad@erldev>
%%% @copyright (C) 2018, Vladislav Vorobyov
%%% @doc
%%%
%%% @end
%%% Created : 23 Nov 2018 by Vladislav Vorobyov <vlad@erldev>
%%%-------------------------------------------------------------------
-module(rabbit_webshovel_consumer_sup).

-behaviour(supervisor2).

%% API
-export([start_link/3]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

start_link(WSName, Connection, Config) ->
    supervisor2:start_link(?MODULE, [WSName, Connection, Config]).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================
init([WSName, Connection, Config = #{name := Name}]) ->
    SupFlags = {rest_for_one, 1, 5},
    ConsumerSpec = {Name,
		    {rabbit_webshovel_consumer_worker,
		     start_link,
		     [WSName, Connection,self(),Config]},
		    permanent,
		    5000,
		    worker,
		    [rabbit_webshovel_consumer_worker]},

    {ok, {SupFlags, [ConsumerSpec]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
