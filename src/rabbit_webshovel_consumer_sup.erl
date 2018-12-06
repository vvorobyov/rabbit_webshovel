%%%-------------------------------------------------------------------
%%% @author Vladislav Vorobyov <vlad@erldev>
%%% @copyright (C) 2018, Vladislav Vorobyov
%%% @doc
%%%
%%% @end
%%% Created : 23 Nov 2018 by Vladislav Vorobyov <vlad@erldev>
%%%-------------------------------------------------------------------
-module(rabbit_webshovel_consumer_sup).
-include("rabbit_webshovel.hrl").
-behaviour(supervisor).
-define(CONS_SPEC(NAME,CONNECTION, CONFIG),
        {rabbit_webshovel_consumer_worker,
         {rabbit_webshovel_consumer_worker,
          start_link, [NAME, CONNECTION, self(), CONFIG]},
         permanent,
         5000,
         worker,
         [rabbit_webshovel_consumer_worker]}).


%% API
-export([start_link/3]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

start_link(WSName, Connection, Config) ->
    supervisor:start_link(?MODULE, [WSName, Connection, Config]).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================
init([WSName, Connection, Config]) ->
    io:format("~n~p ~p ~p ~p~n",[?MODULE,self(),WSName, Config#dst.name]),
    SupFlags = {rest_for_one, 1, 5},
    ConsumerSpec = ?CONS_SPEC(WSName,Connection, Config),
    {ok, {SupFlags, [ConsumerSpec]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
