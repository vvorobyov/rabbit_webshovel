%%%-------------------------------------------------------------------
%%% @author Vladislav Vorobyov <vlad@erldev>
%%% @copyright (C) 2018, Vladislav Vorobyov
%%% @doc
%%%
%%% @end
%%% Created : 23 Nov 2018 by Vladislav Vorobyov <vlad@erldev>
%%%-------------------------------------------------------------------
-module(rabbit_webshovel_connection_sup).

-behaviour(supervisor2).

-include("rabbit_webshovel.hrl").

-define(CONN_SPEC(CONFIG),
        {{connection, CONFIG#webshovel.name},
         {rabbit_webshovel_connection_worker, start_link, [self(), CONFIG]},
         case CONFIG#webshovel.source#src.reconnect_delay of
             N when is_integer(N) andalso N > 0 ->
                 {permanent, N};
             _ -> temporary
         end,
         5000,
         worker,
         [rabbit_webshovel_connection_worker]}).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).


%%%===================================================================
%%% API functions
%%%===================================================================
start_link(Args) ->
    supervisor2:start_link(?MODULE, Args).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================
init(Config) ->
    io:format("~n~p ~p ~p ~p~n",[?MODULE,self(),Config#webshovel.name,'_']),
    SupFlags = {rest_for_one, 5, 5},
    ConnectionSpec =?CONN_SPEC(Config),
    {ok, {SupFlags, [ConnectionSpec]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
