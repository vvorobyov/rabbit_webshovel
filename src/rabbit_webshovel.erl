%%%-------------------------------------------------------------------
%% @doc rabbit_webshovel public API
%% @end
%%%-------------------------------------------------------------------

-module(rabbit_webshovel).

-behaviour(application).

%% Application callbacks
-export([start/0, start/2, stop/0, stop/1]).

%%====================================================================
%% API
%%====================================================================
start()->
    rabbit_webshovel_sup:start_link(),
    ok.

start(normal, []) ->
    rabbit_webshovel_sup:start_link().

%%--------------------------------------------------------------------
stop() -> ok.
stop(_State) -> 
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
