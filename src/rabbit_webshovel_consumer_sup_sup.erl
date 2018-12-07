%%%-------------------------------------------------------------------
%%% @author Vladislav Vorobyov <vlad@erldev>
%%% @copyright (C) 2018, Vladislav Vorobyov
%%% @doc
%%%
%%% @end
%%% Created : 23 Nov 2018 by Vladislav Vorobyov <vlad@erldev>
%%%-------------------------------------------------------------------
-module(rabbit_webshovel_consumer_sup_sup).

-behaviour(supervisor).

-include("rabbit_webshovel.hrl").
-define(CHILD_SPEC(NAME,CONNECTION,CONFIG),
        {CONFIG#dst.name,
         {rabbit_webshovel_consumer_sup, start_link,
          [NAME,CONNECTION, CONFIG]},
         permanent,
         16#ffffffff,
         supervisor,
         [rabbit_webshovel_consumer_sup]}).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================
start_link(Args) ->
    supervisor:start_link(?MODULE, Args).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================
init(Args={WSName,_,_,_}) ->
    io:format("~n~p ~p ~p ~p~n",[?MODULE,self(),WSName,'_']),
    SupFlags = {one_for_one, 1, 5},
    ChildSpecs = make_child_specs(Args),
    {ok, {SupFlags, ChildSpecs}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
make_child_specs({WSName, Connection, SrcProtocol, Config})->
    Fun = fun(DstConfig, AccIn) ->
                  ChildSpec =?CHILD_SPEC(WSName, Connection,
                                         DstConfig#dst{
                                           src_protocol=SrcProtocol}),
                  [ChildSpec|AccIn]
          end,
    lists:foldl(Fun, [], Config).
