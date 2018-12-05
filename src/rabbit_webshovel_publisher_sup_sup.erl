%%%-------------------------------------------------------------------
%%% @author Vladislav Vorobyov <vlad@erldev>
%%% @copyright (C) 2018, Vladislav Vorobyov
%%% @doc
%%%
%%% @end
%%% Created : 28 Nov 2018 by Vladislav Vorobyov <vlad@erldev>
%%%-------------------------------------------------------------------
-module(rabbit_webshovel_publisher_sup_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================
start_link() ->
    supervisor:start_link(?MODULE, []).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================
-spec init(Args :: term()) ->
		  {ok, {SupFlags :: supervisor:sup_flags(),
			[ChildSpec :: supervisor:child_spec()]}} |
		  ignore.
init([]) ->
    io:format("Publisher SupSup started"),
    SupFlags = #{strategy => one_for_one,
		 intensity => 1,
		 period => 5},

    {ok, {SupFlags, []}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
