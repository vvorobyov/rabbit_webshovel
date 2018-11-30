%%%-------------------------------------------------------------------
%%% @author Vladislav Vorobyov <vlad@erldev>
%%% @copyright (C) 2018, Vladislav Vorobyov
%%% @doc
%%%
%%% @end
%%% Created : 29 Nov 2018 by Vladislav Vorobyov <vlad@erldev>
%%%-------------------------------------------------------------------
-module(rabbit_webshovel_http_endpoint_sup).

-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================
start_link(Handle) ->
    supervisor:start_link(?MODULE, Handle).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init(Handle) ->

    SupFlags = #{strategy => simple_one_for_one,
		 intensity => 1,
		 period => 5},

    AChild = #{id => rabbit_webshovel_http_endpoint_worker,
	       start => {rabbit_webshovel_http_endpoint_worker,
			 start_link, [Handle]},
	       restart => temporary,
	       shutdown => 5000,
	       type => worker,
	       modules => [rabbit_webshovel_http_endpoint_worker]},

    {ok, {SupFlags, [AChild]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
