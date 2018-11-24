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

%% API
-export([start_link/1]).

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
-spec start_link(Args :: map()) -> {ok, Pid :: pid()} |
		      {error, {already_started, Pid :: pid()}} |
		      {error, {shutdown, term()}} |
		      {error, term()} |
		      ignore.
start_link(Args) ->
    supervisor2:start_link(?MODULE, Args).

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
-spec init(Args :: map()) ->
		  {ok, {SupFlags :: supervisor:sup_flags(),
			[ChildSpec :: supervisor:child_spec()]}} |
		  ignore.
init(Config0 = #{name := Name, 
		 config := Config}) ->
    SupFlags = {rest_for_one, 1, 5},
    WSConfig = Config0#{supervisor => self()},
    Worker = {Name,
    	      {rabbit_webshovel_connection_worker, start_link, [WSConfig]},
	      case Config of
		  #{reconnect_delay := N}
		    when is_integer(N) andalso N > 0 ->
		      {permanent, N};
		  _ -> 
		      permanent
	      end,
	      5000,
	      worker,
	      [rabbit_webshovel_connection_worker]},
    {ok, {SupFlags, [Worker]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
