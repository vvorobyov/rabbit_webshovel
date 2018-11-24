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
    supervisor:start_link(?MODULE, Args).

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
init(_Args = #{name := Name, connection := Connection, config := Config}) ->
    io:format("~n==================================================~n"
    	      "WebShovel Name : ~p~n"
    	      "Cunsumer sup_sup PID: ~p~n"
    	      "Connection PID ~p~n"
    	      "Config ~p~n"
    	      "~n==================================================~n",
    	      [Name,self(), Connection, Config]),
    SupFlags = {one_for_one, 1, 5},

    %% AChild = #{id => 'AName',
    %% 	       start => {'AModule', start_link, []},
    %% 	       restart => permanent,
    %% 	       shutdown => 5000,
    %% 	       type => worker,
    %% 	       modules => ['AModule']},

    {ok, {SupFlags, []}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
