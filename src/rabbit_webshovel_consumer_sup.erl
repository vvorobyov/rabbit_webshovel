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
-spec init(Args :: term()) ->
		  {ok, {SupFlags :: supervisor:sup_flags(),
			[ChildSpec :: supervisor:child_spec()]}} |
		  ignore.
init(Args = #{ws_name := WSName, name := Name, supervisor := Supervisor}) ->
    
    SupFlags = {rest_for_one, 1, 5},
    io:format("~n===================================================~n"
	      "Connection Name ~p~n"
	      "ConsumerSup Name ~p~n"
	      "ConsumerSup Pid ~p~n"
	      "Main sup Pid ~p~n"
	      "Config ~p~n",
	      [WSName,Name, self(), Supervisor, Args]),
    %% AChild = #{id => 'AName',
    %% 	       start => {'AModule', start_link, []},
    %% 	       restart => permanent,
    %% 	       shutdown => 5000,
    %% 	       type => worker,
    %% 	       modules => ['AModule']},

    ConsumerSpec = make_consumer_spec(Args),
    {ok, {SupFlags, [ConsumerSpec]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
make_consumer_spec(ConsConfig)->
    {consumer,
     {rabbit_webshovel_consumer_worker, start_link, [ConsConfig]},
     permanent,
     5000,
     worker,
     [rabbit_webshovel_consumer_worker]}.
