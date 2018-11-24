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
init(Config) ->
    SupFlags = {one_for_one, 1, 5},
    ChildSpecs = make_child_specs(Config),
    {ok, {SupFlags, ChildSpecs}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
make_child_specs(#{name := WSName, supervisor :=Supervisor, config :=Config})->
    Fun = fun(DstConfig0 = #{name :=Name}, AccIn) ->
		  DstConfig = DstConfig0#{ws_name => WSName, supervisor => Supervisor},
		  ChildSpec =  { Name,
				 {rabbit_webshovel_consumer_sup, start_link, [DstConfig]},
				 permanent,
				 16#ffffffff,
				 supervisor,
				 [rabbit_webshovel_consumer_sup]},
		  [ChildSpec|AccIn]
		end,
    lists:foldl(Fun, [], Config).
