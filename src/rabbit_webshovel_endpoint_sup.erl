%%%-------------------------------------------------------------------
%%% @author Vladislav Vorobyov <vlad@erldev>
%%% @copyright (C) 2018, Vladislav Vorobyov
%%% @doc
%%%
%%% @end
%%% Created : 29 Nov 2018 by Vladislav Vorobyov <vlad@erldev>
%%%-------------------------------------------------------------------
-module(rabbit_webshovel_endpoint_sup).

-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).
-define(HTTP_SPEC(ATRS), #{id => rabbit_webshovel_http_endpoint_worker,
                           start => {rabbit_webshovel_http_endpoint_worker,
                                     start_link, [ATRS]},
                           restart => temporary,
                           shutdown => 5000,
                           type => worker,
                           modules => [rabbit_webshovel_http_endpoint_worker]}).
-define(SOAP_SPEC(ATRS), #{id => rabbit_webshovel_soap_endpoint_worker,
                           start => {rabbit_webshovel_soap_endpoint_worker,
                                     start_link, [ATRS]},
                           restart => temporary,
                           shutdown => 5000,
                           type => worker,
                           modules => [rabbit_webshovel_soap_endpoint_worker]}).
%%%===================================================================
%%% API functions
%%%===================================================================
start_link(Handle) ->
    supervisor:start_link(?MODULE, Handle).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init(Handle=#{protocol := Protocol}) ->
    SupFlags = #{strategy => simple_one_for_one,
		 intensity => 1,
		 period => 5},
    ChildSpec = case Protocol  of
                    http  -> ?HTTP_SPEC(Handle);
                    https -> ?HTTP_SPEC(Handle);
                    soap  -> ?SOAP_SPEC(Handle)
                end,
    {ok, {SupFlags, [ChildSpec]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
