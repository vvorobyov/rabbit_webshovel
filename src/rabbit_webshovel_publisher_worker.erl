%%%-------------------------------------------------------------------
%%% @author Vladislav Vorobyov <vlad@erldev>
%%% @copyright (C) 2018, Vladislav Vorobyov
%%% @doc
%%%
%%% @end
%%% Created :  4 Dec 2018 by Vladislav Vorobyov <vlad@erldev>
%%%-------------------------------------------------------------------
-module(rabbit_webshovel_publisher_worker).

-behaviour(gen_server).

-include("rabbit_webshovel.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-define(SUP_SPEC(NAME, SPEC),
        {NAME,
         {rabbit_webshovel_publisher_sup, start_link, [SPEC]},
         temporary,
         16#ffffffff,
         supervisor,
         [rabbit_webshovel_publisher_sup]}).

-define(HTTP_SPEC(ATRS),
        {rabbit_webshovel_http_endpoint_worker,
         {rabbit_webshovel_http_endpoint_worker,
          start_link, [{self(), ATRS}]},
         temporary,
         60000,
         worker,
         [rabbit_webshovel_http_endpoint_worker]}).

-define(SOAP_SPEC(ATRS),
        {rabbit_webshovel_soap_endpoint_worker,
         {rabbit_webshovel_soap_endpoint_worker,
          start_link, [{self(), ATRS}]},
         temporary,
         60000,
         worker,
         [rabbit_webshovel_soap_endpoint_worker]}).

-define(AMQP091_SPEC(ATRS),
        {rabbit_webshovel_amqp091_worker,
         {rabbit_webshovel_amqp091_worker,
          start_link, [{self(), ATRS}]},
         temporary,
         60000,
         worker,
         [rabbit_webshovel_amqp091_worker]}).
-define(AMQP10_SPEC(ATRS),
        {rabbit_webshovel_amqp10_worker,
         {rabbit_webshovel_amqp10_worker,
          start_link, [{self(), ATRS}]},
         temporary,
         60000,
         worker,
         [rabbit_webshovel_amqp10_worker]}).

%% API
-export([start_link/4]).
-export([publish_message/2]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         handle_continue/2,
         terminate/2, code_change/3, format_status/2]).

-define(SERVER, ?MODULE).

-record(state, {ws_name,
                name,
                supervisor,
                consumer,
                response_sup,
                endpoint_sup,
                config,
                endpoint_msgs =#{}}).

%%%===================================================================
%%% API
%%%===================================================================
start_link(WSName, Supervisor, Consumer, Config) ->
    gen_server:start_link(?MODULE, {WSName, Supervisor,Consumer, Config}, []).

publish_message(Pid, Message)->
    gen_server:cast(Pid,{publish_message, Message}).
%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init({WSName, Supervisor, Consumer, Config}) ->
    io:format("~n~p ~p ~p ~p~n",[?MODULE,self(),WSName, Config#dst.name]),
    process_flag(trap_exit, true),
    {ok, #state{ws_name = WSName,
                name = Config#dst.name,
                supervisor=Supervisor,
                consumer=Consumer,
                config=Config},
    {continue, start_publisher_sups}}.

handle_continue(start_publisher_sups, State)->
    start_publisher_sups(State);
%% handle_continue(start_publisher_sup_sup, State) ->
%%     start_publisher_sup_sup(State);
handle_continue(_Request, State) ->
    {noreply,State}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({publish_message,
             Message={#'basic.deliver'{delivery_tag=_DT},_}},
            State) ->
    io:format("~n rabbit_webshovel_publisher_worker ~p~nmessage: ~p~n",
              [self(),Message]),
    %% {ok, Pid} = supervisor:start_child(State#state.endpoint_sup,[Message]),
    %% Ref=erlang:monitor(process, Pid),
    %% EndPointMsg = State#state.endpoint_msgs,
    %% {noreply,State#state{endpoint_msgs=EndPointMsg#{Ref => DT}}};
    {noreply,State};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, Status) ->
    Status.

%%%===================================================================
%%% Internal functions
%%%===================================================================
start_publisher_sups(State0=#state{})->
    State1 = start_response_sup(State0),
    State = start_endpoint_sup(State1),
    io:format("~n rabbit_webshovel_publisher_worker~n~p~n", [State]),
    {noreply, State}.

start_response_sup(S=#state{config = #dst{response = undefined}}) ->
    S#state{};
start_response_sup(S=#state{ws_name =WSName, name = Name,
                            supervisor=Sup,
                            config = #dst{src_protocol= Protocol,
                                          response = Response}})->
    Spec = case Protocol of
               amqp091 -> ?AMQP091_SPEC({WSName,Name,Response});
               amqp10  -> ?AMQP10_SPEC({WSName, Name,Response})
           end,
    {ok, Pid} = supervisor:start_child( Sup, ?SUP_SPEC(response_sup, Spec)),
    erlang:monitor(process, Pid),
    S#state{response_sup=Pid}.

start_endpoint_sup(S=#state{ws_name =WSName, name = Name,
                            config=#dst{endpoint=Config},
                           supervisor=Sup})->
    Spec = case Config#endpoint.protocol of
               http -> ?HTTP_SPEC({WSName, Name,Config});
               https -> ?HTTP_SPEC({WSName, Name,Config});
               soap -> ?SOAP_SPEC({WSName, Name,Config})
           end,
    {ok, Pid} = supervisor:start_child( Sup, ?SUP_SPEC(endpoint_sup, Spec)),
    erlang:monitor(process,Pid),
    S#state{endpoint_sup=Pid}.
