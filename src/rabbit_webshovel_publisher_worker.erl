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

%% API
-export([start_link/3]).
-export([publish_message/2]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         handle_continue/2,
         terminate/2, code_change/3, format_status/2]).

-define(SERVER, ?MODULE).

-record(state, {supervisor,
                consumer,
                publisher_ssup,
                endpoint_sup,
                handle,
                endpoint_msgs =#{},
                ack_mode}).

-include_lib("amqp_client/include/amqp_client.hrl").

%%%===================================================================
%%% API
%%%===================================================================
start_link(Supervisor, Consumer, Handle) ->
    gen_server:start_link(?MODULE, {Supervisor,Consumer, Handle}, []).

publish_message(Pid, Message)->
    gen_server:cast(Pid,{publish_message, Message}).
%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init({Supervisor,Consumer,Handle= #{ack_mode :=AckMode}}) ->
    process_flag(trap_exit, true),
    {ok, #state{supervisor=Supervisor,
                consumer=Consumer,
                handle=Handle,
                ack_mode=AckMode},
    {continue, start_publisher_sup_sup}}.

handle_continue(start_endpoint_sups, State)->
    start_endpoint_sups(State);
handle_continue(start_publisher_sup_sup, State) ->
    start_publisher_sup_sup(State);

handle_continue(_Request, State) ->
    {noreply,State}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({publish_message,
             Message={#'basic.deliver'{delivery_tag=DT},_}},
            State) ->
    io:format("~n rabbit_webshovel_publisher_worker ~p~nmessage: ~p~n",
              [self(),Message]),
    {ok, Pid} = supervisor:start_child(State#state.endpoint_sup,[Message]),
    Ref=erlang:monitor(process, Pid),
    EndPointMsg = State#state.endpoint_msgs,
    {noreply,State#state{endpoint_msgs=EndPointMsg#{Ref => DT}}};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    io:format("~n rabbit_webshovel_publisher_worker ~p~n"
              "terminate with reason: ~p~n", [self(),_Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, Status) ->
    Status.

%%%===================================================================
%%% Internal functions
%%%===================================================================
start_endpoint_sups(State)->
    io:format("~n rabbit_webshovel_publisher_worker~n~p~n", [State]),
    {noreply, State}.

start_publisher_sup_sup(State)->
    io:format("~p~n", [State]),
    PubSSupSpec = {publisher_super_sup,
                   {rabbit_webshovel_publisher_sup_sup, start_link, []},
                   temporary,
                   16#ffffffff,
                   supervisor,
                   [rabbit_webshovel_publisher_sup_sup]
                  },
    {ok,PubSSPid} = supervisor2:start_child(
                      State#state.supervisor, PubSSupSpec),
    Ref=erlang:monitor(process,PubSSPid),
    EndpointSupSpec = #{id => endpoint_sup,
                        start => {rabbit_webshovel_endpoint_sup,
                                 start_link, [State#state.handle]},
                        restart => permanent,
                        shutdown => 5000,
                        type => supervisor,
                        modules => [rabbit_webshovel_endpoint_sup]},
    {ok,EndpointPid} = supervisor:start_child(PubSSPid, EndpointSupSpec),
    {noreply,State#state{publisher_ssup=Ref, endpoint_sup=EndpointPid}}.
