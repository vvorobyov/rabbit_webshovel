%%%-------------------------------------------------------------------
%%% @author Vladislav Vorobyov <vlad@erldev>
%%% @copyright (C) 2018, Vladislav Vorobyov
%%% @doc
%%%
%%% @end
%%% Created : 23 Nov 2018 by Vladislav Vorobyov <vlad@erldev>
%%%-------------------------------------------------------------------
-module(rabbit_webshovel_consumer_worker).

-behaviour(gen_server).

%% API
-export([start_link/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 handle_continue/2, terminate/2, code_change/3,
	 format_status/2]).

-define(SERVER, ?MODULE).

-include("rabbit_webshovel.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-record(state, {ws_name,
		name,
		connection,
		parent_sup,
		consumer_ch,
		consumer_tag,
		free_chanels = [],
		queue,
		ack_mode,
		handle,
		publisher_sup,
		endpoint_sup,
		endpoint_msgs =#{}}).
%%%===================================================================
%%% API
%%%===================================================================
start_link(WSName, Connection, Supervisor, Config) ->
    gen_server:start_link(?MODULE, [WSName, Connection, Supervisor, Config], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([WSName, Connection, Supervisor,
     Config =  #{name:=Name, 
		 config := #{queue := Queue,
			     prefetch_count := PrefCount,
			     ack_mode := AckMode,
			     handle := Handle}}]) ->
    process_flag(trap_exit, true),
    io:format("`nConsumer ~p config:~p~n",[Name, Config]), %#todo remote in release
    ConsChannel = make_channel(Connection),
    consume(ConsChannel, Queue, PrefCount, AckMode),
    {ok, #state{ws_name = WSName, 
		name = Name,
		connection = Connection,
		parent_sup = Supervisor,
		consumer_ch = ConsChannel,
		queue = Queue,
		ack_mode = AckMode,
		handle = Handle},
     {continue, start_publisher_sup_sup}}.

%% Запуск publisher_sup_sup и подчиненные супервизоры
handle_continue(start_publisher_sup_sup,
		State=#state{parent_sup = Sup,
			     handle = Handle = #{protocol := Protocol}})->
    io:format("~nContinue start_publisher_sup_sup~n"), %#todo remote in release
    {PRef, PPid} = start_publisher_sup_sup(Sup),
    WebEndpointPid = start_endpoint_sup(Protocol, PPid, Handle),
    
    {noreply, State#state{publisher_sup = {PRef, PPid},
			  endpoint_sup = WebEndpointPid}};
%% Не известные сообщения о продолжении
handle_continue(_Request, State) ->
    io:format("~nContinue any~n"),
    {noreply, State}.

%%---------------------HANDLE CALL------------------------------------
handle_call(_Request, _From, State) ->
    io:format("~n====================================================~n"
	      "Module: ~p~n"
	      "Pid:~p~n"
	      "WebShovel Name: ~p~n"
	      "Dest Name: ~p~n"
	      "State: ~p~n"
	      "----------------------------------------------------~n"
	      "~nUnknown call: ~p~n"
	      "====================================================~n",
	      [?MODULE, self(),
	       State#state.ws_name,
	       State#state.name,
	       State, _Request]),
    Reply = ok,
    {reply, Reply, State}.

%%---------------------HANDLE CAST------------------------------------
handle_cast(_Request, State) ->
    io:format("~n====================================================~n"
	      "Module: ~p~n"
	      "Pid:~p~n"
	      "WebShovel Name: ~p~n"
	      "Dest Name: ~p~n"
	      "State: ~p~n"
	      "----------------------------------------------------~n"
	      "~nUnknown cast: ~p~n"
	      "====================================================~n",
	      [?MODULE, self(),
	       State#state.ws_name,
	       State#state.name,
	       State, _Request]),
    {noreply, State}.

%%---------------------HANDLE INFO-------------------------------------

%% Сообщение о завершении rabbit_webshovel_publisher_sup_sup
handle_info({'DOWN', Ref, process, _Pid, shutdown},
	    State=#state{publisher_sup={Ref, _}})->
    {noreply, State};
handle_info({'DOWN', Ref, process, _Pid, _Reason},
	    State=#state{publisher_sup = {Ref, _}})->
    {noreply, State, {continue, start_publisher_supervisor_sup_sup}};

%% Получено сообщение о завершении процесса канала подписки
handle_info({'EXIT', Channel, Reason},
	    State = #state{consumer_ch = Channel})->
    {stop, {close_channel, Reason}, State};
%% Получено сообщение об успешности подписки
handle_info(#'basic.consume_ok'{consumer_tag=ConsTag}, State) ->
    io:format("~nConsumed. Tag: ~p~n",[ConsTag]),
    {noreply, State#state{consumer_tag = ConsTag}};
%% Получено сообщение от брокера
handle_info(Msg = {#'basic.deliver'{consumer_tag=ConsTag},
		   #amqp_msg{payload=Payload}}, 
	    State = #state{consumer_tag=ConsTag,
			   endpoint_sup=EndPoint}) ->
    io:format("~n===================================================~n"
	      "Receive message: ~p~n"
	      "All msg: ~p~n"
	      "===================================================~n",
	      [Payload, Msg]),
    Ref = publish_message(EndPoint,Msg),
    EndPointMsgs = State#state.endpoint_msgs,
    {noreply,
     State#state{
       endpoint_msgs = EndPointMsgs#{Ref => Msg}}};
%% Обработка сообщений от endpoint
handle_info({'DOWN', Ref, process, _Pid, {message_published,
					  Response}},
	    State = #state{}) ->
    case catch(maps:get(Ref, State#state.endpoint_msgs)) of
	{error, _} ->
	    io:format("~nUndefined endpoint process:~p~nMessage:~p~n",
		      [_Pid, Response]),
	    {noreply, State};
	Value ->
	    io:format("~nMsg: ~p~nResponse: ~p~n", [Value, Response]),
	    {noreply, State}
    end;
handle_info(_Info,
	    State = #state{ws_name=WSName,
			   name=Name}) ->
    io:format("~n====================================================~n"
	      "Module: ~p~n"
	      "Pid:~p~n"
	      "WebShovel Name: ~p~n"
	      "Dest Name: ~p~n"
	      "State: ~p~n"
	      "----------------------------------------------------~n"
	      "~nUnknown info: ~p~n"
	      "====================================================~n",
	      [?MODULE, self(), WSName, Name, State, _Info]),
    {noreply, State}.

%%--------------------------------------------------------------------
terminate({close_channel, _Reason}, _State)->
    ok;
terminate(Reason, State) 
  when Reason =:= shutdown; Reason =:= killed ->
    close_channels(State),
    ok;
terminate(_Reason, 
	    State = #state{ws_name=WSName,
			   name=Name}) ->
    io:format("~n====================================================~n"
	      "Module: ~p~n"
	      "Pid:~p~n"
	      "WebShovel Name: ~p~n"
	      "Dest Name: ~p~n"
	      "State: ~p~n"
	      "----------------------------------------------------~n"
	      "~nUnknown terminate reason: ~p~n"
	      "====================================================~n",
	      [?MODULE, self(), WSName, Name, State, _Reason]),
    close_channels(State),
    ok.

%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
format_status(_Opt, Status) ->
    Status.

%%%===================================================================
%%% Internal functions
%%%===================================================================

make_channel(Connection)->
    {ok, Ch} = amqp_connection:open_channel(Connection),
    link(Ch),
    Ch.

consume(Channel,Queue, PrefCount, AckMode)->
    amqp_channel:call(Channel,
		      #'basic.qos'{prefetch_count = PrefCount}),
    amqp_channel:subscribe(Channel, 
    			   #'basic.consume'{queue = Queue, 
					    no_ack= AckMode=:=no_ack},
			   self()).
    
close_channels(#{consume_channel :=Channel}) ->
    amqp_channel:close(Channel).

start_publisher_sup_sup(Supervisor)->
    PubSSupSpec = {publisher,
		   {rabbit_webshovel_publisher_sup_sup,
		    start_link,[]},
		   temporary,
		   16#ffffffff,
		   supervisor,
		   [rabbit_webshovel_publisher_sup_sup]},
    {ok, PPid} = supervisor2:start_child(Supervisor,PubSSupSpec),
    PRef = erlang:monitor(process, PPid),
    {PRef, PPid}.

start_endpoint_sup(https, Supervisor, Handle) ->
    start_endpoint_sup(http, Supervisor, Handle);
start_endpoint_sup(http, Supervisor, Handle) ->
    EndPointSupSpec = {http_endpoint,
		       {rabbit_webshovel_http_endpoint_sup, start_link,
			[Handle]},
		       permanent,
		       16#ffffffff,
		       supervisor,
		       [rabbit_webshovel_http_endpoint_sup]},
    {ok, Pid} = supervisor:start_child(Supervisor, EndPointSupSpec),
    Pid.

publish_message(Supervisor, Message) ->
    {ok, Pid} = supervisor:start_child(Supervisor, [Message]),
    Ref = erlang:monitor(process, Pid),
    Ref.

