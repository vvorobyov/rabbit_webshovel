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
-export([start_link/4,
        ack_message/2,noack_message/2,
        get_channel/1, return_channel/2]).

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
                free_channels = [],
                queue,
                ack_mode,
                handle,
                publisher,
                no_ack_msg=[]}).

%%%===================================================================
%%% API
%%%===================================================================
start_link(WSName, Connection, Supervisor, Config) ->
    gen_server:start_link(?MODULE, [WSName, Connection, Supervisor, Config], []).

ack_message(Pid, DeliveryTag)->
    gen_server:cast(Pid, {ack_message, DeliveryTag}).

noack_message(Pid, DeliveryTag)->
    gen_server:cast(Pid, {noack_message, DeliveryTag}).

get_channel(Pid)->
    gen_server:call(Pid, get_channel).

return_channel(Pid, Channel) when is_pid(Channel)->
    gen_server:cast(Pid,{return_channel, Channel}).

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
    io:format("~nConsumer ~p config:~p~n",[Name, Config]), %#todo remote in release
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
     {continue, start_publisher}}.

%% Запуск publisher_sup_sup и publisher_worker
handle_continue(start_publisher, State = #state{})->
    io:format("~nContinue start_publisher_sup_sup~n"), %#todo remote in release
    start_publisher_worker(State);
%% Не известные cообщения о продолжении
handle_continue(_Request, State) ->
    io:format("~nContinue any~n"),
    {noreply, State}.

%%---------------------HANDLE CALL------------------------------------
handle_call(get_channel, From,
            State=#state{ publisher={_,From},
                          free_channels=[],
                          connection=Connection})->
    Channel =make_channel(Connection),
    {reply, Channel, State};
handle_call(get_channel, From,
            State=#state{ publisher={_,From},
                          free_channels=[Channel|Channels]}) ->
    {reply,Channel,State#state{free_channels=Channels}};
%% Не известные синхронные запросы
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
    {noreply, State}.

%%---------------------HANDLE CAST------------------------------------
%% обработка api ack_message
handle_cast({ack_message, Tag}, State=#state{consumer_ch=Channel,no_ack_msg=Messages}) ->
    case lists:member(Tag,Messages) of
        true ->
            ok = amqp_channel:cast(Channel, #'basic.ack'{delivery_tag=Tag}),
            NoAckMessages=lists:delete(Tag, Messages),
            {noreply, State#state{no_ack_msg=NoAckMessages}};
        false -> {noreply,State}
    end;
%% обработка api noack_message
handle_cast({noack_message, Tag}, State=#state{consumer_ch=Channel,no_ack_msg=Messages}) ->
    case lists:member(Tag,Messages) of
        true ->
            ok = amqp_channel:cast(Channel, #'basic.nack'{delivery_tag=Tag}),
            NoAckMessages=lists:delete(Tag, Messages),
            {noreply, State#state{no_ack_msg=NoAckMessages}};
        false -> {noreply, State}
    end;
%% обработка api return_channel
handle_cast({return_channel, Channel}, State = #state{free_channels=Channels}) ->
    case length(Channels) of
        N when N =< ? CHANNEL_LIMIT ->
            {noreply, State#state{free_channels=[Channel|Channels]}};
        _ -> amqp_channel:close(Channel),
             {noreply, State}
    end;
%% обработка не известных асинхронных вызовов
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

%% Сообщение о завершении rabbit_webshovel_publisher_worker
handle_info({'DOWN', Ref, process, _Pid, shutdown},
            State=#state{publisher={Ref, _}})->
    {noreply, State};
handle_info({'DOWN', Ref, process, _Pid, _Reason},
            State=#state{publisher = {Ref, _}, ack_mode=no_ack})->
    start_publisher_worker(State);
handle_info({'DOWN', Ref, process, _Pid, _Reason},
            State=#state{publisher = {Ref, _}, no_ack_msg=Messages})->
    lists:map(
      fun(Tag)->
              amqp_channel:cast(State#state.consumer_ch,
                                #'basic.nack'{delivery_tag=Tag})
      end, Messages),
    start_publisher_worker(State#state{no_ack_msg=[]});
%% Получено сообщение о завершении процесса канала подписки
handle_info({'EXIT', Channel, Reason},
	    State = #state{consumer_ch = Channel})->
    {stop, {close_channel, Reason}, State};
%% Получено сообщение об успешности подписки
handle_info(#'basic.consume_ok'{consumer_tag=ConsTag}, State) ->
    io:format("~nConsumed. Tag: ~p~n",[ConsTag]),
    {noreply, State#state{consumer_tag = ConsTag}};
%% Получено сообщение от брокера
handle_info(Msg = {#'basic.deliver'{consumer_tag=ConsTag,
                                    delivery_tag=DeliverTag},
                   #amqp_msg{}},
            State = #state{consumer_tag=ConsTag,
                           publisher={_,Publisher},
                           no_ack_msg=Messages}) ->
    publish_message(Publisher, Msg),
    case State#state.ack_mode of
        no_ack -> {noreply, State};
        _ -> {noreply, State#state{no_ack_msg=[DeliverTag|Messages]}}
    end;
%% Получено неизвестное сообщение
handle_info(_Info, State = #state{ws_name=WSName,
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

%%------------------------------------------------------------------
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

close_channels(#state{consumer_ch =Channel}) ->
    amqp_channel:close(Channel).


publish_message(Publisher, Message) ->
    rabbit_webshovel_publisher_worker:publish_message(Publisher,Message),
    ok.

start_publisher_worker(State=#state{parent_sup=Supervisor, handle=Handle})->
    PublishWorkSpec = {publisher_worker,
                       {rabbit_webshovel_publisher_worker,
                        start_link,
                        [Supervisor,self(),Handle#{ack_mode => State#state.ack_mode}]},
                       temporary,
                       16#ffffffff,
                       worker,
                       [rabbit_webshovel_publisher_worker]},
    {ok, Pid} = supervisor2:start_child(Supervisor,PublishWorkSpec),
    Ref = erlang:monitor(process,Pid),
    {noreply, State#state{publisher = {Ref, Pid}}}.
