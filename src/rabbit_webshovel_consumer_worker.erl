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

-define(PUBL_SPEC(NAME,SUPERVISOR, CONFIG),
        {rabbit_webshovel_publisher_worker,
         {rabbit_webshovel_publisher_worker,
          start_link,[NAME, SUPERVISOR, self(), CONFIG]},
         temporary,
         16#ffffffff,
         worker,
         [rabbit_webshovel_publisher_worker]}).
%% API
-export([start_link/4]).
-export([ack_message/2,noack_message/2,
        get_channel/1, return_channel/2]).
-export([error/1]).
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
                supervisor,
                consumer_ch,
                revision_ref,
                consumer_tag,
                free_channels = [],
                used_channels =[],
                config,
                response,
                publisher,
                no_ack_msg=[]}).

%%%===================================================================
%%% API
%%%===================================================================
start_link(WSName, Connection, Supervisor, Config) ->
    gen_server:start_link(?MODULE,
                          [WSName, Connection, Supervisor, Config], []).

ack_message(Pid, DeliveryTag)->
    gen_server:cast(Pid, {ack_message, DeliveryTag}).

noack_message(Pid, DeliveryTag)->
    gen_server:cast(Pid, {noack_message, DeliveryTag}).

get_channel(Pid)->
    gen_server:call(Pid, get_channel).

return_channel(Pid, Channel) when is_pid(Channel)->
    gen_server:cast(Pid,{return_channel, Channel}).

error(Pid)->
    gen_server:cast(Pid, error).
%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([WSName, Connection, Supervisor, Config]) ->
    process_flag(trap_exit, true),
    ConsChannel = make_channel(Connection),
    consume(ConsChannel, Config),
    io:format("~n~p ~p ~p ~p ~p~n",[?MODULE,self(),WSName, Config#dst.name,
                                    ConsChannel]),
    TimerRef = erlang:start_timer(10000,self(),channel_revison),
    {ok, #state{ws_name = WSName,
                name = Config#dst.name,
                connection = Connection,
                supervisor = Supervisor,
                consumer_ch = ConsChannel,
                revision_ref = TimerRef,
                config = Config},
     {continue, start_publisher}}.

%% Запуск publisher_sup_sup и publisher_worker
handle_continue(start_publisher, State = #state{})->
    start_publisher_worker(State);
%% Не известные cообщения о продолжении
handle_continue(_Request, State) ->
    {noreply, State}.

%%---------------------HANDLE CALL------------------------------------
handle_call(get_channel, From,
            S=#state{ publisher={_,From}, free_channels=[],
                      used_channels = UsedCh, connection=Connection})->
    Channel =make_channel(Connection),
    {reply, Channel, S#state{used_channels=[Channel|UsedCh]}};
handle_call(get_channel, From,
            State=#state{ publisher={_,From},
                          used_channels=UsedCh,
                          free_channels=[Channel|FreeCh]}) ->
    {reply,Channel,State#state{free_channels=FreeCh,
                               used_channels=[Channel|UsedCh]}};
%% Не известные синхронные запросы
handle_call(_Request, _From, State) ->
    {noreply, State}.

%%---------------------HANDLE CAST------------------------------------
%% обработка api ack_message
handle_cast({ack_message, Tag}, State) ->
    response_broker(fun ack/2, Tag, State);
%% обработка api noack_message
handle_cast({noack_message, Tag}, State) ->
    response_broker(fun nack/2, Tag, State);
%% обработка api return_channel
handle_cast({return_channel, Channel}, S = #state{}) ->
    UsedCh = lists:delete(Channel, S#state.used_channels),
    FreeCh = [Channel|S#state.free_channels],
    {noreply, S#state{free_channels=FreeCh, used_channels=UsedCh}};
%% обработка не известных асинхронных вызовов
handle_cast(error, State) ->
    _=1/0,
    {noreply, State};
handle_cast(_Request, State) ->
    {noreply, State}.

%%---------------------HANDLE INFO-------------------------------------

%% Сообщение о завершении rabbit_webshovel_publisher_worker
handle_info({'DOWN', Ref, process, _Pid, shutdown},
            State=#state{publisher={Ref, _}})->
    {noreply, State};
handle_info({'DOWN', Ref, process, _Pid, _Reason},
            S=#state{publisher = {Ref, _},
                         config=#dst{ack_mode=no_ack}})->
    FreeCh = lists:append(S#state.free_channels, S#state.used_channels),
    start_publisher_worker(S#state{free_channels=FreeCh, used_channels=[]});
handle_info({'DOWN', Ref, process, _Pid, _Reason},
            S = #state{publisher = {Ref, _}})->
    nack(S#state.consumer_ch, S#state.no_ack_msg),
    FreeCh = lists:append(S#state.free_channels, S#state.used_channels),
    start_publisher_worker(S#state{no_ack_msg=[],
                                   free_channels=FreeCh,
                                   used_channels=[]});
%% Завершении канала подписки
handle_info({'EXIT', Ch, {shutdown, _}}, S=#state{consumer_ch=Ch})->
    {noreply, S};
handle_info({'EXIT', Ch, _Reason}, S=#state{consumer_ch=Ch})->
    ConsChannel = make_channel(S#state.connection),
    consume(ConsChannel, S#state.config),
    io:format("~n~p ~p ~p ~p ~p~n",[?MODULE,self(),S#state.ws_name,S#state.name, ConsChannel]),
    {noreply, S#state{consumer_ch=ConsChannel}};
%% Успешности подписки
handle_info(#'basic.consume_ok'{consumer_tag=ConsTag}, State) ->
    {noreply, State#state{consumer_tag = ConsTag}};
%% Получено сообщение от брокера
handle_info(Msg = {#'basic.deliver'{consumer_tag=ConsTag,
                                    delivery_tag=DeliverTag},
                   #amqp_msg{}},
            S = #state{consumer_tag=ConsTag,
                       publisher={_,Publisher},
                       no_ack_msg=Messages,
                       config = Config}) ->
    publish_message(Publisher, Msg),
    case Config#dst.ack_mode of
        no_ack -> {noreply, S};
        _ -> {noreply, S#state{no_ack_msg=[DeliverTag|Messages]}}
    end;
%% Запуск ревизии свободных каналов
handle_info({timeout, Ref, channel_revison}, S=#state{revision_ref=Ref}) ->
    TimerRef = erlang:start_timer(10000,self(),channel_revison),
    {noreply, S#state{revision_ref=TimerRef}};
%% Получено неизвестное сообщение
handle_info(_Info, State) ->
    io:format("INFO: ~n~p: ~p~n",[?MODULE, _Info]),
    {noreply, State}.

%%------------------------------------------------------------------
terminate(_Reason, State) ->
    io:format("~n============================"
              "~nConsumer close with reason"
              "~n~p~n", [_Reason]),
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

consume(Channel,#dst{queue = Queue,
                     prefetch_count = PrefCount,
                     ack_mode = AckMode})->
    amqp_channel:call(Channel, #'basic.qos'{prefetch_count = PrefCount}),
    amqp_channel:subscribe(Channel,
                           #'basic.consume'{queue = Queue,
                                            no_ack= AckMode=:=no_ack},
                           self()).

close_channels(S=#state{}) ->
    close_channel(S#state.consumer_ch),
    lists:map(fun close_channel/1, S#state.free_channels),
    lists:map(fun close_channel/1, S#state.used_channels).

close_channel(Ch)->
    catch(amqp_channel:close(Ch)).

publish_message(Publisher, Message) ->
    rabbit_webshovel_publisher_worker:publish_message(Publisher,Message),
    ok.

start_publisher_worker(S=#state{})->
    PublishWorkSpec = ?PUBL_SPEC(S#state.ws_name,
                                 S#state.supervisor,
                                 S#state.config),
    {ok, Pid} = supervisor:start_child(S#state.supervisor, PublishWorkSpec),
    Ref = erlang:monitor(process,Pid),
    {noreply, S#state{publisher = {Ref, Pid}}}.

response_broker(Response, Tag,
                S=#state{no_ack_msg=Messages, consumer_ch = Channel}) ->
    case lists:member(Tag,Messages) of
        true ->
            Response(Channel,[Tag]),
            NoAckMessages=lists:delete(Tag, Messages),
            {noreply, S#state{no_ack_msg=NoAckMessages}};
        false -> {noreply,S#state{}}
    end.

nack(_Ch, [])->
    ok;
nack(Ch, [Tag|Rest]) ->
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag=Tag}),
    nack(Ch, Rest).

ack(_Ch, [])->
    ok;
ack(Ch, [Tag|Rest]) ->
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag=Tag}),
    ack(Ch, Rest).
