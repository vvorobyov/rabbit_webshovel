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
%% -record(state, {ws_name, name, connection, consume_channel}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%% @end
%%--------------------------------------------------------------------
%% -spec start_link(Args :: map()) -> {ok, Pid :: pid()} |
%% 		      {error, Error :: {already_started, pid()}} |
%% 		      {error, Error :: term()} |
%% 		      ignore.
start_link(WSName, Connection, Supervisor, Config) ->
    gen_server:start_link(?MODULE, [WSName, Connection, Supervisor, Config], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%% @end
%%--------------------------------------------------------------------
%% -spec init(Args :: term()) -> {ok, State :: term()} |
%% 			      {ok, State :: term(), Timeout :: timeout()} |
%% 			      {ok, State :: term(), hibernate} |
%% 			      {stop, Reason :: term()} |
%% 			      ignore.
init([WSName, Connection, Supervisor,
     Config =  #{name:=Name, 
	config := #{queue := Queue,
		    prefetch_count := PrefCount,
		    ack_mode := AckMode,
		    handle := Handle}}]) ->
    process_flag(trap_exit, true),
    io:format("`nConsumer ~p config:~p~n",[Name, Config]),
    ConsChannel = make_channel(Connection),
    PublishChannel = make_channel(Connection),
    consume(ConsChannel, Queue, PrefCount, AckMode),
    {ok, #{ws_name => WSName, 
	   name => Name,
	   supervisor => Supervisor,
	   connection => Connection,
	   consume_channel => ConsChannel,
	   publish_channel => PublishChannel,
	   queue => Queue,
	   prefetch_count => PrefCount,
	   ack_mode => AckMode,
	   handle => Handle},{continue, start_publisher_sup_sup}}.


handle_continue(start_publisher_sup_sup, State=#{supervisor := Sup})->
    io:format("~nContinue start_publisher_sup_sup~n"),
    PubSSupSpec = {publisher,
		   {rabbit_webshovel_publisher_sup_sup,
		    start_link,[]},
		   temporary,
		   16#ffffffff,
		   supervisor,
		   [rabbit_webshovel_publisher_sup_sup]},
    {ok, PPid} = supervisor2:start_child(Sup,PubSSupSpec),
    PRef = erlang:monitor(process, PPid),
    {noreply, State#{publisher_ss_fer => PRef,
		     publisher_ss_pid => PPid},{continue,
						start_publishers_sup}};
handle_continue(start_publishers_sup,
		State=#{publisher_ss_pid := _Sup}) ->
    io:format("~nContinue start_publishers_sup~n"),

    {noreply, State};
handle_continue(_Request, State) ->
    io:format("~nContinue any~n"),
    {noreply, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, 
	    State = #{ws_name := WSName,
		      name := Name}) ->
    io:format("~n====================================================~n"
	      "Module: ~p~n"
	      "Pid:~p~n"
	      "WebShovel Name: ~p~n"
	      "Dest Name: ~p~n"
	      "State: ~p~n"
	      "----------------------------------------------------~n"
	      "~nUnknown call: ~p~n"
	      "====================================================~n",
	      [?MODULE, self(), WSName, Name, State, _Request]),
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%% @end
%%--------------------------------------------------------------------
handle_cast(_Request,
	    State = #{ws_name := WSName,
		      name := Name}) ->
    io:format("~n====================================================~n"
	      "Module: ~p~n"
	      "Pid:~p~n"
	      "WebShovel Name: ~p~n"
	      "Dest Name: ~p~n"
	      "State: ~p~n"
	      "----------------------------------------------------~n"
	      "~nUnknown cast: ~p~n"
	      "====================================================~n",
	      [?MODULE, self(), WSName, Name, State, _Request]),
    {noreply, State}.

%handle_info
%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%% @end
%%--------------------------------------------------------------------

%% Сообщение о завершении процесса супервизора consumers
handle_info({'DOWN', Ref, process, _Pid, shutdown},
	    State=#{publiser_ss_ref := Ref})->
    {noreply, State};
handle_info({'DOWN', Ref, process, _Pid, _Reason},
	    State=#{publisher_ss_ref := Ref})->
    {noreply, State, {continue, start_publisher_supervisor_sup_sup}};

%% Получено сообщение о завершении процесса канала подписки
handle_info({'EXIT', Channel, Reason}, #{consume_channel := Channel})->
    {stop, {close_channel, Reason}};
%% Получено сообщение об успешности подписки
handle_info(#'basic.consume_ok'{consumer_tag=ConsTag}, State) ->
    io:format("~nConsumed. Tag: ~p~n",[ConsTag]),
    {noreply, State#{consumer_tag => ConsTag}};
%% Получено сообщение от брокера
handle_info(Msg = {#'basic.deliver'{consumer_tag = ConsTag},
		   #amqp_msg{payload = Payload}}, 
	    State = #{ws_name :=WSName, 
		      name :=Name, 
		      consumer_tag := ConsTag,
		      handle := Handle
		     }) ->
    io:format("~n===================================================~n"
	      "WebShovel Name: ~p~n"
	      "Dest Name: ~p~n"
	      "---------------------------------------------------~n"
	      "Receive message: ~p~n"
	      "All msg: ~p~n"
	      "===================================================~n",
	      [WSName, Name, Payload, Msg]),
    publish(Handle, Msg),
    {noreply, State};
handle_info(_Info,
	    State = #{ws_name := WSName,
		      name := Name}) ->
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
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%% @end
%%--------------------------------------------------------------------
terminate({close_channel, _Reason}, _State)->
    ok;
terminate(Reason, State) 
  when Reason =:= shutdown; Reason =:= killed ->
    close_channels(State),
    ok;
terminate(_Reason, 
	    State = #{ws_name := WSName,
		      name := Name}) ->
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
%% @private
%% @doc
%% Convert process state when code is changed
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called for changing the form and appearance
%% of gen_server status when it is returned from sys:get_status/1,2
%% or when it appears in termination error logs.
%% @end
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

%start_http_endpoint_sup(Supervisor)->
%    ok.


publish([],[])->
    ok;
publish(Handler = #{protocol := http}, Message) ->
    publish(Handler = #{protocol => https}, Message);
publish(_Handler = #{protocol := https, method :=Method, uri := URL},
	_Message = {_,#amqp_msg{props = #'P_basic'{
				       content_type = ContentType0
				      },
			    payload = Payload}})
  when (Method =:= post) orelse
       (Method =:= patch) orelse
       (Method =:= put) orelse
       (Method =:= delete) ->
    ContentType = set_content_type(ContentType0),
    Request = {URL, [], ContentType, Payload},
    http_request(Method, Request).
	    
http_request(Method, Request) ->
   httpc:request(Method,Request,[],[{sync, false}]).

set_content_type(undefined) ->
    ?DEFAULT_CONTENT_TYPE;
set_content_type(ContentType)  ->
    binary_to_list(ContentType).
