%%%-------------------------------------------------------------------
%%% @author Vladislav Vorobyov <vlad@erldev>
%%% @copyright (C) 2018, Vladislav Vorobyov
%%% @doc
%%%
%%% @end
%%% Created : 29 Nov 2018 by Vladislav Vorobyov <vlad@erldev>
%%%-------------------------------------------------------------------
-module(rabbit_webshovel_http_endpoint_worker).

-behaviour(gen_server).

%% API
-export([start_link/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 handle_continue/2,
	 terminate/2, code_change/3, format_status/2]).

-include_lib("amqp_client/include/amqp_client.hrl").

-include("rabbit_webshovel.hrl").

-define(SERVER, ?MODULE).

-record(state, {handle, deliver, msg, request_id}).

%%%===================================================================
%%% API
%%%===================================================================
start_link(Handle, Msg) ->
    gen_server:start_link(?MODULE, [Handle, Msg], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%% @end
%%--------------------------------------------------------------------
init([Handle,
      _Msg={Deliver = #'basic.deliver'{},
            AmqpMessage=#amqp_msg{}}]) ->
    process_flag(trap_exit, true),
    {ok, #state{handle=Handle, deliver= Deliver,
		msg=AmqpMessage},{continue, publish_message}}.


%% Handling continue message
handle_continue(publish_message, State = #state{})->
    RequestId = publish_message(State),
    {noreply, State#state{request_id = RequestId}};
handle_continue(_Request, State) ->
    {noreply,State}.

%% Handling call messages
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%% Handling cast messages
handle_cast(_Request, State) ->
    {noreply, State}.

%% Handling all non call/cast messages
handle_info(_Info, State) ->
    io:format("~nInfo ~p~n", [_Info]),
    {stop, {message_published, _Info}, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason :: normal | shutdown | {shutdown, term()} | term(),
		State :: term()) -> any().
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()},
		  State :: term(),
		  Extra :: term()) -> {ok, NewState :: term()} |
				      {error, Reason :: term()}.
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
-spec format_status(Opt :: normal | terminate,
		    Status :: list()) -> Status :: term().
format_status(_Opt, Status) ->
    Status.

%%%===================================================================
%%% Internal functions
%%%===================================================================
publish_message(State = #state{handle = Handle = #{protocol := https}}) ->
    publish_message(State#state{handle = Handle#{protocol => https}});

publish_message(State = #state{handle = #{protocol := https,
                                           method :=Method,
                                           uri := URL}})
  when (Method =:= post) orelse
       (Method =:= patch) orelse
       (Method =:= put) orelse
       (Method =:= delete) ->
    #amqp_msg{
       props = #'P_basic'{content_type=ContentType0},
       payload= Payload} = State#state.msg,
    ContentType = set_content_type(ContentType0),
    Request = {URL, [], ContentType, Payload},
    http_request(Method, Request).

http_request(Method, Request) ->
   httpc:request(Method,Request,[],[{sync, false}]).

set_content_type(undefined) ->
    ?DEFAULT_CONTENT_TYPE;
set_content_type(ContentType)  ->
    binary_to_list(ContentType).
