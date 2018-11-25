%%%-------------------------------------------------------------------
%%% @author Vladislav Vorobyov <vlad@erldev>
%%% @copyright (C) 2018, Vladislav Vorobyov
%%% @doc
%%%
%%% @end
%%% Created : 23 Nov 2018 by Vladislav Vorobyov <vlad@erldev>
%%%-------------------------------------------------------------------
-module(rabbit_webshovel_connection_worker).

-behaviour(gen_server2).

%% API
-export([start_link/1]).
-export([get_connection/1]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3, format_status/2]).

-define(SERVER, ?MODULE).

-record(state, {name,
		connection}).

%%%===================================================================
%%% API
%%%===================================================================
get_connection(Pid) ->
    gen_server:call(Pid, get_connection).
%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%% @end
%%--------------------------------------------------------------------
start_link(Args) ->
    gen_server2:start_link(?MODULE, Args, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%% @end
%%--------------------------------------------------------------------
init(#{name := Name, 
       config := #{amqp_params := AMQPParams}}) ->
    process_flag(trap_exit, true),
    rand:seed(exs64, erlang:timestamp()),
    Connection = make_connection(Name, AMQPParams),
    {ok, #state{name = Name, 
		connection = Connection}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), term()}, State :: term()) ->
			 {reply, Reply :: term(), NewState :: term()} |
			 {reply, Reply :: term(), NewState :: term(), Timeout :: timeout()} |
			 {reply, Reply :: term(), NewState :: term(), hibernate} |
			 {noreply, NewState :: term()} |
			 {noreply, NewState :: term(), Timeout :: timeout()} |
			 {noreply, NewState :: term(), hibernate} |
			 {stop, Reason :: term(), Reply :: term(), NewState :: term()} |
			 {stop, Reason :: term(), NewState :: term()}.

handle_call(get_connection, _From, S=#state{}) ->
    {reply, S#state.connection, S};
handle_call(_Request, _From, State) ->
    io:format("~nUnknown call:  ~p~nState: ~p~n", [_Request, State]),
    Reply = {error, error_request},
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: term()) ->
			 {noreply, NewState :: term()} |
			 {noreply, NewState :: term(), Timeout :: timeout()} |
			 {noreply, NewState :: term(), hibernate} |
			 {stop, Reason :: term(), NewState :: term()}.
handle_cast(_Request, State) ->
    io:format("~nUnknown cast:  ~p~nState: ~p~n", [_Request, State]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%% @end
%%--------------------------------------------------------------------

%% Сообщение о завершении процесса подключения
handle_info({'EXIT', Conn, Reason}, S=#state{connection = Conn}) ->
    {stop, Reason, S};
%% Обработка прочих сообщений
handle_info(_Info, State) ->
    io:format("~nUnknown info:  ~p~nState: ~p~n", [_Info, State]),
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
-spec terminate(Reason :: normal | shutdown | {shutdown, term()} | term(),
		State :: term()) -> any().
terminate({shutdown, {server_initiated_close, _, _}}, _State)->
    ok;
terminate(_Reason, S) ->
    io:format("~nTerminate whit reson:  ~p~nState: ~p~n", [_Reason, S]),
    connection_close(S#state.connection),
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

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Функция создания подключения к брокеру
%% @end
%%--------------------------------------------------------------------
make_connection(WSName, AMQPParams)->
    AmqpParam = lists:nth(rand:uniform(length(AMQPParams)), AMQPParams),
    ConnName = get_connection_name(WSName),
    case amqp_connection:start(AmqpParam, ConnName) of
	{ok, Conn} ->
	    link(Conn),
	    Conn;
	{error, Reason} ->
	    throw({error, {connection_not_started, Reason}, WSName})
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Функция закрытия подключения
%% @end
%%--------------------------------------------------------------------
connection_close(Connection)->
    amqp_connection:close(Connection),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Функция функция формирования имени подключения на основании
%% WebShovel Name
%% @end
%%--------------------------------------------------------------------
get_connection_name(WebShovelName) when is_atom(WebShovelName) ->
    Prefix = <<"WebShovel ">>,
    WebShovelNameAsBinary = atom_to_binary(WebShovelName, utf8),
    <<Prefix/binary, WebShovelNameAsBinary/binary>>;
%% for dynamic shovels, name is a binary
get_connection_name(WebShovelName) when is_binary(WebShovelName) ->
    Prefix = <<"WebShovel ">>,
    <<Prefix/binary, WebShovelName/binary>>;
%% fallback
get_connection_name(_) ->
    <<"WebShovel">>.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Функция генерации спецификации для ConsumerSupSupervisor
%% @end
%%--------------------------------------------------------------------
%% make_consumer_supsup_spec()->
%%     ok.
