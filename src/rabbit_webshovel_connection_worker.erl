%%%-------------------------------------------------------------------
%%% @author Vladislav Vorobyov <vlad@erldev>
%%% @copyright (C) 2018, Vladislav Vorobyov
%%% @doc
%%%
%%% @end
%%% Created : 23 Nov 2018 by Vladislav Vorobyov <vlad@erldev>
%%%-------------------------------------------------------------------
-module(rabbit_webshovel_connection_worker).

-behaviour(gen_server).

%% API
-export([start_link/2]).
-export([get_connection/1]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 handle_continue/2, terminate/2, code_change/3, format_status/2]).

-define(SERVER, ?MODULE).

%% -record(state, {name = unknown,
%% 		supervisor = unknown,
%% 		cons_ref = unknown,
%% 		cons_config = unknown,
%% 		connection = unknown}).

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
start_link(Supervisor, Config) ->
    gen_server:start_link(?MODULE, [Supervisor, Config], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%% @end
%%--------------------------------------------------------------------
init([Supervisor,
     #{name := Name, 
       source := #{amqp_params := AMQPParams},
       destinations:=DestConfig}]) ->
    io:format("~n!!! Init connection: ~p. PID:~p !!!~n",[Name, self()]),
    process_flag(trap_exit, true),
    rand:seed(exs64, erlang:timestamp()),
    Connection = make_connection(Name, AMQPParams),
    io:format("~n Connection PID: ~p~n",[Connection]),
    {ok, #{name => Name, 
	   supervisor => Supervisor,
	   connection => Connection,
	   cons_config => DestConfig},
     {continue, start_consumers_sup_sup}}.    


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%% @end
%%--------------------------------------------------------------------
handle_call(get_connection, _From, State=#{connection := Connection}) ->
    {reply, Connection, State};
handle_call(_Request, _From, State=#{name := Name}) ->
    io:format("~n====================================================~n"
	      "Module: ~p~n"
	      "Pid:~p~n"
	      "WebShovel Name: ~p~n"
	      "State: ~p~n"
	      "----------------------------------------------------~n"
	      "~nUnknown call: ~p~n"
	      "====================================================~n",
	      [?MODULE, self(), Name, State, _Request]),
    Reply = {error, error_request},
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling continue
%% @end
%%--------------------------------------------------------------------
handle_continue(start_consumers_sup_sup, State)->
    Ref = start_consumers_sup_sup(State),
    {noreply, State#{cons_ref => Ref}};
handle_continue(_Request, State=#{name := Name}) ->
    io:format("~n====================================================~n"
	      "Module: ~p~n"
	      "Pid:~p~n"
	      "WebShovel Name: ~p~n"
	      "State: ~p~n"
	      "----------------------------------------------------~n"
	      "~nUnknown continue: ~p~n"
	      "====================================================~n",
	      [?MODULE, self(), Name, State, _Request]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%% @end
%%--------------------------------------------------------------------
handle_cast(_Request, State=#{name := Name}) ->
    io:format("~n====================================================~n"
	      "Module: ~p~n"
	      "Pid:~p~n"
	      "WebShovel Name: ~p~n"
	      "State: ~p~n"
	      "----------------------------------------------------~n"
	      "~nUnknown cast: ~p~n"
	      "====================================================~n",
	      [?MODULE, self(), Name, State, _Request]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%% @end
%%--------------------------------------------------------------------

%% Сообщение о завершении процесса супервизора consumers
handle_info({'DOWN', Ref,process, _Pid, shutdown}, State=#{cons_ref := Ref})->
    io:format("~n~nCons_sup_sup terminate with reason: shutdown~n~n"),
    {noreply, State};
handle_info({'DOWN',Ref,process, _Pid, Reason}, State=#{cons_ref := Ref})->
    io:format("~n~nCons_sup_sup terminate with reason: ~p~n~n", [Reason] ),
    {noreply, State, {continue, start_consumers_sup_sup}};
%% Сообщение о завершении процесса подключения
handle_info({'EXIT', Conn, Reason}, State=#{connection := Conn}) ->
    {stop, {connection_close,Reason}, State};
%% Обработка прочих сообщений
handle_info(_Info, State=#{name := Name}) ->
    io:format("~n====================================================~n"
	      "Module: ~p~n"
	      "Pid:~p~n"
	      "WebShovel Name: ~p~n"
	      "State: ~p~n"
	      "----------------------------------------------------~n"
	      "~nUnknown info: ~p~n"
	      "====================================================~n",
	      [?MODULE, self(), Name, State, _Info]),
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
terminate({connection_close, Reason}, State=#{name := Name}) ->
    io:format("~n====================================================~n"
	      "Module: ~p~n"
	      "Pid:~p~n"
	      "WebShovel Name: ~p~n"
	      "State: ~p~n"
	      "----------------------------------------------------~n"
	      "Connection close with reason : ~p~n"
	      "Terminate connection worker~n"
	      "====================================================~n",
	      [?MODULE, self(), Name, State, Reason]),
    ok;
terminate(_Reason, State=#{name := Name, connection := Connection}) ->
    io:format("~n====================================================~n"
	      "Module: ~p~n"
	      "Pid:~p~n"
	      "WebShovel Name: ~p~n"
	      "State: ~p~n"
	      "----------------------------------------------------~n"
	      "Terminate with reason: ~p~n"
	      "====================================================~n",
	      [?MODULE, self(), Name, State, _Reason]),
    connection_close(Connection),
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
%% Функция запуска ConsumerSupSupervisor
%% @end
%%--------------------------------------------------------------------
start_consumers_sup_sup(#{name := Name, 
			  connection := Connection, 
			  cons_config := Config,
			  supervisor := Supervisor})->
    CunsSSupSpec = {consumers,
		    {rabbit_webshovel_consumer_sup_sup,
		     start_link, [Name,
				  Connection,
				  Config]},
		    temporary, 
		    16#ffffffff,
		    supervisor,
		    [rabbit_webshovel_consumer_sup_sup]},
    
    {ok, Pid} = supervisor2:start_child(Supervisor, CunsSSupSpec),
    erlang:monitor(process,Pid).
