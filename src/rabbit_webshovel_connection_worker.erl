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


start_link(Supervisor, Config) ->
    gen_server:start_link(?MODULE, [Supervisor, Config], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([Supervisor,
     #{name := Name, 
       source := #{amqp_params := AMQPParams},
       destinations:=DestConfig}]) ->
    io:format("~n!!! Init connection: ~p. PID:~p !!!~n",[Name, self()]),
    process_flag(trap_exit, true),
    rand:seed(exs64, erlang:timestamp()),
    Connection = make_connection(Name, AMQPParams),
    {ok, #{name => Name, 
	   supervisor => Supervisor,
	   connection => Connection,
	   cons_config => DestConfig},
     {continue, start_consumers_sup_sup}}.    
%%-----------------------HANDLE CALL-----------------------------------
%% Не известные зипросы
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

%%-----------------------HANDLE CONTINUE-----------------------------------
%% Запуск consumers_sup_sup
handle_continue(start_consumers_sup_sup, State)->
    Ref = start_consumers_sup_sup(State),
    {noreply, State#{cons_ref => Ref}};
%% Не известные запросы продолжения
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

%%-----------------------HANDLE CAST-----------------------------------
%% Неизвестные асинхронные запросы
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


%%-----------------------HANDLE INFO-----------------------------------
%% Сообщение о завершении процесса супервизора consumers по
%% причине sutdown
handle_info({'DOWN', Ref, process, _Pid, shutdown},
	    State=#{cons_ref := Ref})->
    {noreply, State};

%% Сообщение о завершении процесса супервизора consumers по
%% причине ошибки
handle_info({'DOWN', Ref, process, _Pid, _Reason},
	    State=#{cons_ref := Ref})->
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


%%-----------------------HANDLE TERMINATE--------------------------------
%% Завершение работы в связи с закрытием подключения
terminate({connection_close, Reason},
	  _State=#{name := Name}) ->
    io:format("~n====================================================~n"
	      "WebShovel Name: ~p~n"
	      "----------------------------------------------------~n"
	      "Connection close with reason : ~p~n"
	      "====================================================~n",
	      [ Name, Reason]),
    ok;
%% Завершение работы иницированное супервизором
terminate(Reason,
	  _State = #{connection := Connection})
  when Reason =:= shutdown; Reason =:= killed ->
    connection_close(Connection),
    ok;
%% Другие причины завершения работы 
terminate(Reason,
	  _State=#{name := Name, connection := Connection}) ->
    io:format("~n====================================================~n"
	      "WebShovel Name: ~p~n"
	      "----------------------------------------------------~n"
	      "Terminate webshovel with reason: ~p~n"
	      "====================================================~n",
	      [Name, Reason]),
    connection_close(Connection),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, Status) ->
    Status.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% Функция создания подключения к брокеру
make_connection(WSName,[])->
    throw({error, 
	   {connection_not_started, read_log_for_reason}, WSName});
make_connection(WSName, [AmqpParam|Rest])->
    ConnName = get_connection_name(WSName),
    case amqp_connection:start(AmqpParam, ConnName) of
	{ok, Conn} ->
	    link(Conn),
	    io:format("~n!!! Connected.Connection PID: ~p~n",[Conn]),
	    Conn;
	{error, Reason} ->
	    io:format("~n!!! Error start connection with reason:~p~n", 
		      [Reason]),
	    make_connection(WSName, Rest)
    end.

%% Функция закрытия подключения
connection_close(Connection)->
    amqp_connection:close(Connection),
    ok.

%% Функция функция формирования имени подключения на основании
%% WebShovel Name
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

%% Функция запуска ConsumerSupSupervisor
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
