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
-include("rabbit_webshovel.hrl").
%% API
-export([start_link/2]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 handle_continue/2, terminate/2, code_change/3, format_status/2]).

-define(SERVER, ?MODULE).

-define(CONS_SS_SPEC(NAME,CONNECTION,PROTOCOL, CONFIG),
        {rabbit_webshovel_consumer_sup_sup,
         {rabbit_webshovel_consumer_sup_sup,
          start_link, [{NAME, CONNECTION, PROTOCOL, CONFIG}]},
         temporary,
         16#ffffffff,
         supervisor,
         [rabbit_webshovel_consumer_sup_sup]}).

-record(state, {name,
                supervisor,
                protocol,
                connection,
                src_config,
                dst_config,
                cons_ref}).

%%%===================================================================
%%% API
%%%===================================================================


start_link(Supervisor, Config) ->
    gen_server:start_link(?MODULE, [Supervisor, Config], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([Supervisor, Config = #webshovel{}]) ->
    io:format("~n~p ~p ~p ~p~n",[?MODULE,self(),Config#webshovel.name,'_']),
    process_flag(trap_exit, true),
    Connection = make_connection(Config),
    {ok, #state{name = Config#webshovel.name,
                protocol = Config#webshovel.source#src.protocol,
                supervisor = Supervisor,
                connection = Connection,
                dst_config = Config#webshovel.destinations,
                src_config = Config#webshovel.source},
     {continue, start_consumers_sup_sup}}.

%%-----------------------HANDLE CALL----------------------------------
%% Не известные зипросы
handle_call(_Request, _From, State) ->
    {noreply, State}.

%%-----------------------HANDLE CONTINUE------------------------------
%% Запуск consumers_sup_sup
handle_continue(start_consumers_sup_sup, State)->
    start_consumers_sup_sup(State);
%% Не известные запросы продолжения
handle_continue(_Request, State) ->
    {noreply, State}.

%%-----------------------HANDLE CAST----------------------------------
%% Неизвестные асинхронные запросы
handle_cast(_Request, State) ->
    {noreply, State}.


%%-----------------------HANDLE INFO----------------------------------
%% Сообщение о завершении процесса супервизора consumers по
%% причине sutdown
handle_info({'DOWN', Ref, process, _Pid, shutdown},
            State=#state{cons_ref = Ref})->
    {noreply, State};
%% Сообщение о завершении процесса супервизора consumers по
%% причине ошибки
handle_info({'DOWN', Ref, process, _Pid, _Reason},
	    State=#state{cons_ref = Ref})->
    start_consumers_sup_sup(State);
%% Сообщение о завершении процесса подключения
handle_info({'EXIT', Conn, _Reason},
            State=#state{connection = Conn, name=Name, src_config = SrcConfig }) ->
    stop_consumer_sup_sup(State),
    Connection = make_connection(Name, SrcConfig#src.amqp_params),
    start_consumers_sup_sup(State#state{connection=Connection});
%% Обработка прочих сообщений
handle_info(_Info, State) ->
    {noreply, State}.


%%-----------------------HANDLE TERMINATE-----------------------------
terminate(_Reason, #state{connection = Connection}) ->
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
make_connection(#webshovel{name = Name,
                           source = #src{amqp_params=AmqpParams}})->
    make_connection(Name,AmqpParams).

make_connection(WSName,[])->
    throw({error,
	   {connection_not_started, read_log_for_reason}, WSName});
make_connection(Name, [AmqpParam|Rest])->
    ConnName = get_connection_name(Name),
    case amqp_connection:start(AmqpParam, ConnName) of
        {ok, Conn} ->
            link(Conn),
            Conn;
        {error, Reason} ->
            io:format("~n!!! Error start connection with reason:~p~n",
                      [Reason]),
            make_connection(Name, Rest)
    end.

%% Функция закрытия подключения
connection_close(Connection)->
    catch(amqp_connection:close(Connection)),
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
start_consumers_sup_sup(S = #state{name = Name,
                                   connection = Connection,
                                   dst_config = Config,
                                   protocol = Protocol,
                                   supervisor = Supervisor})->
    CunsSSupSpec = ?CONS_SS_SPEC(Name, Connection, Protocol, Config),
    {ok, Pid} = supervisor2:start_child(Supervisor, CunsSSupSpec),
    Ref = erlang:monitor(process,Pid),
    {noreply, S#state{cons_ref = Ref}}.

stop_consumer_sup_sup(#state{cons_ref= Ref, supervisor=Sup})->
    erlang:demonitor(Ref, [flush]),
    ok = supervisor2:terminate_child(Sup, rabbit_webshovel_consumer_sup_sup),
   %% ok = supervisor2:delete_child(Sup, rabbit_webshovel_consumer_sup_sup),
    ok.
