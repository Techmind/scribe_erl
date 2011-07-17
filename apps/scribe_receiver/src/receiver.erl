%%% -------------------------------------------------------------------
%%% Author  : Ilya
%%% Description :
%%%
%%% Created : 13.06.2011
%%% -------------------------------------------------------------------
-module(receiver).

-behaviour(gen_server).
%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------

-include("../include/receiver.hrl").

%% --------------------------------------------------------------------
%% External exports
%% --------------------------------------------------------------------

-export([start/1, add/1, add/2, getStatus/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%statuses = dead,statring,alive,stopping,stopped,warning

-record(state, {config,status=alive,stores=[]}).

%% ====================================================================
%% External functions
%% ====================================================================
start(Config) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Config, []).

add(Msg) ->
    gen_server:call(?MODULE, Msg).

add(Category, Message) ->
    gen_server:call(?MODULE, #logMessage{category = Category, message = Message}).

%%register(Pid) ->
%%   gen_server:call(?MODULE, {register, Pid}).

getStatus() ->
    gen_server:call(?MODULE, status).
	

%% ====================================================================
%% Server functions
%% ====================================================================

%% --------------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State}          |
%%          {ok, State, Timeout} |
%%          ignore               |
%%          {stop, Reason}
%% --------------------------------------------------------------------
init(Config) ->	
    {ok, #state{config=Config,status=alive}}.

%% --------------------------------------------------------------------
%% Function: handle_call/3
%% Description: Handling call messages
%% Returns: {reply, Reply, State}          |
%%          {reply, Reply, State, Timeout} |
%%          {noreply, State}               |
%%          {noreply, State, Timeout}      |
%%          {stop, Reason, Reply, State}   | (terminate/2 is called)
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_call(status, _, #state{status = Status} = State) ->
    {reply, #receiverReply{response=Status}, State};

%%handle_call({register, Pid}, _, #state{} = State) ->
%%    NewState = registerStore(State, Pid),
%%    
%%    {reply, #receiverReply{response=ok}, NewState};

handle_call(#logMessage{category = Category, message = Message}, _, State) ->
  %%  {ok, State} = storager:findStore(Category),
	%% Send Messages to all stores
  Code = storager:store(State, Message),
	{reply, #receiverReply{code=Code}, State};


handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%% --------------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%% --------------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%% --------------------------------------------------------------------
%% Function: terminate/2
%% Description: Shutdown the server
%% Returns: any (ignored by gen_server)
%% --------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%% --------------------------------------------------------------------
%% Func: code_change/3
%% Purpose: Convert process state when code is changed
%% Returns: {ok, NewState}
%% --------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% --------------------------------------------------------------------
%%% Internal functions
%% --------------------------------------------------------------------

%%registerStore(#state{stores = Stores} = State, Pid) ->
%%  State#state{stores = [Pid | Stores]}.