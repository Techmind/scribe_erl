%%% -------------------------------------------------------------------
%%% Author  : Ilya
%%% Description : Keeps track of Categories Bindings
%%%
%%% Created : 13.06.2011
%%% -------------------------------------------------------------------
-module(storager).

-behaviour(gen_server).
%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------

%% --------------------------------------------------------------------
%% External exports
-export([start/1, findStore/1, store/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {stores = []}).
-record(storage, {state, category}).

%% ====================================================================
%% External functions
%% ====================================================================
start(Config) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Config, []).

findStore(Category) ->
	gen_server:call(?MODULE, {find_store, Category}).

store(Store, Message) ->
	gen_server:call(?MODULE, {store, Store, Message}).

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
	Stores = init_stores(Config),
    {ok, #state{stores = Stores}}.

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
handle_call({find_store, _Category}, _, #state{stores = [FirstStore | _]} = State) ->
    {reply, {ok, FirstStore}, State};

handle_call({store, FirstStore, Message}, _, State) ->
	Code = external_store:store(FirstStore, Message),
    {reply, {Code}, State}.

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

init_stores(_Config) ->
	{ok, State} = external_store:init(file, [{path, "/tmp/file"}]),
	[#storage{state=State, category=default}].