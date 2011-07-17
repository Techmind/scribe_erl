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
-export([start/1, store/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {storePids = []}).

%% ====================================================================
%% External functions
%% ====================================================================
start(Config) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Config, []).

store(Category, Message) ->
	gen_server:call(?MODULE, {store, Category, Message}).


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
  {ok, #state{storePids = Stores}}.

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
handle_call({store, Category, Message}, _, #state{storePids = StorePids} = State) ->
  _Replies = lists:foreach(fun(Pid) -> external_store:store(Pid, Category, Message ) end, 
                           StorePids),
  {reply, ok, State}.

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
  PropsArray = [
                [{type, "file"}, {category="default"}],
                [{type, "file"}, {category="1"}]
  ],  
  States = lists:foreach(fun(Config) -> external_store:start(Config) end, PropsArray),
  Pids = lists:foldl(
           fun(Elem, PidsList) -> 
             case Elem of 
                 {ok, Pid} -> [Pid | PidsList];
                              
                 _ -> PidsList
             end
           end
           , [], States),
	#state{storePids = Pids}.