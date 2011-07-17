%%% -------------------------------------------------------------------
%%% Author  : Ilya
%%% Description : Keeps track of Categories Bindings
%%%
%%% Created : 13.06.2011
%%% -------------------------------------------------------------------
-module(external_store).

-behaviour(gen_server).


%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include("../include/external_store.hrl").

%% --------------------------------------------------------------------
%% External exports
%% --------------------------------------------------------------------


%% export behaviour
-export([behaviour_info/1]).

%% function calls
-export([start/1,
         addMessage/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(storeState, {storeModule, 
externalState, 
baseConfig = #baseConfig{}, 
queueState = #queueState{},
lastHandleTime = undefined                                         
}).

%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------

-include("../include/external_store.hrl").

%% ====================================================================
%% External functions
%% ====================================================================
behaviour_info(callbacks) ->
  [{init, 2},
   {store, 3}];
behaviour_info(_Other) ->
  undefined.


start(Props) ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, Props, []).

addMessage(Pid, LogEntry) ->
  gen_server:call(Pid, {add, LogEntry}).

%% check for rotation

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
init(Props) -> 
  BaseProps = #baseConfig{maxWriteInterval = MaxWriteInterval} = base_init(Props),
  Type = list_to_atom(proplists:get_value(type, Props)),
  Mod = list_to_atom("external_store_" ++ atom_to_list(Type)),
  case Mod:init(Props, BaseProps) of
    {ok, TypedState} ->
                timer:send_interval(MaxWriteInterval, self(), timer),
                {ok, 
                    #storeState{
                      storeModule = Mod, 
                      externalState = TypedState,
                      baseConfig = BaseProps,
                      lastHandleTime = erlang:time()
                    }
                  };
    _ -> error
  end
.

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
handle_call({add, LogEntry}, _From, State) ->
    StateNew = store(State, LogEntry),
    {reply, ok, StateNew}.

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
handle_info(timer, State) ->
    NewState = timer(State),
    {noreply, NewState};

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


%% ====================================================================
%% Internal functions
%% ====================================================================
base_init(Props) ->
    TargetWriteSize = list_to_integer(proplists:get_value(target_write_size, Props, "1024024")),

    MaxWriteInterval = list_to_integer(proplists:get_value(max_write_interval, Props, "10")),

    Category = list_to_atom(proplists:get_value(category, Props)),
    CategoriesString = proplists:get_value(categories, Props),
    CategoriesList = 
                    case length(CategoriesString) of
                        0 ->  [Category]; 
                        _ -> re:split(CategoriesString, " ")
                    end,
    #baseConfig{category = CategoriesList, targetWriteSize = TargetWriteSize, maxWriteInterval = MaxWriteInterval}
.
store(#storeState{baseConfig = #baseConfig{targetWriteSize = TargetWriteSize}, 
                  queueState = #queueState{messages = Messages, currentSize = Size}
                 } = State
     , LogEntry) ->
  {_, Message} = LogEntry,
  NewSize = Size + length(Message),
  QueuedState = State#queueState{messages = [LogEntry | Messages], currentSize = NewSize},
  %% handle messages if size is good
  if 
      (NewSize >= TargetWriteSize) ->
          processMsgBuffers(QueuedState);
      true ->
          QueuedState
  end.

processMsgBuffers(#storeState{storeModule = StoreModule, externalState = ModuleState,
                              queueState = QueueState
                             } = State) ->
  %% [TODO] check commands here
  
  %% periodicCheck
  ModuleStateAfterPeriodic = StoreModule:periodicCheck(ModuleState),

  {ModuleState2, QueueStateAfter} 
    = StoreModule:handleMessages(ModuleStateAfterPeriodic, QueueState),
  
  State#storeState{externalState = ModuleState2, queueState = QueueStateAfter, lastHandleTime = erlang:time()}
.

timer(#storeState{baseConfig = #baseConfig{maxWriteInterval = MaxWriteInterval}, lastHandleTime = LastHandleTime} = State) ->
   CurrentTime = erlang:time(),
  if 
    (CurrentTime - LastHandleTime) >= MaxWriteInterval ->
      processMsgBuffers(State);
    true ->
      State
end.
  