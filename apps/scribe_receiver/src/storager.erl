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
-export([start/1, findStore/1, store/3, findAndStore/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {stores = []}).
-record(config, {writeCategory=true, addNewlines=true, paddingChunkSize=1024, maxFileSize=1024024}).
-record(storage, {state, category, config}).

%% ====================================================================
%% External functions
%% ====================================================================
start(Config) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Config, []).

findStore(Category) ->
	gen_server:call(?MODULE, {find_store, Category}).

store(Store, Category, Message) ->
	gen_server:call(?MODULE, {store, Store, Category, Message}).

findAndStore(Category, Message) ->
	{ok, FirstStore} = findStore(Category),
	store(FirstStore, Category, Message).
	

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

handle_call({store, FirstStore, Category, Message}, _, State) ->
	#storage{state = StoreState, config = Config} = FirstStore,
	[_StreamLength, Stream] = makeStream(Config, Category, Message),
	Code = external_store:store(StoreState, Stream),
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
	StoreConfig = #config{paddingChunkSize = 10240},
	[#storage{state=State, category=default, config=StoreConfig}].

makeStream(Config, Category, Message) ->
  makeStream(Config, Category, Message, 0).

makeStream(#config{writeCategory=WriteCategory, addNewlines=AddNewLines, paddingChunkSize=PaddingChunkSize, maxFileSize=MaxFileSize} = Config, Category, Message, Length) ->
	MessageSize = byte_size(Message),
	MessageFrameIoList = [<<MessageSize:4/big-unsigned-integer-unit:8>>, Message],
	[CategoryFrameIoList, CategoryFrameSize] 
		= if 
			WriteCategory == true ->
				CategorySize = byte_size(Category),
				[[<<CategorySize:4/big-unsigned-integer-unit:8>>, Category, <<10>>], CategorySize + 5]; 
			true -> [[], 0] 
		end,
	[NewLineIoSize, NewLineIoList] = if AddNewLines == true -> [1,<<10>>]; true -> [0, undefined] end,
	LengthBeforePadding = CategoryFrameSize + 4 + MessageSize + NewLineIoSize,
	[Padding, PaddingLength] = pad(PaddingChunkSize, Length, LengthBeforePadding),
	[_FullLength = PaddingLength + LengthBeforePadding,  [Padding, CategoryFrameIoList, MessageFrameIoList, NewLineIoList]].

pad(PaddingChunkSize, LengthLeft, MessageSize) ->
	Padding = if 
				PaddingChunkSize > 0 ->
					SpaceLeftInChunk 
									 = ((PaddingChunkSize - LengthLeft) rem PaddingChunkSize)*8,
					if 
						MessageSize > SpaceLeftInChunk -> SpaceLeftInChunk;
						true -> 0
					end;
				true -> 0 
				end,
	[<<0:Padding/big-unsigned-integer-unit:8>>, Padding].