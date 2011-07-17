%% Author: Ilya
%% Created: 13.06.2011
%% Description: TODO: Add description to external_store_file
-module(external_store_file).

-behaviour(external_store).

%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------

-include("../include/external_store.hrl").
-include("../include/receiver.hrl").

-include_lib("kernel/include/file.hrl").

%% --------------------------------------------------------------------
%% External exports
%% --------------------------------------------------------------------

-export([init/2,
         get/1,
         periodicCheck/1,
         handleMessages/2
        ]).

-record(fileConfig, {writeCategory = true, addNewlines = true, paddingChunkSize = 1024, 
  maxWriteSize = 1024024, maxFileSize = 1024024}).

-record(pathConfig, {baseFilePath, baseFileName}).
-record(fileState, {fileRef = undefined, fileSize = 0}).
-record(rollState, {lastRollTime = 0, lastRollMinute = 0, lastRollHour = 0}).
-record(storeConfig, {rotateInterval=?ROTATE_DAILY, maxSize = 0, rollPeriod = 0, 
                        rotateOnOpen = false, writeMeta = false, framed = true}).

-record(state, {fileConfig = #fileConfig{}, baseState = #baseConfig{}, 
                 fileState = #fileState {fileRef = undefined}, pathConfig = #pathConfig{}, 
                 storeConfig = #storeConfig{}, rollState = #rollState{}, 
                 queueState = #queueState{}}).

%% ====================================================================
%% External functions
%% ====================================================================

init(Props, BaseState) ->  
  %%{ok, FileRef} = file:open(proplists:get_value(path, Props), [write]),
  FileRef = undefined,
  
  %% reading rotation config
  RotateInterval = list_to_atom(proplists:get_value(rotate_interval, Props, atom_to_list(?ROTATE_NEVER))),
  MaxSize = list_to_integer(proplists:get_value(max_size, Props, "0")),
  RollPeriod = list_to_integer(proplists:get_value(roll_period, Props, "0")),
  RotateOnOpen = list_to_atom(proplists:get_value(rotate_on_open, Props, "true")),
  WriteMeta = list_to_atom(proplists:get_value(write_meta, Props, "true")),

  %% reading file config
  WriteCategory=list_to_atom(proplists:get_value(write_category, Props, "true")),
  AddNewlines=list_to_atom(proplists:get_value(add_new_lines, Props, "true")),
  PaddingChunkSize=list_to_integer(proplists:get_value(padding_chunk_size, Props, "1024")), 
  MaxWriteSize=list_to_integer(proplists:get_value(max_write_size, Props, "1024024")),
  MaxFileSize=list_to_integer(proplists:get_value(max_file_size, Props, "1024024")),

  %% reading Path configs
  BaseFilePath = proplists:get_value(file_path, Props, "/tmp/"),
  BaseFileName = proplists:get_value(base_filename, Props, "/tmp/file"),
  
  {ok, 
   #state{fileState = #fileState{fileRef=FileRef}, 
          storeConfig=#storeConfig{
                                   rotateInterval = RotateInterval,
                                   maxSize = MaxSize,
                                   rollPeriod = RollPeriod,
                                   rotateOnOpen = RotateOnOpen,
                                   writeMeta = WriteMeta
                                  },
          fileConfig=#fileConfig{
                                 writeCategory = WriteCategory,
                                 addNewlines = AddNewlines,
                                 paddingChunkSize = PaddingChunkSize,
                                 maxWriteSize = MaxWriteSize,
                                 maxFileSize = MaxFileSize
                                 },
          pathConfig=#pathConfig{
                                 baseFileName = BaseFileName,
                                 baseFilePath = BaseFilePath
                                 },
          baseState=BaseState}}
.

handleMessages(#state{fileState=#fileState{fileRef = FileRef}} = State
              , #queueState{} = QueueState) ->
  %% try opening file
  {FileOpenRef, StateOpened} = if (FileRef /= undefined) ->
       {FileRef, State};
     true ->
       FileOpenedState = open(State),
       #state{fileState=#fileState{fileRef = FileRefNew}} = FileOpenedState, 
       {FileRefNew, FileOpenedState}
  end,
  
  %% either send messages o return nothing
  if (FileOpenRef /= undefined) ->
    writeMessages(State, QueueState);
  true ->     
    {StateOpened, QueueState}
  end
.  
%% [TODO]
get(_StoreState) ->
  [].

%% ====================================================================
%% Internal Functions
%% ====================================================================
needSizeRotate(#storeConfig{maxSize=MaxSize}, #queueState{currentSize=CurrentSize}) ->
  (CurrentSize > MaxSize) and (MaxSize /= 0).	

needTimeRotate(#storeConfig{rotateInterval=RotateInterval, rollPeriod=RollPeriod}, 
			         #rollState{lastRollTime=LastRollTime, lastRollMinute=LastRollMinute, lastRollHour=LastRollHour}
  ) ->  
  case RotateInterval of
    ?ROTATE_DAILY ->
      {{_,_,Day},{Hour,Minutes,_Seconds}} = erlang:localtime(),
      (Day /= LastRollTime) and (Minutes >= LastRollMinute) and (Hour >= LastRollHour);
    ?ROTATE_HOURLY ->
      {{Hour,Minutes,_}} = erlang:time(),
      (Hour /= LastRollTime) and (Minutes >= LastRollMinute);
    ?ROTATE_OTHER ->
      {_Megaseconds,_Seconds,Microseconds} = erlang:now(),
      Microseconds >= LastRollTime + RollPeriod;
    ?ROTATE_NEVER ->
      false
	end
.	

periodicCheck(#state{storeConfig = Config, rollState = RollState} = State) ->
  Rotate = 
    needSizeRotate(Config, State) or needTimeRotate(RollState, Config),
  if 
    Rotate == true ->
       rotateFile(State);
    true -> 
       State
  end
.

makeBaseFilename(NowTime, #state{storeConfig = #storeConfig{rollPeriod=RollPeriod}, pathConfig = #pathConfig{baseFileName=BaseFileName, baseFilePath=BaseFilePath}}) ->
  {{Year,Month,Day},_} = calendar:now_to_local_time(NowTime),
  if 
    RollPeriod /= ?ROTATE_NEVER ->
     io:format("~s/~s-~B-~2..0B-~2..0B", [BaseFilePath, BaseFileName, Year, Month, Day]);
    true ->
     io:format("~s/~s", [BaseFilePath, BaseFileName])
  end
.

makeStream(#fileConfig {writeCategory=WriteCategory, addNewlines=AddNewLines, paddingChunkSize=PaddingChunkSize}, Category, Message, Length) ->
  MessageSize = byte_size(Message),
  MessageFrameIoList = [<<MessageSize:4/big-unsigned-integer-unit:8>>, Message],
  {CategoryFrameIoList, CategoryFrameSize} 
    = if 
      WriteCategory == true ->
        CategorySize = byte_size(Category),
        {[<<CategorySize:4/big-unsigned-integer-unit:8>>, Category, <<10>>], CategorySize + 5}; 
      true -> [[], 0] 
    end,
  {NewLineIoSize, NewLineIoList} = if AddNewLines == true -> {1,<<10>>}; true -> {0, []} end,
  LengthBeforePadding = CategoryFrameSize + 4 + MessageSize + NewLineIoSize,
  {Padding, PaddingLength} = pad(PaddingChunkSize, Length, LengthBeforePadding),
  {_FullLength = PaddingLength + LengthBeforePadding,  [Padding, CategoryFrameIoList, MessageFrameIoList, NewLineIoList]}
.

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
  {<<0:Padding/big-unsigned-integer-unit:8>>, Padding}.

rotateFile(#state{} = State) -> 
  %% [TODO] logToSasl
  NowTime = erlang:time(),
  openInternal(true, NowTime, State)
.

openInternal(IncrementFilename, NowTime, #state{fileState = #fileState{fileRef=FileRef}, storeConfig=#storeConfig{writeMeta=WriteMeta}} = State) -> 
  NewestSuffix = 
    min(
      0, 
      findNewestFileSuffix(makeBaseFilename(NowTime, State)) 
        + case IncrementFilename of true -> 1; false -> 0 end
    ),
  FileName = makeFullFilename(NewestSuffix, NowTime, State),
  RolledState = markRoll(State, NowTime),
  %% writing next file name
  if 
    FileRef /= undefined ->
      if 
        WriteMeta ->
          file :write(FileRef, [?META_LOGFILE_PREFIX, FileName]);
        true ->
          ok
      end,
      file:close(FileRef);
    true ->
      ok
  end,
  
  %% [TODO] add subdirectory
  
  %% create directory
  {ok} = filelib:ensure_dir(file:dirname(FileName)),
  
  case file:open(FileName, [write]) of
    {ok, FileRefNew} -> 
        {ok, #file_info{size = FileSize}} = file:read_file_info(FileName),
        RolledState#state{fileState = #fileState{fileRef = FileRefNew, fileSize = FileSize}};
    {error, Reason} -> 
      error_logger:error_report(file_open_failed, [{file, FileName}, {reason, Reason}]), 
      RolledState#state{fileState = #fileState{fileRef = undefined}}
  end
.

markRoll(#state{storeConfig=#storeConfig{rotateInterval=RotateInterval}} = State, {_,_,Microseconds} = NowTime) ->
  {{_Year,_Month,Day},{Hour,_Minutes,_Seconds}} = calendar:now_to_local_time(NowTime),
  case RotateInterval of
    ?ROTATE_DAILY ->
      State#rollState{lastRollTime=Day};
    ?ROTATE_HOURLY ->
      State#rollState{lastRollTime=Hour};
    ?ROTATE_OTHER ->
      State#rollState{lastRollTime=Microseconds};
    ?ROTATE_NEVER ->
      State
  end
.

makeFullFilename(NewestSuffix, NowTime, State) ->
  io:format("~s_~5..0B", [makeBaseFilename(NowTime, State), NewestSuffix])
.

findNewestFileSuffix(Path) ->
    Files = file:list_dir(Path),
    Suffixes = lists:map(fun(FilePath) -> getFileSuffix(FilePath, Path) end, Files),
    lists:foldr(max, -1, Suffixes)
.

getFileSuffix(FilePath, Path) ->
  SuffixPos = string:rstr(FilePath, "-"),
  IsEqual = string:equal(string:substr(FilePath, 0, SuffixPos), Path),
  if IsEqual ->
      list_to_integer(string:substr(FilePath, SuffixPos + 1));
    true ->
      -1
  end
  .

open(State) ->
  openInternal(false, erlang:time(), State)
.

writeMessages(#state{} = State
              , #queueState{messages = Messages} = QueueState) ->    
  {StateNew, UnprocessedList, _NumWritten} = processMsg(State, 0, 0, 0, [], Messages),

  {StateNew, QueueState#queueState{messages = UnprocessedList}}
.

processMsg(#state{
              fileConfig = FileConfig                     
            } = State, 
            NumWritten, 
            NumBuffered, BufferedSize, Buffer, [#logMessage{category = Category, message = Message} | Messages]) ->
    
  {StreamLength, Stream} = makeStream(FileConfig, Category, Message, BufferedSize),
  BufferNew = [Stream | Buffer],
  BufferSizeNew = BufferedSize + StreamLength,
  NumBufferedNew = NumBuffered + 1,
    
  {StateNew, CantProcess, NumWrittenNew, NumBufferedNew2, BufferedSizeNew2, BufferNew2} =
    checkWrite(State, NumWritten, NumBufferedNew, BufferSizeNew, BufferNew, false),

  if CantProcess ->
      {StateNew, lists:append(BufferNew, Messages), NumWritten};
    true ->
      processMsg(StateNew, NumWrittenNew, NumBufferedNew2, BufferedSizeNew2, BufferNew2, Messages)
  end
;

processMsg(#state{} = State, 
            NumWritten, 
            NumBuffered, BufferedSize, Buffer, []) ->
    
  {StateNew, CantProcess, NumWrittenNew, _, _, BufferNew2} =
    checkWrite(State, NumWritten, NumBuffered, BufferedSize, Buffer, true),

  if CantProcess ->
      {StateNew, BufferNew2, NumWrittenNew};
    true ->
      {StateNew, [], NumWrittenNew}
  end
.

checkWrite(#state{
              fileConfig = #fileConfig{maxWriteSize = MaxWriteSize, maxFileSize = MaxFileSize},
              fileState = #fileState{fileRef = FileRef, fileSize = FileSize}                     
            } = State, 
            NumWritten, 
            NumBuffered, BufferedSize, Buffer, ForceWrite) ->    
    
    if (((BufferedSize > MaxWriteSize) and (MaxFileSize /= 0)) or ForceWrite) ->
      case file:write(FileRef, Buffer) of
        ok -> 
          FileSizeNew = FileSize + BufferedSize,
          StateChanged = 
            if ((FileSizeNew > MaxWriteSize) and (MaxWriteSize /= 0)) ->
              rotateFile(State);
            true ->
              State#state{fileState = #fileState{fileSize = FileSizeNew, fileRef = FileRef}}
            end,                
          {
            StateChanged,
            false,
            NumWritten + NumBuffered,
            0, 0, []
          };
        {error, Reason} ->
          error_logger:error_report(file_write_failed, [{reason, Reason}]), 
          {
            State,
            true,
            NumWritten,
            {0, 0, []}
          }
      end;
    true ->
     {State, false, NumWritten, NumBuffered, BufferedSize, Buffer}
    end
.

