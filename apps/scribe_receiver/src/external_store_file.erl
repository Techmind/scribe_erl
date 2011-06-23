%% Author: Ilya
%% Created: 13.06.2011
%% Description: TODO: Add description to external_store_file
-module(external_store_file).

-behaviour(external_store).

%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------

-include("../include/external_store.hrl").

%% --------------------------------------------------------------------
%% External exports
%% --------------------------------------------------------------------

-export([init/1,
         get/1,
         store/2,
         destroy/1,
         periodicCheck/1
        ]).

-record(rollState, {lastRollTime=0, lastRollMinute=0, lastRollHour=0}).
-record(storeConfig, {rotateInterval=?ROTATE_DAILY, maxSize = 0, rollPeriod = 0, rotateOnOpen = false, writeMeta=false}).
-record(state, {fileRef=undefined, baseFileName, config=#storeConfig{}, rollState = #rollState{}, currentSize=0}).

%% ====================================================================
%% External functions
%% ====================================================================

init(Props) ->  
  {ok, FileRef} = file:open(proplists:get_value(path, Props), [write]),
  RotateInterval = list_to_atom(proplists:get_value(roteta_interval, Props, atom_to_list(?ROTATE_NEVER))),
  {ok, 
   #state{fileRef = FileRef, config=#storeConfig{rotateInterval=RotateInterval}}
  }.

%% [TODO]
get(_StoreState) ->
  [].

store(#state{fileRef = FileRef}, Stream) ->
  ok = file:write(FileRef, Stream),
  ok.

destroy(_StoreState) ->
  ok.

needSizeRotate(#storeConfig{maxSize=MaxSize}, #state{currentSize=CurrentSize}) ->
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

periodicCheck(#state{config = Config, rollState = RollState} = State) ->
  Rotate = 
    needSizeRotate(Config, State) or needTimeRotate(RollState, Config),
  if 
    Rotate == true ->
       rotateFile(State);
    true -> 
       State
  end
.

makeBaseFilename(NowTime, #state{config = #storeConfig{rollPeriod=RollPeriod}, baseFileName=BaseFileName}) ->
  {{Year,Month,Day},_} = calendar:now_to_local_time(NowTime),
  if 
    RollPeriod /= ?ROTATE_NEVER ->
     io:format("~s-~B-~2..0B-~2..0B", [BaseFileName, Year, Month, Day]);
    true ->
     BaseFileName
  end
.

rotateFile(#state{} = State) -> 
  %% [TODO] logToSasl
  NowTime = erlang:time(),
  NewState = openInternal(true, NowTime, State)
.

openInternal(IncrementFilename, NowTime, #state{fileRef=FileRef, config=#storeConfig{writeMeta=WriteMeta}} = State) -> 
  NewestSuffix = 
    min(
      0, 
      findNewestFileSuffix(makeBaseFilename(NowTime, State)) 
        + case IncrementFilename of true -> 1; false -> 0 end
    ),
  FileName = makeFullFilename(NewestSuffix, NowTime, State),
  RolledState = markRoll(State, NowTime),
  if 
    FileRef /= undefined ->
      if 
        WriteMeta ->
          file:write(FileRef, [?META_LOGFILE_PREFIX, FileName]);
        true ->
          ok
      end,
      file:close(FileRef);
    true ->
      ok
  end
.

markRoll(#state{config=#storeConfig{rotateInterval=RotateInterval}} = State, {_,_,Microseconds} = NowTime) ->
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
    MaxSuffix = lists:foldr(max, -1, Suffixes)
.

getFileSuffix(FilePath, Path) ->
  SuffixPos = string:rstr(FilePath, "-"),
  IsEqual = string:equal(string:substr(FilePath, 0, SuffixPos), Path),
  if IsEqual == true ->
      list_to_integer(string:substr(FilePath, SuffixPos + 1));
    true ->
      -1
  end
  .