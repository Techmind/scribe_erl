%% Author: Ilya
%% Created: 13.06.2011
%% Description: TODO: Add description to external_store_file
-module(external_store_file).

-behaviour(external_store).
%% --------------------------------------------------------------------
%% External exports
%% --------------------------------------------------------------------

-export([init/1,
         get/1,
         store/2,
         destroy/1
        ]).

-record(state, {fileRef}).

%% ====================================================================
%% External functions
%% ====================================================================

init(Props) ->  
  {ok, FileRef} = file:open(proplists:get_value(path, Props), [write]),
  {ok, #state{fileRef = FileRef}}.

%% [TODO]
get(_StoreState) ->
  [].

store(#state{fileRef = FileRef}, Stream) ->
  ok = file:write(FileRef, Stream),
  ok.

destroy(_StoreState) ->
  ok.

