%%% -------------------------------------------------------------------
%%% Author  : Ilya
%%% Description : Keeps track of Categories Bindings
%%%
%%% Created : 13.06.2011
%%% -------------------------------------------------------------------
-module(external_store).

%% --------------------------------------------------------------------
%% External exports
%% --------------------------------------------------------------------

-export([behaviour_info/1]).

-export([init/2,
         get/1,
         store/2,
         destroy/1
        ]).

-record(storeState, {state, store_module}).

%% ====================================================================
%% External functions
%% ====================================================================
behaviour_info(callbacks) ->
  [{init, 1},
   {get, 1},
   {store, 2},
   {destroy, 1}];
behaviour_info(_Other) ->
  undefined.


init(Type, Props) ->  
  Mod = list_to_atom("external_store_" ++ atom_to_list(Type)),
  case Mod:init(Props) of
    {ok, SState} -> {ok, #storeState{store_module = Mod, state = SState}};
    _ -> error 
  end.

get(#storeState{store_module = Mod, state = StoreState}) ->
  Mod:get(StoreState).

store(#storeState{store_module = Mod, state = StoreState}, Message) ->
  Mod:store(StoreState, Message).

destroy(#storeState{store_module = Mod, state = StoreState}) ->
  Mod:destroy(StoreState).