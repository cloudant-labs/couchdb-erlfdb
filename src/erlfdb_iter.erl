% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(erlfdb_iter).


-export([
    create/3,
    destroy/1,

    set_current/2,
    checkpoint/1,
    get_checkpoint/2,
    reset_checkpoint/1,

    new_tx/1,
    get_tx/1
]).


-define(IS_DB, {erlfdb_database, _}).
-define(IS_TX, {erlfdb_transaction, _}).
-define(IS_IT, {erlfdb_transaction, {iter, _}}).

-define(ERLFDB_ITER, '$erlfdb_iter').
-define(ERLFDB_ITER_DB, '$erlfdb_iter_db').
-define(ERLFDB_ITER_OPTS, '$erlfdb_iter_opts').
-define(ERLFDB_ITER_TX, '$erlfdb_iter_tx').
-define(ERLFDB_ITER_CURRENT, '$erlfdb_iter_current').
-define(ERLFDB_ITER_CHECKPOINT, '$erlfdb_iter_checkpoint').


create(?IS_DB = Db, ?IS_TX = Tx, Opts) when is_list(Opts) ->
    case get(?ERLFDB_ITER) of
        undefined -> ok;
        _ -> error(iterator_already_created)
    end,
    apply_opts(Tx, Opts),
    Ref = make_ref(),
    put(?ERLFDB_ITER, Ref),
    put(?ERLFDB_ITER_DB, Db),
    put(?ERLFDB_ITER_TX, Tx),
    put(?ERLFDB_ITER_OPTS, Opts),
    {erlfdb_transaction, {iter, Ref}}.


destroy(?IS_IT = Iter) ->
    validate(Iter),
    erase(?ERLFDB_ITER),
    erase(?ERLFDB_ITER_DB),
    erase(?ERLFDB_ITER_TX),
    erase(?ERLFDB_ITER_OPTS),
    erase(?ERLFDB_ITER_CURRENT),
    erase(?ERLFDB_ITER_CHECKPOINT),
    ok.


set_current(?IS_IT = Iter, {UserAcc, Key}) ->
    validate(Iter),
    put(?ERLFDB_ITER_CURRENT, {UserAcc, Key}),
    Iter.


checkpoint(?IS_IT = Iter) ->
    validate(Iter),
    put(?ERLFDB_ITER_CHECKPOINT, get(?ERLFDB_ITER_CURRENT)),
    Iter.


get_checkpoint(?IS_IT = Iter, Default) ->
    validate(Iter),
    case get(?ERLFDB_ITER_CHECKPOINT) of
        undefined -> Default;
        Val -> Val
    end.


reset_checkpoint(?IS_IT = Iter) ->
    validate(Iter),
    erase(?ERLFDB_ITER_CURRENT),
    erase(?ERLFDB_ITER_CHECKPOINT),
    Iter.


new_tx(?IS_IT = Iter) ->
    validate(Iter),
    Db = get(?ERLFDB_ITER_DB),
    Tx = erlfdb:create_read_only_transation(Db),
    apply_opts(Tx, get(?ERLFDB_ITER_OPTS)),
    put(?ERLFDB_ITER_TX, Tx),
    Iter.


get_tx(?IS_IT = Iter) ->
    validate(Iter),
    ?IS_TX = get(?ERLFDB_ITER_TX);

get_tx(?IS_TX = Tx) ->
    Tx.


% Private

apply_opts(?IS_TX = Tx, Opts) ->
    lists:foreach(fun
        ({K, V}) -> erlfdb:set_option(Tx, K, V);
        (K) when is_atom(K) -> erfdb:set_option(Tx, K)
    end, Opts),
    erlfdb:set_option(Tx, disallow_writes),
    erlfdb:set_option(Tx, retry_limit, 0),
    erlfdb:set_option(Tx, max_retry_delay, 0).


validate(Iter) ->
    {erlfdb_transaction, {iter, Ref}} = Iter,
    case get(?ERLFDB_ITER) of
        Ref -> ok;
        _ -> error(iterator_error)
    end.
