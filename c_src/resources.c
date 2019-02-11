// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

#include <assert.h>

#include "atoms.h"
#include "resources.h"
#include "util.h"


ErlNifResourceType* ErlFDBFutureRes;
ErlNifResourceType* ErlFDBClusterRes;
ErlNifResourceType* ErlFDBDatabaseRes;
ErlNifResourceType* ErlFDBTransactionRes;
ErlNifResourceType* ErlFDBTransactionLockRes;


int
erlfdb_init_resources(ErlNifEnv* env)
{

    ErlFDBFutureRes = enif_open_resource_type(
            env,
            NULL,
            "erlfdb_future",
            erlfdb_future_dtor,
            ERL_NIF_RT_CREATE,
            NULL
        );
    if(ErlFDBFutureRes == NULL) {
        return 0;
    }

    ErlFDBClusterRes = enif_open_resource_type(
            env,
            NULL,
            "erlfdb_cluster",
            erlfdb_cluster_dtor,
            ERL_NIF_RT_CREATE,
            NULL
        );
    if(ErlFDBClusterRes == NULL) {
        return 0;
    }

    ErlFDBDatabaseRes = enif_open_resource_type(
            env,
            NULL,
            "erlfdb_database",
            erlfdb_database_dtor,
            ERL_NIF_RT_CREATE,
            NULL
        );
    if(ErlFDBDatabaseRes == NULL) {
        return 0;
    }

    ErlFDBTransactionRes = enif_open_resource_type(
            env,
            NULL,
            "erlfdb_transaction",
            erlfdb_transaction_dtor,
            ERL_NIF_RT_CREATE,
            NULL
        );
    if(ErlFDBTransactionRes == NULL) {
        return 0;
    }

    ErlFDBTransactionLockRes = enif_open_resource_type(
            env,
            NULL,
            "erlfdb_transaction_lock",
            erlfdb_transaction_lock_dtor,
            ERL_NIF_RT_CREATE,
            NULL
        );
    if(ErlFDBTransactionLockRes == NULL) {
        return 0;
    }

    return 1;
}


void
erlfdb_future_dtor(ErlNifEnv* env, void* obj)
{
    ErlFDBFuture* f = (ErlFDBFuture*) obj;

    if(f->future != NULL) {
        fdb_future_destroy(f->future);
    }
}


void
erlfdb_cluster_dtor(ErlNifEnv* env, void* obj)
{
    ErlFDBCluster* c = (ErlFDBCluster*) obj;

    if(c->cluster != NULL) {
        fdb_cluster_destroy(c->cluster);
    }
}


void
erlfdb_database_dtor(ErlNifEnv* env, void* obj)
{
    ErlFDBDatabase* d = (ErlFDBDatabase*) obj;

    if(d->database != NULL) {
        fdb_database_destroy(d->database);
    }
}


void
erlfdb_transaction_dtor(ErlNifEnv* env, void* obj)
{
    ErlFDBTransaction* t = (ErlFDBTransaction*) obj;

    if(t->transaction != NULL) {
        fdb_transaction_destroy(t->transaction);
    }
}


void
erlfdb_transaction_lock_dtor(ErlNifEnv* env, void* obj)
{
    ErlFDBTransactionLock* l = (ErlFDBTransactionLock*) obj;
    erlfdb_transaction_lock_destroy(env, l);
}


void
erlfdb_transaction_lock_destroy(ErlNifEnv* env, ErlFDBTransactionLock* l)
{
    ErlNifPid dst_pid;
    ErlNifEnv* tx_env;
    ERL_NIF_TERM head;
    ERL_NIF_TERM tail;
    const ERL_NIF_TERM* tuple;
    int arity;

    if(l->t != NULL) {
        enif_mutex_lock(l->t->lock);
        assert(l->t->owner == l && "multiple tx lock owner failure");
        assert(l->t->locked && "invalid lock owner on unlocked tx");

        tx_env = l->t->env;
        tail = l->t->waiters;

        while(!enif_is_empty_list(l->t->env, l->t->waiters)) {
            if(!enif_get_list_cell(tx_env, tail, &head, &tail)) {
                break;
            }

            if(!enif_get_tuple(tx_env, head, &arity, &tuple)) {
                break;
            }

            if(arity != 2) {
                break;
            }

            if(!enif_get_local_pid(tx_env, tuple[0], &dst_pid)) {
                break;
            }

            // No checking errors because either we can't send
            // or dst_pid is dead so nothing to be done but let
            // each waiter timeout on their own.
            enif_send(env, &dst_pid, NULL, T2(env, ATOM_unlocked, tuple[1]));
        }

        enif_free_env(l->t->env);

        l->t->env = NULL;
        l->t->locked = 0;
        l->t->owner = NULL;

        enif_mutex_unlock(l->t->lock);

        // This is done outside holding the mutex in case we're
        // the last reference. Triggering the ErlFDBTransaction
        // destructor would lead to a deadlock when it tried
        // to also acquire the mutex.
        enif_release_resource(l->t);
        l->t = NULL;
    }
}
