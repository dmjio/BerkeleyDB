#include <db.h>
#include <string.h>

#include "wrapper.h"

int hs_open(DB *db, DB_TXN *txnid, const char *file,
    const char *database, DBTYPE type, u_int32_t flags, int mode)
{ return db->open(db,txnid,file,database,type,flags,mode); }

int hs_close(DB *db, u_int32_t flags)
{ return db->close(db, flags); }

void hs_gc_close(DB *db)
{ db->close(db, DB_NOSYNC); }

int hs_put(DB *db,
        DB_TXN *txnid, DBT *key, DBT *data, u_int32_t flags)
{ return db->put(db,txnid,key,data,flags); }

int hs_get(DB *db,
        DB_TXN *txnid, DBT *key, DBT *data, u_int32_t flags)
{ return db->get(db,txnid,key,data,flags); }

void hs_clear_dbt(DBT *dbt)
{ memset(dbt, 0, sizeof(DBT)); }

void hs_multiple_init(void **ptr, DBT *data)
{
  DB_MULTIPLE_INIT(*ptr,data);
}

void hs_multiple_next(void **ptr, DBT *data, void **retdata, size_t *retdlen)
{
  DB_MULTIPLE_NEXT(*ptr,data,*retdata,*retdlen);
}

int hs_set_flags(DB *db, u_int32_t flags)
{ return db->set_flags(db,flags); }

int hs_get_flags(DB *db, u_int32_t *flags)
{ return db->get_flags(db,flags); }


int hs_cursor(DB *db, DB_TXN *txnid, DBC **cursorp, u_int32_t flags)
{ return db->cursor(db,txnid,cursorp,flags); }


int hs_cursor_get(DBC *DBcursor,
    DBT *key, DBT *data, u_int32_t flags)
{ return DBcursor->get(DBcursor, key, data, flags); }

int hs_cursor_close(DBC *DBcursor)
{ return DBcursor->close(DBcursor); }


int hs_env_open(DB_ENV *dbenv, char *db_home, u_int32_t flags, int mode)
  { return dbenv->open(dbenv,db_home,flags,mode); }


