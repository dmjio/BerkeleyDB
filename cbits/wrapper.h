#if !defined(__WRAPPER_H__)
#define __WRAPPER_H__

int hs_open(DB *db, DB_TXN *txnid, const char *file,
    const char *database, DBTYPE type, u_int32_t flags, int mode);

int hs_close(DB *db, u_int32_t flags);
void hs_gc_close(DB *db);

int hs_put(DB *db, DB_TXN *txnid, DBT *key, DBT *data, u_int32_t flags);

int hs_get(DB *db, DB_TXN *txnid, DBT *key, DBT *data, u_int32_t flags);

void hs_clear_dbt(DBT *dbt);


void hs_multiple_init(void **ptr, DBT *data);

void hs_multiple_next(void **ptr, DBT *data, void **retdata, size_t *retdlen);

int hs_set_flags(DB *db, u_int32_t flags);
int hs_get_flags(DB *db, u_int32_t *flags);

int hs_cursor(DB *db, DB_TXN *txnid, DBC **cursorp, u_int32_t flags);
int hs_cursor_get(DBC *DBcursor, DBT *key, DBT *data, u_int32_t flags);
int hs_cursor_close(DBC *DBcursor);

int hs_env_open(DB_ENV *dbenv, char *db_home, u_int32_t flags, int mode);

#endif
