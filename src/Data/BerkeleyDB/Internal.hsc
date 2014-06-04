{-# LANGUAGE ForeignFunctionInterface, EmptyDataDecls #-}
#include <db.h>
module Data.BerkeleyDB.Internal where

import Foreign.C.Error
import Foreign.C
import Foreign
import System.IO.Unsafe
import Data.Char

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as Char8
import qualified Data.ByteString.Unsafe as B
import qualified Data.ByteString.Internal as B

data DB
data DBT
data DBC
data ENV

type DBTYPE = #{type DBTYPE}

data Flag
    = AutoCommit
    | Create
    | Excl
    | Multiversion
    | NoMMap
    | ReadOnly
    | ReadUncommitted
    | Thread
    | Truncate
    deriving (Read, Show, Enum, Eq, Ord)

data DbType
    = BTree
    | Hash
    | Recno
    | Queue
    | UnknownType

fromDbType BTree = #{const DB_BTREE}
fromDbType Hash = #{const DB_HASH}
fromDbType Recno = #{const DB_RECNO}
fromDbType Queue = #{const DB_QUEUE}
fromDbType UnknownType = #{const DB_UNKNOWN}

fromFlag AutoCommit = #{const DB_AUTO_COMMIT}
fromFlag Create = #{const DB_CREATE}
fromFlag Excl = #{const DB_EXCL}
fromFlag Multiversion = #{const DB_MULTIVERSION}
fromFlag _ = 0

fromFlags = foldr (.|.) 0 . map fromFlag

data SyncOption = Sync | NoSync

fromSyncOption Sync = 0
fromSyncOption NoSync = #{const DB_NOSYNC}

data PutFlag
    = Append
    | NoDupData
    | NoOverwrite
    deriving (Read, Show, Enum, Eq, Ord)

fromPutFlag Append = #{const DB_APPEND}
fromPutFlag NoDupData = #{const DB_NODUPDATA}
fromPutFlag NoOverwrite = #{const DB_NOOVERWRITE}

fromPutFlags = foldr (.|.) 0 . map fromPutFlag

data DbFlag
    = Consume
    | ConsumeWait
    | Multiple
    | Duplicates
    | SortDuplicates
    | Next

fromDbFlag Consume = #{const DB_CONSUME}
fromDbFlag ConsumeWait = #{const DB_CONSUME_WAIT}
fromDbFlag Multiple = #{const DB_MULTIPLE}
fromDbFlag Duplicates = #{const DB_DUP}
fromDbFlag SortDuplicates = #{const DB_DUPSORT}
fromDbFlag Next = #{const DB_NEXT}

fromDbFlags = foldr (.|.) 0 . map fromDbFlag

type Object = B.ByteString



--int db_env_create(DB_ENV **dbenvp, u_int32_t flags);
foreign import ccall unsafe db_env_create
  :: Ptr (Ptr ENV) -> CUInt -> IO CInt

createEnv :: IO (Ptr ENV)
createEnv = alloca $ \tmp ->
            do throwErrnoIf (/=0) "db_env_create" $ db_env_create tmp 0
               peek tmp

--int DB_ENV->open(DB_ENV *dbenv, char *db_home, u_int32_t flags, int mode);
foreign import ccall unsafe hs_env_open
  :: Ptr ENV -> CString -> CUInt -> CInt -> IO CInt




--int db_create(DB **dbp, DB_ENV *dbenv, u_int32_t flags);
foreign import ccall unsafe "db_create" c_create ::
  Ptr (Ptr DB) -> Ptr ENV -> Word32 -> IO CInt

create :: IO (Ptr DB)
create = alloca $ \tmp ->
         do --env <- createEnv
--            let flags = #{const DB_INIT_CDB} .|. #{const DB_INIT_MPOOL} .|. #{const DB_CREATE} .|. #{const DB_PRIVATE} .|. #{const DB_THREAD}
--            throwErrnoIf (/=0) "env_open" $ hs_env_open env nullPtr flags 0
            throwErrnoIf (/=0) "create" $ c_create tmp nullPtr 0
            peek tmp


-- int DB->open(DB *db, DB_TXN *txnid, const char *file,
--    const char *database, DBTYPE type, u_int32_t flags, int mode);
foreign import ccall unsafe "hs_open" c_open ::
  Ptr DB -> Ptr () -> CString -> CString -> DBTYPE -> Word32 -> CInt -> IO CInt

open :: Maybe FilePath -> Maybe String -> DbType -> [Flag] -> IO (Ptr DB)
open mbFile mbDatabase dbType flags
    = do ptr <- create
         setFlags ptr [Duplicates]
         maybeWith withCString mbFile $ \cfile ->
           maybeWith withCString mbDatabase $ \cdatabase ->
             throwErrnoIf (/=0) "open" $
               c_open ptr nullPtr cfile cdatabase (fromDbType dbType) (fromFlags flags) 0
         return ptr




-- int DB->close(DB *db, u_int32_t flags);
foreign import ccall unsafe "hs_close" c_close :: Ptr DB -> Word32 -> IO CInt

close :: Ptr DB -> SyncOption -> IO ()
close db sync
    = do throwErrnoIf (/=0) "close" $ c_close db (fromSyncOption sync)
         return ()

foreign import ccall unsafe "&hs_gc_close" closePtr :: FunPtr (Ptr DB -> IO ())


foreign import ccall unsafe "hs_clear_dbt" clear_dbt :: Ptr DBT -> IO ()

withDBT :: Object -> (Ptr DBT -> IO a) -> IO a
withDBT object fn
    = allocaBytes #{size DBT} $ \dbt ->
      do clear_dbt dbt
         B.unsafeUseAsCStringLen object $ \(ptr,len) ->
           do #{poke DBT, data} dbt ptr
              #{poke DBT, size} dbt (fromIntegral len :: CInt)
              fn dbt


-- int DB->put(DB *db, DB_TXN *txnid, DBT *key, DBT *data, u_int32_t flags);
foreign import ccall unsafe "hs_put" c_put ::
  Ptr DB -> Ptr () -> Ptr DBT -> Ptr DBT -> Word32 -> IO CInt

put :: Ptr DB -> Object -> Object -> [PutFlag] -> IO ()
put db key value flags
    = withDBT key $ \dbtKey ->
      withDBT value $ \dbtValue ->
      do throwErrnoIf (/=0) "put" $
           c_put db nullPtr dbtKey dbtValue (fromPutFlags flags)
         return ()


-- int DB->get(DB *db, DB_TXN *txnid, DBT *key, DBT *data, u_int32_t flags);
foreign import ccall unsafe "hs_get" c_get ::
  Ptr DB -> Ptr () -> Ptr DBT -> Ptr DBT -> Word32 -> IO CInt

get :: Ptr DB -> Object -> [DbFlag] -> IO (Maybe Object)
get db object flags
    = withDBT object $ \keyDbt ->
      withDBT nullObject $ \dataDbt ->
      do #{poke DBT, flags} keyDbt (#{const DB_DBT_MALLOC} :: Word32)
         ret <- c_get db nullPtr keyDbt dataDbt (fromDbFlags flags)
         case ret of
             #{const DB_NOTFOUND} -> return Nothing
             0 -> do ptr <- #{peek DBT, data} dataDbt
                     size <- #{peek DBT, size} dataDbt :: IO CInt
                     bs <- B.unsafePackCStringFinalizer ptr (fromIntegral size) (free ptr)
                     return $ Just bs
             _ -> throwErrno "get"
{-
foreign import ccall unsafe hs_multiple_init
  :: Ptr (Ptr a) -> Ptr DBT -> IO ()

foreign import ccall unsafe hs_multiple_next
  :: Ptr (Ptr a) -> Ptr DBT -> Ptr (Ptr b) -> Ptr #{type size_t} -> IO ()
-}
getMany :: Ptr DB -> Object -> [DbFlag] -> IO [Object]
getMany db object flags
    = withDBT object $ \keyDbt ->
      withDBT nullObject $ \dataDbt ->
      dbtGetMany dataDbt (c_get db nullPtr keyDbt dataDbt (fromDbFlags (Multiple:flags)))

dbtGetMany dataDbt fn
    = let loop size =
           do ptr <- mallocBytes size
              #{poke DBT, data} dataDbt ptr
              #{poke DBT, flags} dataDbt (#{const DB_DBT_USERMEM} :: Word32)
              #{poke DBT, ulen} dataDbt (fromIntegral size :: Word32)
              ret <- fn
              case ret of
                #{const DB_NOTFOUND} -> free ptr >> return []
                #{const DB_BUFFER_SMALL} -> free ptr >> loop (size*2)
                0 -> do ulen <- fmap fromIntegral (#{peek DBT, ulen} dataDbt :: IO Word32)
                        fptr <- newForeignPtr finalizerFree ptr
                        let intPtr = castPtr ptr
                        let walker n ls = do offset <- peekByteOff intPtr (ulen - #{size u_int32_t} * (n+1)) :: IO Word32
                                             len <- peekByteOff intPtr (ulen - #{size u_int32_t} * (n+2)) :: IO Word32
--                                             putStrLn $ "walker: " ++ show (ulen, offset, len, ptr)
                                             if offset == -1 || (offset == 0 && len == 0)
                                                then return ls
                                                else let bs = B.fromForeignPtr fptr (fromIntegral offset) (fromIntegral len)
                                                     in walker (n+2) (bs:ls)
                        walker 0 []
                _ -> throwErrno "getMany"
      in loop (1024*sizeOf (undefined :: Int))


--int hs_set_flags(DB *db, u_int32_t flags);
foreign import ccall unsafe hs_set_flags :: Ptr DB -> Word32 -> IO CInt

setFlags :: Ptr DB -> [DbFlag] -> IO ()
setFlags ptr flags
    = do throwErrnoIf (/=0) "setFlags" $ hs_set_flags ptr (fromDbFlags flags)
         return ()

--int hs_get_flags(DB *db, u_int32_t *flags);

foreign import ccall unsafe "hs_cursor"
  hs_cursor :: Ptr DB -> Ptr () -> Ptr (Ptr DBC) -> Word32 -> IO CInt

foreign import ccall unsafe "hs_cursor_get"
  hs_cursor_get :: Ptr DBC -> Ptr DBT -> Ptr DBT -> Word32 -> IO CInt

foreign import ccall unsafe "hs_cursor_close"
  hs_cursor_close :: Ptr DBC -> IO CInt

closeCursor :: Ptr DBC -> IO ()
closeCursor ptr = do throwErrnoIf (/=0) "hs_cursor_close" $ hs_cursor_close ptr
                     return ()

newCursor :: Ptr DB -> IO (Ptr DBC)
newCursor db
    = alloca $ \dbcPtr -> do throwErrnoIf (/=0) "hs_cursor" $ hs_cursor db nullPtr dbcPtr 0
                             peek dbcPtr

getAtCursor :: Ptr DBC -> [DbFlag] -> IO (Maybe (Object,[Object]))
getAtCursor dbc flags
    = withDBT nullObject $ \keyDbt ->
      withDBT nullObject $ \dataDbt ->
      do clear_dbt keyDbt
         #{poke DBT, flags} keyDbt (#{const DB_DBT_MALLOC} :: Word32)
         objs <- dbtGetMany dataDbt $ hs_cursor_get dbc keyDbt dataDbt (fromDbFlags $ Multiple:flags)
         if null objs then return Nothing else do
         ptr <- #{peek DBT, data} keyDbt
         size <- #{peek DBT, size} keyDbt :: IO CInt
         keyObject <- B.unsafePackCStringFinalizer ptr (fromIntegral size) (free ptr)
         let pp = B.take 40 . Char8.filter isPrint
--         putStrLn $ "Key: " ++ show (pp keyObject) ++ ": " ++ show (map pp objs)
--         putStrLn $ "Key ptr: " ++ show ptr
--         putStrLn $ "  Objs: " ++ show (map (B.take 30 . Char8.filter isAlphaNum) objs)
         return (Just (keyObject,objs))

{-
getAllObjects :: Ptr DB -> IO [(Object,[Object])]
getAllObjects ptr
    = do dbc <- alloca $ \dbcPtr -> do throwErrnoIf (/=0) "hs_cursor" $ hs_cursor ptr nullPtr dbcPtr 0
                                       peek dbcPtr
         let flags = #{const DB_MULTIPLE} .|. #{const DB_NEXT}
         let loop = --unsafeInterleaveIO $
                    withDBT nullObject $ \keyDbt ->
                    withDBT nullObject $ \dataDbt ->
                    do clear_dbt keyDbt
                       #{poke DBT, flags} keyDbt (#{const DB_DBT_MALLOC} :: Word32)
                       objs <- dbtGetMany dataDbt $ hs_cursor_get dbc keyDbt dataDbt flags
                       if null objs then return [] else do
                       ptr <- #{peek DBT, data} keyDbt
                       size <- #{peek DBT, size} keyDbt :: IO CInt
                       keyObject <- B.unsafePackCStringFinalizer ptr (fromIntegral size) (free ptr)
                       let pp = B.take 40 . Char8.filter isPrint
--                       putStrLn $ "Key: " ++ show (pp keyObject) ++ ": " ++ show (map pp objs)
--                       putStrLn $ "Key ptr: " ++ show ptr
--                       putStrLn $ "  Objs: " ++ show (map (B.take 30 . Char8.filter isAlphaNum) objs)
                       rest <- loop
                       return ((keyObject,objs):rest)
         loop
-}

nullObject = B.fromForeignPtr B.nullForeignPtr 0 0

