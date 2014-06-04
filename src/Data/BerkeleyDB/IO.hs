{-# LANGUAGE ForeignFunctionInterface #-}
module Data.BerkeleyDB.IO
    ( new
    , insert
--    , Data.BerkeleyDB.IO.lookup
    , lookupMany
    , getAllObjects
    , serialise
    , Db(..)
    , module Data.BerkeleyDB.Internal
    ) where

import Data.Binary
import qualified Data.ByteString.Lazy as Lazy
import qualified Data.ByteString.Unsafe as Strict
import qualified Data.ByteString as Strict

import qualified Data.BerkeleyDB.Internal as D
import Data.BerkeleyDB.Internal (DbType(..),DbFlag(..))

import Foreign.C
import Foreign (ForeignPtr, Ptr, FunPtr, newForeignPtr, withForeignPtr, castPtr)
import Control.Monad
import System.IO.Unsafe
import Foreign.ForeignPtr

newtype Db key value = Db (ForeignPtr D.DB)

newtype Cursor key value = Cursor (Ptr D.DBC)

new :: (Binary key, Binary value) => DbType -> IO (Db key value)
new dbType
    = do --putStrLn "newdb"
         ptr <- D.open Nothing Nothing dbType [D.Create,D.Thread]
         liftM Db $ newForeignPtr D.closePtr ptr

insert :: (Binary key, Binary value) => Db key value -> key -> value -> IO ()
insert (Db db) key val
    = withForeignPtr db $ \dbPtr ->
      do D.put dbPtr (serialise key) (serialise val) []

{-
{-# INLINE lookup #-}
lookup :: (Binary key, Binary value) => Db key value -> key -> IO (Maybe value)
lookup (Db db) key
    = withForeignPtr db $ \dbPtr ->
      do mbObject <- D.get dbPtr (serialise key) []
         case mbObject of
           Just object -> return $ Just (deserialise object)
           Nothing     -> return Nothing
-}

{-# INLINE lookupMany #-}
lookupMany :: (Binary key, Binary value) => Db key value -> key -> IO [value]
lookupMany (Db db) key
    = withForeignPtr db $ \dbPtr ->
      do objects <- D.getMany dbPtr (serialise key) []
         return $ map deserialise objects

{-
setFlags :: Db key value -> [DbFlag] -> IO ()
setFlags (Db db) flags
    = withForeignPtr db $ \dbPtr ->
      D.setFlags dbPtr flags
-}

newCursor :: Db key value -> IO (Cursor key value)
newCursor (Db db)
    = withForeignPtr db $ \dbPtr ->
      do dbc <- D.newCursor dbPtr
         return $ Cursor dbc

getAtCursor :: (Binary key, Binary value) => Cursor key value -> IO (Maybe (key, [value]))
getAtCursor (Cursor ptr)
    = do ret <- D.getAtCursor ptr [Next]
         case ret of
           Nothing -> return Nothing
           Just (key, vals) -> return $ Just (deserialise key, map deserialise vals)

closeCursor :: Cursor key value -> IO ()
closeCursor (Cursor ptr)
    = D.closeCursor ptr

getAllObjects :: (Binary key, Binary value) => Db key value -> IO [(key,[value])]
getAllObjects db@(Db fptr)
    = do cursor <- newCursor db
         let loop = unsafeInterleaveIO $
                    do mbPair <- getAtCursor cursor
                       case mbPair of
                         Nothing   -> do closeCursor cursor
                                         touchForeignPtr fptr
                                         return []
                         Just pair -> liftM (pair:) loop
         loop

serialise :: Binary val => val -> D.Object
serialise val
    = case Lazy.toChunks (encode val) of
        [chunk] -> chunk
        ls      -> Strict.concat ls

deserialise :: Binary val => D.Object -> val
deserialise bs
    = decode $ Lazy.fromChunks [bs]
