{-# LANGUAGE CPP                #-}
{-# LANGUAGE DeriveDataTypeable #-}
-----------------------------------------------------------------------------
-- |
-- Module      :  Data.BerkeleyDB
-- Copyright   :  (c) David Himmelstrup 2008
-- License     :  BSD-style
-- Maintainer  :  lemmih@gmail.com
-- Stability   :  experimental
-- Portability :  non-portable (requires libDB)
--
-- An efficient implementation of maps from keys to values (dictionaries).
--
-- Since many function names (but not the type name) clash with
-- "Prelude" names, this module is usually imported @qualified@, e.g.
--
-- >  import Data.BerkeleyDB (DB)
-- >  import qualified Data.BerkeleyDB as DB
--
-- The implementation of 'Db' uses the berkeley db library. See
--    <http://en.wikipedia.org/wiki/Berkeley_DB> and
--    <http://www.oracle.com/technology/products/berkeley-db/index.html>
--
-- Note that this implementation behaves exactly like a @Data.Map.Map ByteString ByteString@,
-- with the key and value encoded by @Data.Binary.encode/Data.Binary.decode@.
-- This means that keys aren't sorted according to Ord. Affected functions are:
-- 'toList', 'assocs', 'elems'.
-----------------------------------------------------------------------------
module Data.BerkeleyDB
  ( -- * Db type
    Db

    -- * Operators
    , (!) --, (\\)

    -- * Query
  , null
  , size
  , member
  , notMember
  , lookup
  , findWithDefault

    -- * Construction
  , empty
  , singleton

    -- ** Insertion
  , insert
  , insertWith
  , insertWithKey

    -- ** Delete\/Update
  , delete
  , adjust
  , adjustWithKey
  , update
  , updateWithKey
  , updateLookupWithKey
  , alter

    -- * Combine
    -- ** Union
  , union
  , unionWith
  , unionWithKey
  , unions
  , unionsWith

    -- ** Difference
{-  , difference
  , differenceWith
  , differenceWithKey

    -- ** Intersection
  , intersection
  , intersectionWith
  , intersectionWithKey-}

    -- * Traversal
    -- ** Map
  , map
  , mapWithKey
{-  , mapAccum
  , mapAccumWithKey
  , mapKeys
  , mapKeysWith
  , mapKeysMonotonic-}

    -- ** Fold
  , fold
--  , foldWithKey

    -- * Conversion
  , elems
  , keys
--  , keysSet
  , assocs

    -- ** Lists
  , toList
  , fromList
  , fromListWith
  , fromListWithKey

    -- ** Ordered lists
{-  , toAscList
  , fromAscList
  , fromAscListWith
  , fromAscListWithKey
  , fromDistinctAscList-}

    -- * Filter
  , filter
  , filterWithKey
{-  , partition
  , partitionWithKey-}

{-  , mapMaybe
  , mapMaybeWithKey
  , mapEither
  , mapEitherWithKey

  , split
  , splitLookup-}

    -- * Submap
--  , isSubdbOf, isSubdbOfBy
--  , isProperSubdbOf, isProperSubdbOfBy

    -- * Indexed
    -- * Min\/Max
{-  , findMin
  , findMax
  , deleteMin
  , deleteMax
  , deleteFindMin
  , deleteFindMax
  , updateMin
  , updateMax
  , updateMinWithKey
  , updateMaxWithKey
  , minView
  , maxView
  , minViewWithKey
  , maxViewWithKey -}
  ) where

import           Data.Binary
import           Data.Monoid
#if __GLASGOW_HASKELL__
import           Data.Data
import           GHC.Generics             hiding (prec)
import           Text.Read                hiding (get)
#endif

import qualified Data.BerkeleyDB.Internal as Internal
import qualified Data.BerkeleyDB.IO       as IO

import qualified Data.ByteString          as Strict
import qualified Data.ByteString.Lazy     as Lazy

import           Control.Concurrent
import           Control.Monad            (forM_, liftM, mplus, replicateM)
import           Data.Binary
import           Data.IORef
import           Data.List                (foldl', sort)
import           Data.Maybe
import           Data.Typeable            (Typeable)
import           Foreign
import           System.IO.Unsafe

import           Prelude                  hiding (filter, lookup, map, null)
import qualified Prelude

data Db key value = Empty
                  | Db { ioDB    :: IO.Db key (Int,Maybe Internal.Object)
                       , range   :: ![Range]
                       , uniqGen :: {- UNPACK -} !(IORef Int)
                       , dbSize  :: !Int
                       }
    deriving (Typeable)
data Range = Range Int Int deriving Show

{--------------------------------------------------------------------
  Instances
--------------------------------------------------------------------}

instance (Show key, Show value, Binary key, Binary value) => Binary (Db key value) where
--    put = put . toList
    put Empty = put (0::Int)
    put (Db db range uniq size)
      = do let lst = unsafePerformIO $
                       withVar db $ \(IO.Db db) ->
                         withForeignPtr db $ \dbPtr ->
                           do cursor <- Internal.newCursor dbPtr
                              let loop = unsafeInterleaveIO $
                                         do mbPair <- Internal.getAtCursor cursor [Internal.Next]
                                            case mbPair of
                                              Nothing -> Internal.closeCursor cursor >> touchForeignPtr db >> return []
                                              Just pair -> liftM (pair:) loop
                              loop
               bss :: [(Internal.Object, Internal.Object)]
               bss = flip mapMaybe lst $ \(key,values) ->
                       do value <- findValue range (Prelude.map (decode.fromObject) values)
                          return (key, value)
           put size
           mapM_ put bss
--    get = fmap fromList get
    get = do n <- get
             bss <- replicateM n get :: Get [(Internal.Object, Internal.Object)]
             unsafePerformIO $
               do db <- IO.new IO.BTree
                  let IO.Db dbForeign = db
                  withForeignPtr dbForeign $ \dbPtr -> forM_ bss $ \(key,value) -> Internal.put dbPtr key (toObject $ encode $ (0::Int,Just value)) []
                  uniq <- newIORef 1
                  return $ return $ Db db (addToRange 0 []) uniq n

instance (Binary key, Binary value, Show key, Show value) => Show (Db key value) where
    showsPrec d m  = showParen (d > 10) $
                     showString "fromList " . shows (toList m)

instance (Binary k, Binary a, Read k, Read a) => Read (Db k a) where
#ifdef __GLASGOW_HASKELL__
  readPrec = parens $ prec 10 $ do
    Ident "fromList" <- lexP
    xs <- readPrec
    return (fromList xs)

  readListPrec = readListPrecDefault
#else
  readsPrec p = readParen (p > 10) $ \ r -> do
    ("fromList",s) <- lex r
    (xs,t) <- reads s
    return (fromList xs,t)
#endif


instance (Binary key, Binary value, Eq key, Eq value) => Eq (Db key value) where
    db1 == db2 = size db1 == size db2 && toList db1 == toList db2

instance (Binary key, Binary value, Ord key, Ord value) => Ord (Db key value) where
    db1 `compare` db2 = sort (toList db1) `compare` sort (toList db2)

instance (Binary k, Binary a) => Monoid (Db k a) where
    mempty = empty
    mappend = union
    mconcat = unions

#if __GLASGOW_HASKELL__
{--------------------------------------------------------------------
  A Data instance
--------------------------------------------------------------------}

-- This instance preserves data abstraction at the cost of inefficiency.
-- We omit reflection services for the sake of data abstraction.

instance (Data k, Data a, Binary k, Binary a) => Data (Db k a) where
  gfoldl f z map = z fromList `f` (toList map)
  toConstr _     = error "toConstr"
  gunfold _ _    = error "gunfold"
  dataTypeOf _   = mkNoRepType "Data.BerkeleyDB.Db"
  dataCast2 f    = gcast2 f

#endif



{--------------------------------------------------------------------
  Methods
--------------------------------------------------------------------}

-- | /O(log n)/. Find the value at a key.
-- Calls 'error' when the element can not be found.
(!) :: (Binary k, Binary v) => Db k v -> k -> v
db ! k = case lookup k db of
           Nothing -> error "Data.BerkeleyDB.!: element not in the database"
           Just x  -> x

-- | /O(1)/. The empty database.
empty :: (Binary key, Binary value) => Db key value
empty = Empty

-- | /O(1)/. A map with a single element.
singleton :: (Binary k, Binary a) => k -> a -> Db k a
singleton k a = insert k a empty

-- | /O(log n)/. Insert a new key and value in the database.
-- If the key is already present in the database, the associated value is
-- replaced with the supplied value, i.e. 'insert' is equivalent to
-- @'insertWith' 'const'@.
insert :: (Binary key, Binary value) => key -> value -> Db key value -> Db key value
insert key val db
    = unsafePerformIO $
      withDB db $ \(Db db range uniq size) ->
      do myUniq <- atomicModifyIORef uniq (\a -> (a+1,a))
         withVar db $ \ioDB ->
           do exist <- fmap isJust $ lookupPrim key ioDB range
              IO.insert ioDB key (myUniq, Just $ toObject $ encode val)
              return $ Db db (addToRange myUniq range) uniq (if exist then size else size+1)

-- | /O(log n)/. Insert with a combining function.
-- @'insertWith' f key value db@
-- will insert the pair (key, value) into @db@ if key does
-- not exist in the database. If the key does exist, the function will
-- insert the pair @(key, f new_value old_value)@.
insertWith :: (Binary k, Binary a) => (a -> a -> a) -> k -> a -> Db k a -> Db k a
insertWith fn key val db
    = insertWithKey (\k x y -> fn x y) key val db

-- | /O(log n)/. Insert with a combining function.
-- @'insertWithKey' f key value db@
-- will insert the pair (key, value) into @db@ if key does
-- not exist in the database. If the key does exist, the function will
-- insert the pair @(key,f key new_value old_value)@.
-- Note that the key passed to f is the same key passed to 'insertWithKey'.
insertWithKey :: (Binary k, Binary a) => (k -> a -> a -> a) -> k -> a -> Db k a -> Db k a
insertWithKey fn key val db
    = unsafePerformIO $
      withDB db $ \(Db db range uniq size) ->
      do myUniq <- atomicModifyIORef uniq (\a -> (a+1,a))
         withVar db $ \ioDB ->
           do mbOldValue <- lookupPrim key ioDB range
              case mbOldValue of
                Nothing -> do IO.insert ioDB key (myUniq, Just $ toObject $ encode val)
                              return $ Db db (addToRange myUniq range) uniq (size+1)
                Just oldValue -> do let newvalue = fn key val (decode (fromObject oldValue))
--                                    putStrLn $ "oldValue: " ++ show oldValue
                                    IO.insert ioDB key (myUniq, Just $ toObject $ encode newvalue)
                                    return $ Db db (addToRange myUniq range) uniq size



-- | /O(log n)/. Lookup the value at a key in the database.
--
-- The function will
-- @return@ the result in the monad or @fail@ in it the key isn't in the
-- database. Often, the monad to use is 'Maybe', so you get either
-- @('Just' result)@ or @'Nothing'@.
lookup :: (Binary key, Binary value, Monad m) => key -> Db key value -> m value
lookup key Empty = fail "Data.BerkeleyDB.lookup: Key not found"
lookup key (Db db range uniq size)
    = unsafePerformIO $
      do withVar db $ \db -> do mbValue <- lookupPrim key db range
                                case mbValue of
                                  Nothing    -> return $ fail "Data.BerkeleyDB.lookup: Key not found"
                                  Just value -> return $ return (decode (fromObject value))

-- | /O(log n)/. The expression @('findWithDefault' def k db)@ returns
-- the value at key @k@ or returns @def@ when the key is not in the database.
findWithDefault :: (Binary k, Binary a) => a -> k -> Db k a -> a
findWithDefault def k db
    = case lookup k db of
        Nothing -> def
        Just x  -> x

-- | /O(log n)/. Is the key a member of the map?
member :: (Binary key, Binary value) => key -> Db key value -> Bool
member key db
    = isJust (lookup key db)

-- | /O(log n)/. Is the key not a member of the map?
notMember :: (Binary key, Binary value) => key -> Db key value -> Bool
notMember k m = not $ member k m


--lookupPrim :: (Binary key, Binary value) => key -> IO.Db key value -> [Range] -> IO (Maybe value)
lookupPrim key db range
    = do rets <- IO.lookupMany db key
         return $ findValue range rets

-- | /O(log n*m)/.
-- The expression (@'union' t1 t2@) takes the left-biased union of @t1@ and @t2@.
-- It prefers @t1@ when duplicate keys are encountered,
-- i.e. (@'union' == 'unionWith' 'const'@).
union :: (Binary key, Binary value) => Db key value -> Db key value -> Db key value
union t1 t2
    = unionWith const t1 t2

-- | /O(log n*m)/. Union with a combining function.
unionWith :: (Binary key, Binary value) => (value -> value -> value) -> Db key value -> Db key value -> Db key value
unionWith fn t1 t2
    = unionWithKey (\k x y -> fn x y) t1 t2

-- | /O(log n*m))/.
-- Union with a combining function. This function is most efficient on (bigset `union` smallset).
unionWithKey :: (Binary key, Binary value) => (key -> value -> value -> value) -> Db key value -> Db key value -> Db key value
unionWithKey fn t1 t2
    = foldl' (\db (k,v) -> insertWith (flip (fn k)) k v db) t1 (toList t2)

-- | The union of a list of databases:
--   (@'unions' == 'Prelude.foldl' 'union' 'empty'@).
unions :: (Binary k, Binary a) => [Db k a] -> Db k a
unions = foldl' union empty

-- | The union of a list of databases, with a combining operation:
--   (@'unionsWith' f == 'Prelude.foldl' ('unionWith' f) 'empty'@).
unionsWith :: (Binary k, Binary a) => (a -> a -> a) -> [Db k a] -> Db k a
unionsWith fn = foldl' (unionWith fn) empty

-- | /O(n)/. Fold the values in the map, such that
-- @'fold' f z == 'Prelude.foldr' f z . 'elems'@.
-- For example,
--
-- > elems map = fold (:) [] map
--
fold :: (Binary k, Binary a) => (a -> b -> b) -> b -> Db k a -> b
fold f z m
    = Prelude.foldr f z (elems m)

-- | /O(log n)/. Delete a key and its value from the database. When the key is not
-- a member of the database, the original database is returned.
delete :: (Binary key, Binary value) => key -> Db key value -> Db key value
delete key Empty = Empty
delete key (Db db range uniq size)
    = unsafePerformIO $
      do myUniq <- atomicModifyIORef uniq (\a -> (a+1,a))
         withVar db $ \ioDB ->
           do exist <- fmap isJust $ lookupPrim key ioDB range
              IO.insert ioDB key (myUniq, Nothing)
              return $ Db db (addToRange myUniq range) uniq (if exist then size-1 else size)

-- | /O(log n)/. Adjust a value at a specific key. When the key is not
-- a member of the map, the original map is returned.
adjust :: (Binary k, Binary a) => (a -> a) -> k -> Db k a -> Db k a
adjust fn k db = adjustWithKey (\k x -> fn x) k db

-- | /O(log n)/. Adjust a value at a specific key. When the key is not
-- a member of the map, the original map is returned.
adjustWithKey :: (Binary k, Binary a) => (k -> a -> a) -> k -> Db k a -> Db k a
adjustWithKey fn k db = updateWithKey (\k v -> Just (fn k v)) k db

-- | /O(log n)/. The expression (@'update' f k map@) updates the value @x@
-- at @k@ (if it is in the map). If (@f x@) is 'Nothing', the element is
-- deleted. If it is (@'Just' y@), the key @k@ is bound to the new value @y@.
update :: (Binary k, Binary a) => (a -> Maybe a) -> k -> Db k a -> Db k a
update fn k db = updateWithKey (\k x -> fn x) k db

-- | /O(log n)/. The expression (@'updateWithKey' f k db@) updates the
-- value @x@ at @k@ (if it is in the database). If (@f k x@) is 'Nothing',
-- the element is deleted. If it is (@'Just' y@), the key @k@ is bound
-- to the new value @y@.
updateWithKey :: (Binary k, Binary a) => (k -> a -> Maybe a) -> k -> Db k a -> Db k a
updateWithKey fn key db = snd (updateLookupWithKey fn key db)

-- | /O(log n)/. The expression (@'updateWithKey' f k db@) updates the
-- value @x@ at @k@ (if it is in the database). If (@f k x@) is 'Nothing',
-- the element is deleted. If it is (@'Just' y@), the key @k@ is bound
-- to the new value @y@.
updateLookupWithKey :: (Binary k, Binary a) => (k -> a -> Maybe a) -> k -> Db k a -> (Maybe a, Db k a)
updateLookupWithKey _ _ Empty = (Nothing, Empty)
updateLookupWithKey fn key orig@(Db db range uniq size)
    = unsafePerformIO $
      withVar db $ \ioDB ->
      do mbVal <- lookupPrim key ioDB range
         case mbVal of
           Nothing  -> return (Nothing, orig)
           Just val -> do myUniq <- atomicModifyIORef uniq (\a -> (a+1,a))
                          let oldval = decode (fromObject val)
                              newval = fn key oldval
                          IO.insert ioDB key (myUniq, fmap (toObject.encode) newval)
                          return (newval `mplus` Just oldval, Db db (addToRange myUniq range) uniq (if isJust newval then size else size-1))

-- FIXME: Use a cursor to avoid two lookups.
-- | /O(log n)/. The expression (@'alter' f k db@) alters the value @x@ at @k@, or absence thereof.
-- 'alter' can be used to insert, delete, or update a value in a 'Db'.
-- In short : @'lookup' k ('alter' f k m) = f ('lookup' k m)@
alter :: (Binary k, Binary a) => (Maybe a -> Maybe a) -> k -> Db k a -> Db k a
alter f k db
    = case f (lookup k db) of
        Nothing -> delete k db
        Just x' -> insert k x' db

-- | /O(n)/. Convert to a list of key\/value pairs.
toList :: (Binary key, Binary value) => Db key value -> [(key, value)]
toList = unsafePerformIO . toListIO

toListIO :: (Binary key, Binary value) => Db key value -> IO [(key, value)]
toListIO Empty = return []
toListIO (Db db range uniq size)
    = do -- putStrLn "Initiating toList"
         assocs <- withVar db $ \db -> IO.getAllObjects db
         let real = [ (key, decode $ fromObject value) | (key, values) <- assocs, Just value <- [findValue range values]]
         --putStrLn $ "  Elements: " ++ show (length real)
         return real

-- | /O(n)/. Return all key\/value pairs in the map in ascending key order.
assocs :: (Binary key, Binary value) => Db key value -> [(key,value)]
assocs m
  = toList m


-- | /O(n*log n)/. Build a database from a list of key\/value pairs. See also 'fromAscList'.
fromList :: (Binary key, Binary value) => [(key, value)] -> Db key value
fromList = fromListWith const
{-
    = unsafePerformIO $
      do db <- IO.new IO.BTree
         forM_ lst $ \(key,value) -> IO.insert db key (0, Just $ toObject $ encode value)
         uniq <- newIORef 1
         var <- newMVar db
         return $ Db var [Range 0 0] uniq
-}

-- | /O(n*log n)/. Build a database from a list of key\/value pairs with a combining function.
fromListWith :: (Binary k, Binary a) => (a -> a -> a) -> [(k,a)] -> Db k a
fromListWith fn = fromListWithKey (\k -> fn)

-- | /O(n*log n)/. Build a database from a list of key\/value pairs with a combining function.
fromListWithKey :: (Binary k, Binary a) => (k -> a -> a -> a) -> [(k,a)] -> Db k a
fromListWithKey fn = foldl' (\db (k,v) -> insertWithKey fn k v db) empty


-- | /O(n)/.
-- Return all elements of the database in the ascending order of their keys
-- sorted by their binary representation.
elems :: (Binary key, Binary value) => Db key value -> [value]
elems = Prelude.map snd . toList

-- | /O(n)/. Return all keys of the database in ascending order
-- sorted by their binary representation.
keys :: (Binary key, Binary value) => Db key value -> [key]
keys = Prelude.map fst . toList

-- | /O(1)/. Is the map empty?
null :: Db key value -> Bool
null db = size db == 0

-- | /O(1)/. The number of elements in the map.
size :: Db key value -> Int
size Empty = 0
size (Db{dbSize=s}) = s

-- | /O(n)/. Map a function over all values in the database.
map :: (Binary a, Binary b,Binary k) => (a -> b) -> Db k a -> Db k b
map fn db = mapWithKey (\_key val -> fn val) db

-- | /O(n)/. Map a function over all values in the database.
mapWithKey :: (Binary a, Binary b,Binary k) => (k -> a -> b) -> Db k a -> Db k b
mapWithKey fn db
    = unsafePerformIO $
      do let lst = toList db
         newDb <- IO.new IO.BTree
         uniq <- newIORef 1
         forM_ lst $ \(key,value) -> IO.insert newDb key (0, Just $ toObject $ encode $ fn key value)
         return $ Db newDb (addToRange 0 []) uniq (size db)

-- | /O(n)/. Filter all values that satisfy the predicate.
filter :: (Binary k, Binary a) => (a -> Bool) -> Db k a -> Db k a
filter p m
  = filterWithKey (\k x -> p x) m

-- | /O(n)/. Filter all keys\/values that satisfy the predicate.
filterWithKey :: (Binary k, Binary a) => (k -> a -> Bool) -> Db k a -> Db k a
filterWithKey p Empty = Empty
filterWithKey p orig@(Db db range uniq size)
    = unsafePerformIO $
      do myUniq <- atomicModifyIORef uniq (\a -> (a+1,a))
         let loop n [] = return $ Db db (addToRange myUniq range) uniq (size-n)
             loop n ((key,val):rs)
               | p key val = loop n rs
               | otherwise = do withVar db $ \dbIO -> IO.insert dbIO key (myUniq, Nothing)
                                loop (n+1) rs
         loop 0 =<< toListIO orig


{--------------------------------------------------------------------
  Utilities
--------------------------------------------------------------------}


toObject lbs = Strict.concat (Lazy.toChunks lbs)

fromObject bs = Lazy.fromChunks [bs]

withVar var fn = fn var

withDB Empty fn
    = do db <- do db <- IO.new IO.BTree
                  ref <- newIORef 0
                  return $ Db db [] ref 0
         fn db
withDB db fn = fn db


findValue range [] = Nothing
findValue range ((uniqId, value):rs)
    | uniqId `isInRange` range = value
    | otherwise = findValue range rs


isInRange :: Int -> [Range] -> Bool
isInRange i [] = False
isInRange i (Range x y:rs)
    | i > x = False
    | i < y = isInRange i rs
    | otherwise = True

addToRange :: Int -> [Range] -> [Range]
addToRange i [] = [Range i i]
addToRange i (Range x y:rs)
    = merge (Range i i:Range x y:rs)

merge [] = []
merge [x] = [x]
merge (Range x y:Range a b:rs)
    | y == a+1    = merge (Range x b:rs)
    | otherwise = Range x y:merge (Range a b:rs)



