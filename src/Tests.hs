module Main (main) where

import           Data.Binary
import           Data.Binary.Get
import           Data.List
import           Test.QuickCheck
import           Test.QuickCheck.All
import           Test.QuickCheck.Gen
import           Test.QuickCheck.State

import qualified Data.ByteString       as Strict

import qualified Data.BerkeleyDB       as Pure
import qualified Data.Map              as Map

import           Control.Concurrent    (yield)
import           Debug.Trace
import           System.IO
import           System.Mem            (performGC)

data T = T (Pure.Db Int Int) (Map.Map Int Int)
instance Show T where
    show (T db _) = show (Pure.toList db)

newtype Db = Db { unDb :: Pure.Db Int Int }
instance Show Db where
    show (Db db) = show (Pure.toList db)

instance Arbitrary T where
    arbitrary = do lst <- arbitrary
                   snd <- arbitrary
                   return $ T (foldr Pure.delete (Pure.fromList lst) snd) (foldr Map.delete (Map.fromList lst) snd)
    coarbitrary = undefined

instance Arbitrary Db where
    arbitrary = do lst <- arbitrary
                   snd <- arbitrary
                   return $ Db (foldl' (\db k -> Pure.delete k db) (Pure.fromList lst) snd)
    coarbitrary = undefined

instance (Arbitrary key, Arbitrary value,Binary key,Binary value) => Arbitrary (Pure.Db key value) where
    arbitrary = fmap Pure.fromList arbitrary
    coarbitrary = undefined


eq db m = if sort (Pure.toList db) == sort (Map.toList m) && Pure.size db == Map.size m
          then True
          else False --trace (show $ Pure.toList db) False



prop_fromList vals = Pure.fromList vals `eq` Map.fromList (vals :: [(Int,Int)])

prop_fromList_fold vals = Pure.fromList vals == foldl' (\db (k,v) -> Pure.insert k v db) Pure.empty (vals::[(Int,Int)])

prop_null (T db m) = Pure.null db == Map.null m
prop_size (T db m) = Pure.size db == Map.size m

prop_insert (T db m) key val = Pure.insert key val db `eq` Map.insert key val m

prop_delete (T db m) key = Pure.delete key db `eq` Map.delete key m

prop_map (T db m) = Pure.map succ db `eq` Map.map succ m

prop_mapWithKey (T db m) = Pure.mapWithKey (+) db `eq` Map.mapWithKey (+) m

prop_insertWith (T db m) key val = Pure.insertWith (+) key val db `eq` Map.insertWith (+) key val m

prop_insertWithKey (T db m) key val = Pure.insertWithKey (\k x y -> k) key val db `eq` Map.insertWithKey (\k x y -> k) key val m

prop_singleton key val = Pure.singleton key val `eq` Map.singleton (key::Int) (val::Int)

prop_lookup (T db m) key = Pure.lookup key db == (Map.lookup key m :: Maybe Int)

prop_find (T db m) key = Pure.member key db ==> (db Pure.! key) == (m Map.! key)

prop_member (T db m) key = Pure.member key db == Map.member key m
prop_notMember (T db m) key = Pure.notMember key db == Map.notMember key m

prop_assocs_self (Db db) = Pure.assocs db == Pure.toList db
prop_assocs (T db m) = sort (Pure.assocs db) == sort (Map.assocs m)

prop_elems (T db m) = sort (Pure.elems db) == sort (Map.elems m)

prop_keys (T db m) = sort (Pure.keys db) == sort (Map.keys m)

-- The result of 'show' should be the same if we have the same key ordering.
prop_show (T db m) = show (Pure.toList db) == show (Map.toList m) ==> show db == show m

prop_diverge (T db m) keys lst
    = foldr Pure.delete db keys `eq` foldr Map.delete m keys &&
      foldr (uncurry Pure.insert) db lst `eq` foldr (uncurry Map.insert) m lst

prop_diverge_insert (T db m) lst1 lst2
    = foldr (uncurry Pure.insert) db lst1 `eq` foldr (uncurry Map.insert) m lst1 &&
      foldr (uncurry Pure.insert) db lst2 `eq` foldr (uncurry Map.insert) m lst2

prop_fold_sum (T db m)
    = Pure.fold (+) 0 db == Map.fold (+) 0 m

prop_union (T db1 m1) (T db2 m2)
    = Pure.union db1 db2 `eq` Map.union m1 m2

prop_unionWith (T db1 m1) (T db2 m2)
    = Pure.unionWith (+) db1 db2 `eq` Map.unionWith (+) m1 m2

prop_unionWithKey (T db1 m1) (T db2 m2)
    = Pure.unionWithKey (\k x y -> k+x+y) db1 db2 `eq` Map.unionWithKey (\k x y -> k+x+y) m1 m2

prop_filter (T db m) cutoff
    = Pure.filter (>cutoff) db `eq` Map.filter (>cutoff) m

-- Mostly false predicate
prop_filterWithKey (T db m)
    = Pure.filterWithKey (\k x -> k==x) db `eq` Map.filterWithKey (\k x -> k==x) m

-- Mostly true predicate
prop_filterWithKey2 (T db m)
    = Pure.filterWithKey (\k x -> k/=x) db `eq` Map.filterWithKey (\k x -> k/=x) m

prop_adjust (T db m) key
    = Pure.adjust succ key db `eq` Map.adjust succ key m

prop_adjustWithKey (T db m) key
    = Pure.adjustWithKey (+) key db `eq` Map.adjustWithKey (+) key m

prop_update (T db m) key
    = Pure.update (const Nothing) key db `eq` Map.update (const Nothing) key m

prop_update_succ (T db m) key
    = Pure.update (Just . succ) key db `eq` Map.update (Just . succ) key m

prop_updateLookup (T db m) key
    = let (v1,db') = Pure.updateLookupWithKey (\k x -> Nothing) key db
          (v2,m')  = Map.updateLookupWithKey (\k x -> Nothing) key m
      in db' `eq` m' && v1==v2

prop_updateLookup2 (T db m) key
    = let (v1,db') = Pure.updateLookupWithKey (\k x -> Just k) key db
          (v2,m')  = Map.updateLookupWithKey (\k x -> Just k) key m
      in db' `eq` m' && v1==v2

prop_compare (T db1 m1) (T db2 m2)
    = db1 `compare` db2 == m1 `compare` m2

prop_alter (T db m) key val
    = Pure.alter (fmap (+val)) key db `eq` Map.alter (fmap (+val)) key m

prop_find_def (T db m) key def
    = Pure.findWithDefault def key db == Map.findWithDefault def key m

prop_fromListWithKey lst
    = Pure.fromListWithKey (\k x y -> k+x+y) lst `eq` Map.fromListWithKey (\k x y -> k+x+y) (lst :: [(Int,Int)])

mapConformity
    = runTests "Data.Map conformity" defOpt
        [ run prop_fromList
        , run prop_fromList_fold
        , run prop_null
        , run prop_size
        , run prop_insert
        , run prop_insertWith
        , run prop_insertWithKey
        , run prop_delete
        , run prop_map
        , run prop_mapWithKey
        , run prop_singleton
        , run prop_lookup
        , run prop_find
        , run prop_member
        , run prop_notMember
        , run prop_assocs_self
        , run prop_assocs
        , run prop_elems
        , run prop_keys
        , run prop_show
        , run prop_diverge
        , run prop_diverge_insert
        , run prop_fold_sum
        , run prop_union
        , run prop_unionWith
        , run prop_unionWithKey
        , run prop_filter
        , run prop_filterWithKey
        , run prop_filterWithKey2
        , run prop_adjust
        , run prop_adjustWithKey
        , run prop_update
        , run prop_update_succ
        , run prop_updateLookup
        , run prop_updateLookup2
        , run prop_compare
        , run prop_alter
        , run prop_find_def
        , run prop_fromListWithKey
        ]

encodings
    = runTests "Encodings" defOpt
        [ run prop_binary_id
        , run prop_binary_use
        , run prop_binary_extra
        , run prop_readShow_id
        , run prop_readShow_use
        , run prop_readShow_extra
        , run prop_readShow_list
        ]

prop_binary_id (Db db) = decode (encode db) == db
prop_binary_use (T db m) key val = Pure.insert key val (decode (encode db)) `eq` Map.insert key val m
prop_binary_extra (Db db) = decode (encode (db,"string")) == (db,"string")

prop_readShow_id (Db db) = read (show db) == db
prop_readShow_use (T db m) key val = Pure.insert key val (read (show db)) `eq` Map.insert key val m
prop_readShow_extra (Db db) = read (show (db,"string")) == (db,"string")
prop_readShow_list dbList = read (show (map unDb dbList)) == map unDb dbList

--gcProp = \_ -> performGC >> yield >> return (TestOk "" 0 [])
--withGC lst = intersperse gcProp lst


runAllTests = sequence_ [mapConformity, encodings]

main :: IO ()
main = do hSetBuffering stdout NoBuffering
          runAllTests
          --check defaultConfig{configMaxTest=2000} prop_fromList
