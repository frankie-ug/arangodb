////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Jan Christoph Uhde
////////////////////////////////////////////////////////////////////////////////

#include "RocksDBCollection.h"
#include "Aql/PlanCache.h"
#include "Basics/ReadLocker.h"
#include "Basics/Result.h"
#include "Basics/StaticStrings.h"
#include "Basics/StringUtils.h"
#include "Basics/VelocyPackHelper.h"
#include "Basics/WriteLocker.h"
#include "Cache/CacheManagerFeature.h"
#include "Cache/Common.h"
#include "Cache/Manager.h"
#include "Cache/TransactionalCache.h"
#include "Cluster/ClusterMethods.h"
#include "Indexes/Index.h"
#include "Indexes/IndexIterator.h"
#include "RestServer/DatabaseFeature.h"
#include "RocksDBEngine/RocksDBBuilderIndex.h"
#include "RocksDBEngine/RocksDBCommon.h"
#include "RocksDBEngine/RocksDBComparator.h"
#include "RocksDBEngine/RocksDBEngine.h"
#include "RocksDBEngine/RocksDBIterators.h"
#include "RocksDBEngine/RocksDBKey.h"
#include "RocksDBEngine/RocksDBLogValue.h"
#include "RocksDBEngine/RocksDBMethods.h"
#include "RocksDBEngine/RocksDBPrimaryIndex.h"
#include "RocksDBEngine/RocksDBSettingsManager.h"
#include "RocksDBEngine/RocksDBTransactionCollection.h"
#include "RocksDBEngine/RocksDBTransactionState.h"
#include "StorageEngine/EngineSelectorFeature.h"
#include "StorageEngine/StorageEngine.h"
#include "Transaction/Helpers.h"
#include "Utils/CollectionNameResolver.h"
#include "Utils/Events.h"
#include "Utils/OperationOptions.h"
#include "VocBase/KeyGenerator.h"
#include "VocBase/LocalDocumentId.h"
#include "VocBase/LogicalCollection.h"
#include "VocBase/ManagedDocumentResult.h"
#include "VocBase/ticks.h"
#include "VocBase/voc-types.h"

#include <rocksdb/utilities/transaction.h>
#include <rocksdb/utilities/transaction_db.h>
#include <velocypack/Iterator.h>
#include <velocypack/velocypack-aliases.h>

using namespace arangodb;

RocksDBCollection::RocksDBCollection(LogicalCollection& collection,
                                     arangodb::velocypack::Slice const& info)
    : PhysicalCollection(collection, info),
      _objectId(basics::VelocyPackHelper::stringUInt64(info, "objectId")),
      _numberDocuments(0),
      _revisionId(0),
      _primaryIndex(nullptr),
      _cache(nullptr),
      _cachePresent(false),
      _cacheEnabled(
          !collection.system() &&
          basics::VelocyPackHelper::readBooleanValue(info, "cacheEnabled", false) &&
          CacheManagerFeature::MANAGER != nullptr),
      _numIndexCreations(0) {
  TRI_ASSERT(!ServerState::instance()->isCoordinator());
  VPackSlice s = info.get("isVolatile");
  if (s.isBoolean() && s.getBoolean()) {
    THROW_ARANGO_EXCEPTION_MESSAGE(
        TRI_ERROR_BAD_PARAMETER,
        "volatile collections are unsupported in the RocksDB engine");
  }

  TRI_ASSERT(_logicalCollection.isAStub() || _objectId != 0);
  rocksutils::globalRocksEngine()->addCollectionMapping(
      _objectId, _logicalCollection.vocbase().id(), _logicalCollection.id());

  if (_cacheEnabled) {
    createCache();
  }
}

RocksDBCollection::RocksDBCollection(LogicalCollection& collection,
                                     PhysicalCollection const* physical)
    : PhysicalCollection(collection, VPackSlice::emptyObjectSlice()),
      _objectId(static_cast<RocksDBCollection const*>(physical)->_objectId),
      _numberDocuments(0),
      _revisionId(0),
      _primaryIndex(nullptr),
      _cache(nullptr),
      _cachePresent(false),
      _cacheEnabled(static_cast<RocksDBCollection const*>(physical)->_cacheEnabled &&
                    CacheManagerFeature::MANAGER != nullptr),
      _numIndexCreations(0) {
  TRI_ASSERT(!ServerState::instance()->isCoordinator());
  rocksutils::globalRocksEngine()->addCollectionMapping(
      _objectId, _logicalCollection.vocbase().id(), _logicalCollection.id());

  if (_cacheEnabled) {
    createCache();
  }
}

RocksDBCollection::~RocksDBCollection() {
  if (useCache()) {
    try {
      destroyCache();
    } catch (...) {
    }
  }
}

std::string const& RocksDBCollection::path() const {
  return StaticStrings::Empty;  // we do not have any path
}

void RocksDBCollection::setPath(std::string const&) {
  // we do not have any path
}

Result RocksDBCollection::updateProperties(VPackSlice const& slice, bool doSync) {
  auto isSys = _logicalCollection.system();

  _cacheEnabled =
      !isSys &&
      basics::VelocyPackHelper::readBooleanValue(slice, "cacheEnabled", _cacheEnabled) &&
      CacheManagerFeature::MANAGER != nullptr;
  primaryIndex()->setCacheEnabled(_cacheEnabled);

  if (_cacheEnabled) {
    createCache();
    primaryIndex()->createCache();
  } else {
    // will do nothing if cache is not present
    destroyCache();
    primaryIndex()->destroyCache();
    TRI_ASSERT(_cache.get() == nullptr);
  }

  // nothing else to do
  return TRI_ERROR_NO_ERROR;
}

arangodb::Result RocksDBCollection::persistProperties() {
  // only code path calling this causes these properties to be
  // already written in RocksDBEngine::changeCollection()
  return Result();
}

PhysicalCollection* RocksDBCollection::clone(LogicalCollection& logical) const {
  return new RocksDBCollection(logical, this);
}

/// @brief export properties
void RocksDBCollection::getPropertiesVPack(velocypack::Builder& result) const {
  TRI_ASSERT(result.isOpenObject());
  result.add("objectId", VPackValue(std::to_string(_objectId)));
  result.add("cacheEnabled", VPackValue(_cacheEnabled));
  TRI_ASSERT(result.isOpenObject());
}

/// @brief closes an open collection
int RocksDBCollection::close() {
  READ_LOCKER(guard, _indexesLock);
  for (auto it : _indexes) {
    it->unload();
  }
  return TRI_ERROR_NO_ERROR;
}

void RocksDBCollection::load() {
  if (_cacheEnabled) {
    createCache();
    if (_cachePresent) {
      uint64_t numDocs = numberDocuments();
      if (numDocs > 0) {
        _cache->sizeHint(static_cast<uint64_t>(0.3 * numDocs));
      }
    }
  }
  READ_LOCKER(guard, _indexesLock);
  for (auto it : _indexes) {
    it->load();
  }
}

void RocksDBCollection::unload() {
  if (useCache()) {
    destroyCache();
    TRI_ASSERT(!_cachePresent);
  }
  READ_LOCKER(guard, _indexesLock);
  for (auto it : _indexes) {
    it->unload();
  }
}

TRI_voc_rid_t RocksDBCollection::revision() const { return _revisionId; }

TRI_voc_rid_t RocksDBCollection::revision(transaction::Methods* trx) const {
  auto state = RocksDBTransactionState::toState(trx);
  auto trxCollection = static_cast<RocksDBTransactionCollection*>(
      state->findCollection(_logicalCollection.id()));

  TRI_ASSERT(trxCollection != nullptr);

  return trxCollection->revision();
}

uint64_t RocksDBCollection::numberDocuments() const { return _numberDocuments; }

uint64_t RocksDBCollection::numberDocuments(transaction::Methods* trx) const {
  TRI_ASSERT(!ServerState::instance()->isCoordinator());
  auto state = RocksDBTransactionState::toState(trx);
  auto trxCollection = static_cast<RocksDBTransactionCollection*>(
      state->findCollection(_logicalCollection.id()));

  TRI_ASSERT(trxCollection != nullptr);

  return trxCollection->numberDocuments();
}

/// @brief report extra memory used by indexes etc.
size_t RocksDBCollection::memory() const { return 0; }

void RocksDBCollection::open(bool /*ignoreErrors*/) {
  TRI_ASSERT(_objectId != 0);
  RocksDBEngine* engine = static_cast<RocksDBEngine*>(EngineSelectorFeature::ENGINE);
  TRI_ASSERT(engine != nullptr);
  if (!engine->inRecovery()) {
    loadInitialNumberDocuments();
  }
}

void RocksDBCollection::prepareIndexes(arangodb::velocypack::Slice indexesSlice) {
  TRI_ASSERT(indexesSlice.isArray());

  StorageEngine* engine = EngineSelectorFeature::ENGINE;
  std::vector<std::shared_ptr<Index>> indexes;
  {
    READ_LOCKER(guard, _indexesLock);  // link creation needs read-lock too
    if (indexesSlice.length() == 0 && _indexes.empty()) {
      engine->indexFactory().fillSystemIndexes(_logicalCollection, indexes);
    } else {
      engine->indexFactory().prepareIndexes(_logicalCollection, indexesSlice, indexes);
    }
  }

  WRITE_LOCKER(guard, _indexesLock);
  TRI_ASSERT(_indexes.empty());
  for (std::shared_ptr<Index>& idx : indexes) {
    auto const id = idx->id();
    for (auto const& it : _indexes) {
      if (it->id() == id) {  // index is there twice
        idx.reset();
      }
    }

    if (idx) {
      TRI_UpdateTickServer(static_cast<TRI_voc_tick_t>(id));
      _indexes.emplace_back(idx);
      if (idx->type() == Index::TRI_IDX_TYPE_PRIMARY_INDEX) {
        TRI_ASSERT(idx->id() == 0);
        _primaryIndex = static_cast<RocksDBPrimaryIndex*>(idx.get());
      }
    }
  }

  if (_indexes[0]->type() != Index::IndexType::TRI_IDX_TYPE_PRIMARY_INDEX ||
      (TRI_COL_TYPE_EDGE == _logicalCollection.type() &&
       (_indexes[1]->type() != Index::IndexType::TRI_IDX_TYPE_EDGE_INDEX ||
        _indexes[2]->type() != Index::IndexType::TRI_IDX_TYPE_EDGE_INDEX))) {
    std::string msg =
        "got invalid indexes for collection '" + _logicalCollection.name() + "'";
    LOG_TOPIC(ERR, arangodb::Logger::ENGINES) << msg;
#ifdef ARANGODB_ENABLE_MAINTAINER_MODE
    for (auto it : _indexes) {
      LOG_TOPIC(ERR, arangodb::Logger::ENGINES) << "- " << it.get();
    }
#endif
    THROW_ARANGO_EXCEPTION_MESSAGE(TRI_ERROR_INTERNAL, msg);
  }

  TRI_ASSERT(!_indexes.empty());
}

std::shared_ptr<Index> RocksDBCollection::createIndex(VPackSlice const& info,
                                                      bool restore, bool& created) {
  TRI_ASSERT(info.isObject());
  Result res;

  // Step 0. Lock all the things
  TRI_vocbase_t& vocbase = _logicalCollection.vocbase();
  TRI_vocbase_col_status_e status;
  res = vocbase.useCollection(&_logicalCollection, status);
  if (res.fail()) {
    THROW_ARANGO_EXCEPTION(res);
  }
  _numIndexCreations.fetch_add(1, std::memory_order_release);
  auto colGuard = scopeGuard([&] {
    vocbase.releaseCollection(&_logicalCollection);
    _numIndexCreations.fetch_sub(1, std::memory_order_release);
  });

  RocksDBBuilderIndex::Locker locker(this);
  if (!locker.lock()) {
    THROW_ARANGO_EXCEPTION(TRI_ERROR_LOCK_TIMEOUT);
  }

  std::shared_ptr<Index> idx;
  {  // Step 1. Check for matching index
    WRITE_LOCKER(guard, _indexesLock);
    if ((idx = findIndex(info, _indexes)) != nullptr) {
      created = false;  // We already have this index.
      return idx;
    }
  }

  RocksDBEngine* engine = static_cast<RocksDBEngine*>(EngineSelectorFeature::ENGINE);

  // Step 2. We are sure that we do not have an index of this type.
  // We also hold the lock. Create it
  const bool generateKey = !restore;
  idx = engine->indexFactory().prepareIndexFromSlice(info, generateKey,
                                                     _logicalCollection, false);
  if (!idx) {
    THROW_ARANGO_EXCEPTION(TRI_ERROR_ARANGO_INDEX_CREATION_FAILED);
  }

  // we cannot persist primary or edge indexes
  TRI_ASSERT(idx->type() != Index::IndexType::TRI_IDX_TYPE_PRIMARY_INDEX);
  TRI_ASSERT(idx->type() != Index::IndexType::TRI_IDX_TYPE_EDGE_INDEX);

  {
    READ_LOCKER(guard, _indexesLock);
    for (auto const& other : _indexes) {  // conflicting index exists
      if (other->id() == idx->id()) {
        return other;  // index already exists
      }
    }
  }

  auto buildIdx =
      std::make_shared<RocksDBBuilderIndex>(std::static_pointer_cast<RocksDBIndex>(idx));
  // Step 3. add index to collection entry (for removal after a crash)
  if (!engine->inRecovery()) {  // manually modify collection entry, other
                                // methods need lock
    RocksDBKey key;             // read collection info from database
    key.constructCollection(_logicalCollection.vocbase().id(),
                            _logicalCollection.id());
    rocksdb::PinnableSlice value;
    rocksdb::Status s = engine->db()->Get(rocksdb::ReadOptions(),
                                          RocksDBColumnFamily::definitions(),
                                          key.string(), &value);
    if (!s.ok()) {
      res.reset(rocksutils::convertStatus(s));
    } else {
      VPackBuilder builder;
      builder.openObject();
      for (auto const& pair : VPackObjectIterator(VPackSlice(value.data()))) {
        if (pair.key.isEqualString("indexes")) {  // append new index 
          VPackArrayBuilder arrGuard(&builder, "indexes");
          builder.add(VPackArrayIterator(pair.value));
          buildIdx->toVelocyPack(builder, Index::makeFlags(Index::Serialize::Internals));
          continue;
        }
        builder.add(pair.key);
        builder.add(pair.value);
      }
      builder.close();
      res = engine->writeCreateCollectionMarker(_logicalCollection.vocbase().id(),
                                                _logicalCollection.id(), builder.slice(),
                                                RocksDBLogValue::Empty());
    }
  }

  const bool inBackground =
      basics::VelocyPackHelper::getBooleanValue(info, StaticStrings::IndexInBackground, false);
  // Step 4. fill index
  if (res.ok()) {
    if (inBackground) {  // allow concurrent inserts into index
      _indexes.emplace_back(buildIdx);
      res = buildIdx->fillIndexBackground(locker);
    } else {
      res = buildIdx->fillIndexForeground();
    }
  }
  TRI_ASSERT(res.fail() || locker.isLocked());  // always lock to avoid inconsistencies
  locker.lock();
  
  // Step 5. cleanup
  if (res.ok()) {
    {
      WRITE_LOCKER(guard, _indexesLock);
      if (inBackground) {  // swap in actual index
        for (size_t i = 0; i < _indexes.size(); i++) {
          if (_indexes[i]->id() == buildIdx->id()) {
            _indexes[i] = idx;
            break;
          }
        }
      } else {
        _indexes.push_back(idx);
      }
    }

#if USE_PLAN_CACHE
    arangodb::aql::PlanCache::instance()->invalidate(_logicalCollection->vocbase());
#endif

    if (!engine->inRecovery()) {  // write new collection marker
      auto builder =
          _logicalCollection.toVelocyPackIgnore({"path", "statusString"}, true,
                                                /*forPersistence*/ true);
      VPackBuilder indexInfo;
      idx->toVelocyPack(indexInfo, Index::makeFlags(Index::Serialize::Internals));
      res = engine->writeCreateCollectionMarker(
          _logicalCollection.vocbase().id(), _logicalCollection.id(), builder.slice(),
          RocksDBLogValue::IndexCreate(_logicalCollection.vocbase().id(),
                                       _logicalCollection.id(), indexInfo.slice()));
    }
  }

  if (res.fail()) {
    {  // We could not create the index. Better abort
      WRITE_LOCKER(guard, _indexesLock);
      auto it = _indexes.begin();
      while (it != _indexes.end()) {
        if ((*it)->id() == idx->id()) {
          _indexes.erase(it);
          break;
        }
        it++;
      }
    }
    idx->drop();
    THROW_ARANGO_EXCEPTION(res);
  }

  created = true;
  return idx;
}

/// @brief Drop an index with the given iid.
bool RocksDBCollection::dropIndex(TRI_idx_iid_t iid) {
  // usually always called when _exclusiveLock is held
  if (iid == 0) {
    // invalid index id or primary index
    return true;
  }

  std::shared_ptr<arangodb::Index> toRemove;
  {
    size_t i = 0;
    WRITE_LOCKER(guard, _indexesLock);
    for (std::shared_ptr<Index>& idx : _indexes) {
      if (iid == idx->id()) {
        toRemove = std::move(idx);
        _indexes.erase(_indexes.begin() + i);
        break;
      }
      ++i;
    }
  }
  if (!toRemove) {  // index not found
    // We tried to remove an index that does not exist
    events::DropIndex("", std::to_string(iid), TRI_ERROR_ARANGO_INDEX_NOT_FOUND);
    return false;
  }

  READ_LOCKER(guard, _indexesLock);

  RocksDBIndex* cindex = static_cast<RocksDBIndex*>(toRemove.get());
  TRI_ASSERT(cindex != nullptr);

  Result res = cindex->drop();
  if (res.ok()) {
    events::DropIndex("", std::to_string(iid), TRI_ERROR_NO_ERROR);

    // trigger compaction before deleting the object
    cindex->compact();

    auto builder =
        _logicalCollection.toVelocyPackIgnore({"path", "statusString"}, true, true);

    // log this event in the WAL and in the collection meta-data
    auto engine = static_cast<RocksDBEngine*>(EngineSelectorFeature::ENGINE);
    res = engine->writeCreateCollectionMarker(
        _logicalCollection.vocbase().id(), _logicalCollection.id(), builder.slice(),
        RocksDBLogValue::IndexDrop(_logicalCollection.vocbase().id(),
                                   _logicalCollection.id(), iid));
  }
  return res.ok();
}

std::unique_ptr<IndexIterator> RocksDBCollection::getAllIterator(transaction::Methods* trx) const {
  return std::unique_ptr<IndexIterator>(
      new RocksDBAllIndexIterator(&_logicalCollection, trx, primaryIndex()));
}

std::unique_ptr<IndexIterator> RocksDBCollection::getAnyIterator(transaction::Methods* trx) const {
  return std::unique_ptr<IndexIterator>(
      new RocksDBAnyIndexIterator(&_logicalCollection, trx, primaryIndex()));
}

void RocksDBCollection::invokeOnAllElements(transaction::Methods* trx,
                                            std::function<bool(LocalDocumentId const&)> callback) {
  std::unique_ptr<IndexIterator> cursor(this->getAllIterator(trx));
  bool cnt = true;
  auto cb = [&](LocalDocumentId token) {
    if (cnt) {
      cnt = callback(token);
    }
  };

  while (cursor->next(cb, 1000) && cnt) {
  }
}

////////////////////////////////////
// -- SECTION DML Operations --
///////////////////////////////////

Result RocksDBCollection::truncate(transaction::Methods& trx, OperationOptions& options) {
  TRI_ASSERT(_objectId != 0);
  auto state = RocksDBTransactionState::toState(&trx);
  RocksDBMethods* mthds = state->rocksdbMethods();

  if (state->isOnlyExclusiveTransaction() &&
      state->hasHint(transaction::Hints::Hint::ALLOW_RANGE_DELETE) &&
      this->canUseRangeDeleteInWal() && _numberDocuments >= 32 * 1024) {
    // non-transactional truncate optimization. We perform a bunch of
    // range deletes and circumwent the normal rocksdb::Transaction.
    // no savepoint needed here
    TRI_ASSERT(!state->hasOperations());  // not allowed

    TRI_IF_FAILURE("RocksDBRemoveLargeRangeOn") {
      return Result(TRI_ERROR_DEBUG);
    }

    RocksDBEngine* engine = rocksutils::globalRocksEngine();
    rocksdb::DB* db = engine->db()->GetRootDB();

    TRI_IF_FAILURE("RocksDBCollection::truncate::forceSync") {
      engine->settingsManager()->sync(false);
    }

    // pre commit sequence needed to place a blocker
    rocksdb::SequenceNumber seq = rocksutils::latestSequenceNumber();
    auto guard = scopeGuard([&] {  // remove blocker afterwards
      _meta.removeBlocker(state->id());
    });
    _meta.placeBlocker(state->id(), seq);

    rocksdb::WriteBatch batch;
    // delete documents
    RocksDBKeyBounds bounds = RocksDBKeyBounds::CollectionDocuments(_objectId);
    rocksdb::Status s =
        batch.DeleteRange(bounds.columnFamily(), bounds.start(), bounds.end());
    if (!s.ok()) {
      return rocksutils::convertStatus(s);
    }

    // delete indexes, place estimator blockers
    {
      READ_LOCKER(guard, _indexesLock);
      for (std::shared_ptr<Index> const& idx : _indexes) {
        RocksDBIndex* ridx = static_cast<RocksDBIndex*>(idx.get());
        bounds = ridx->getBounds();
        s = batch.DeleteRange(bounds.columnFamily(), bounds.start(), bounds.end());
        if (!s.ok()) {
          return rocksutils::convertStatus(s);
        }
      }
    }

    // add the log entry so we can recover the correct count
    auto log = RocksDBLogValue::CollectionTruncate(trx.vocbase().id(),
                                                   _logicalCollection.id(), _objectId);

    s = batch.PutLogData(log.slice());

    if (!s.ok()) {
      return rocksutils::convertStatus(s);
    }

    rocksdb::WriteOptions wo;

    s = db->Write(wo, &batch);

    if (!s.ok()) {
      return rocksutils::convertStatus(s);
    }

    seq = db->GetLatestSequenceNumber() - 1;  // post commit sequence

    uint64_t numDocs = _numberDocuments.exchange(0);

    _meta.adjustNumberDocuments(seq, /*revision*/ newRevisionId(),
                                -static_cast<int64_t>(numDocs));

    {
      READ_LOCKER(guard, _indexesLock);
      for (std::shared_ptr<Index> const& idx : _indexes) {
        idx->afterTruncate(seq);  // clears caches / clears links (if applicable)
      }
    }

    guard.fire();  // remove blocker

    if (numDocs > 64 * 1024) {
      // also compact the ranges in order to speed up all further accesses
      compact();
    }
    TRI_ASSERT(!state->hasOperations());  // not allowed
    return Result{};
  }

  TRI_IF_FAILURE("RocksDBRemoveLargeRangeOff") {
    return Result(TRI_ERROR_DEBUG);
  }

  // normal transactional truncate
  RocksDBKeyBounds documentBounds = RocksDBKeyBounds::CollectionDocuments(_objectId);
  rocksdb::Comparator const* cmp = RocksDBColumnFamily::documents()->GetComparator();
  rocksdb::ReadOptions ro = mthds->iteratorReadOptions();
  rocksdb::Slice const end = documentBounds.end();
  ro.iterate_upper_bound = &end;

  // avoid OOM error for truncate by committing earlier
  uint64_t const prvICC = state->options().intermediateCommitCount;
  state->options().intermediateCommitCount = std::min<uint64_t>(prvICC, 10000);

  std::unique_ptr<rocksdb::Iterator> iter =
      mthds->NewIterator(ro, documentBounds.columnFamily());
  iter->Seek(documentBounds.start());

  uint64_t found = 0;
  while (iter->Valid() && cmp->Compare(iter->key(), end) < 0) {
    ++found;
    TRI_ASSERT(_objectId == RocksDBKey::objectId(iter->key()));
    VPackSlice doc = VPackSlice(iter->value().data());
    TRI_ASSERT(doc.isObject());

    // To print the WAL we need key and RID
    VPackSlice key;
    TRI_voc_rid_t rid = 0;
    transaction::helpers::extractKeyAndRevFromDocument(doc, key, rid);
    TRI_ASSERT(key.isString());
    TRI_ASSERT(rid != 0);

    RocksDBSavePoint guard(&trx, TRI_VOC_DOCUMENT_OPERATION_REMOVE);

    state->prepareOperation(_logicalCollection.id(),
                            rid,  // actual revision ID!!
                            TRI_VOC_DOCUMENT_OPERATION_REMOVE);

    LocalDocumentId const docId = RocksDBKey::documentId(iter->key());
    auto res = removeDocument(&trx, docId, doc, options);

    if (res.fail()) {  // Failed to remove document in truncate.
      return res;
    }

    bool hasPerformedIntermediateCommit = false;

    res = state->addOperation(_logicalCollection.id(), docId.id(), TRI_VOC_DOCUMENT_OPERATION_REMOVE,
                              hasPerformedIntermediateCommit);

    if (res.fail()) {  // This should never happen...
      return res;
    }
    guard.finish(hasPerformedIntermediateCommit);

    trackWaitForSync(&trx, options);
    iter->Next();
  }

  // reset to previous value after truncate is finished
  state->options().intermediateCommitCount = prvICC;

  if (found > 64 * 1024) {
    // also compact the ranges in order to speed up all further accesses
    compact();
  }

#ifdef ARANGODB_ENABLE_MAINTAINER_MODE
  if (state->numCommits() == 0) {
    // check IN TRANSACTION if documents have been deleted
    if (mthds->countInBounds(RocksDBKeyBounds::CollectionDocuments(_objectId), true)) {
      THROW_ARANGO_EXCEPTION_MESSAGE(TRI_ERROR_INTERNAL,
                                     "deletion check in collection truncate "
                                     "failed - not all documents have been "
                                     "deleted");
    }
  }
#endif

  TRI_IF_FAILURE("FailAfterAllCommits") { return Result(TRI_ERROR_DEBUG); }
  TRI_IF_FAILURE("SegfaultAfterAllCommits") {
    TRI_SegfaultDebugging("SegfaultAfterAllCommits");
  }
  return Result{};
}

LocalDocumentId RocksDBCollection::lookupKey(transaction::Methods* trx,
                                             VPackSlice const& key) const {
  TRI_ASSERT(key.isString());
  return primaryIndex()->lookupKey(trx, StringRef(key));
}

bool RocksDBCollection::lookupRevision(transaction::Methods* trx, VPackSlice const& key,
                                       TRI_voc_rid_t& revisionId) const {
  TRI_ASSERT(key.isString());
  LocalDocumentId documentId;
  revisionId = 0;
  // lookup the revision id in the primary index
  if (!primaryIndex()->lookupRevision(trx, StringRef(key), documentId, revisionId)) {
    // document not found
    TRI_ASSERT(revisionId == 0);
    return false;
  }

  // document found, but revisionId may not have been present in the primary
  // index this can happen for "older" collections
  TRI_ASSERT(documentId.isSet());

  // now look up the revision id in the actual document data

  return readDocumentWithCallback(trx, documentId, [&revisionId](LocalDocumentId const&, VPackSlice doc) {
    revisionId = transaction::helpers::extractRevFromDocument(doc);
  });
}

Result RocksDBCollection::read(transaction::Methods* trx, arangodb::StringRef const& key,
                               ManagedDocumentResult& result, bool) {
  LocalDocumentId const documentId = primaryIndex()->lookupKey(trx, key);
  if (documentId.isSet()) {
    return lookupDocumentVPack(documentId, trx, result, true);
  }
  // not found
  return Result(TRI_ERROR_ARANGO_DOCUMENT_NOT_FOUND);
}

// read using a token!
bool RocksDBCollection::readDocument(transaction::Methods* trx,
                                     LocalDocumentId const& documentId,
                                     ManagedDocumentResult& result) const {
  if (documentId.isSet()) {
    auto res = lookupDocumentVPack(documentId, trx, result, true);
    return res.ok();
  }
  return false;
}

// read using a token!
bool RocksDBCollection::readDocumentWithCallback(transaction::Methods* trx,
                                                 LocalDocumentId const& documentId,
                                                 IndexIterator::DocumentCallback const& cb) const {
  if (documentId.isSet()) {
    return lookupDocumentVPack(documentId, trx, cb, true).ok();
  }
  return false;
}

Result RocksDBCollection::insert(arangodb::transaction::Methods* trx,
                                 arangodb::velocypack::Slice const slice,
                                 arangodb::ManagedDocumentResult& mdr,
                                 OperationOptions& options, TRI_voc_tick_t& resultMarkerTick,
                                 bool, TRI_voc_tick_t& revisionId, KeyLockInfo* /*keyLockInfo*/,
                                 std::function<Result(void)> callbackDuringLock) {
  // store the tick that was used for writing the document
  // note that we don't need it for this engine
  resultMarkerTick = 0;

  LocalDocumentId const documentId = LocalDocumentId::create();
  auto isEdgeCollection = (TRI_COL_TYPE_EDGE == _logicalCollection.type());
  transaction::BuilderLeaser builder(trx);
  Result res(newObjectForInsert(trx, slice, isEdgeCollection, *builder.get(),
                                options.isRestore, revisionId));

  if (res.fail()) {
    return res;
  }

  VPackSlice newSlice = builder->slice();

  if (options.overwrite) {
    // special optimization for the overwrite case:
    // in case the operation is a RepSert, we will first check if the specified
    // primary key exists. we can abort this low-level insert early, before any
    // modification to the data has been done. this saves us from creating a
    // RocksDB transaction SavePoint. if we don't do the check here, we will
    // always create a SavePoint first and insert the new document. when then
    // inserting the key for the primary index and then detecting a unique
    // constraint violation, the transaction would be rolled back to the
    // SavePoint state, which will rebuild *all* data in the WriteBatch up to
    // the SavePoint. this can be super-expensive for bigger transactions. to
    // keep things simple, we are not checking for unique constraint violations
    // in secondary indexes here, but defer it to the regular index insertion
    // check
    VPackSlice keySlice = transaction::helpers::extractKeyFromDocument(newSlice);
    if (keySlice.isString()) {
      LocalDocumentId const documentId =
          primaryIndex()->lookupKey(trx, StringRef(keySlice));
      if (documentId.isSet()) {
        if (options.indexOperationMode == Index::OperationMode::internal) {
          // need to return the key of the conflict document
          return res.reset(TRI_ERROR_ARANGO_UNIQUE_CONSTRAINT_VIOLATED,
                           keySlice.copyString());
        }
        return res.reset(TRI_ERROR_ARANGO_UNIQUE_CONSTRAINT_VIOLATED);
      }
    }
  }

  RocksDBSavePoint guard(trx, TRI_VOC_DOCUMENT_OPERATION_INSERT);

  auto state = RocksDBTransactionState::toState(trx);
  state->prepareOperation(_logicalCollection.id(), revisionId, TRI_VOC_DOCUMENT_OPERATION_INSERT);

  res = insertDocument(trx, documentId, newSlice, options);

  if (res.ok()) {
    trackWaitForSync(trx, options);
    if (options.silent) {
      mdr.clear();
    } else {
      mdr.setManaged(newSlice.begin(), documentId);
      TRI_ASSERT(!mdr.empty());
    }

    bool hasPerformedIntermediateCommit = false;

    auto result = state->addOperation(_logicalCollection.id(), revisionId,
                                      TRI_VOC_DOCUMENT_OPERATION_INSERT,
                                      hasPerformedIntermediateCommit);

    if (result.ok() && callbackDuringLock != nullptr) {
      result = callbackDuringLock();
    }

    if (result.fail()) {
      THROW_ARANGO_EXCEPTION(result);
    }

    guard.finish(hasPerformedIntermediateCommit);
  }

  return res;
}

Result RocksDBCollection::update(arangodb::transaction::Methods* trx,
                                 arangodb::velocypack::Slice const newSlice,
                                 ManagedDocumentResult& mdr, OperationOptions& options,
                                 TRI_voc_tick_t& resultMarkerTick, bool,
                                 TRI_voc_rid_t& prevRev, ManagedDocumentResult& previous,
                                 arangodb::velocypack::Slice const key,
                                 std::function<Result(void)> callbackDuringLock) {
  resultMarkerTick = 0;

  LocalDocumentId const documentId = LocalDocumentId::create();
  auto isEdgeCollection = (TRI_COL_TYPE_EDGE == _logicalCollection.type());
  Result res = this->read(trx, key, previous, /*lock*/ false);

  if (res.fail()) {
    return res;
  }

  TRI_ASSERT(!previous.empty());

  LocalDocumentId const oldDocumentId = previous.localDocumentId();
  VPackSlice oldDoc(previous.vpack());
  TRI_voc_rid_t const oldRevisionId = transaction::helpers::extractRevFromDocument(oldDoc);

  prevRev = oldRevisionId;

  // Check old revision:
  if (!options.ignoreRevs) {
    TRI_voc_rid_t expectedRev = 0;

    if (newSlice.isObject()) {
      expectedRev = TRI_ExtractRevisionId(newSlice);
    }

    int result = checkRevision(trx, expectedRev, prevRev);

    if (result != TRI_ERROR_NO_ERROR) {
      return res.reset(result);
    }
  }

  if (newSlice.length() <= 1) {
    // shortcut. no need to do anything
    mdr = previous;
    TRI_ASSERT(!mdr.empty());

    trackWaitForSync(trx, options);
    return res;
  }

  // merge old and new values
  TRI_voc_rid_t revisionId;
  transaction::BuilderLeaser builder(trx);
  res = mergeObjectsForUpdate(trx, oldDoc, newSlice, isEdgeCollection,
                              options.mergeObjects, options.keepNull,
                              *builder.get(), options.isRestore, revisionId);

  if (res.fail()) {
    return res;
  }

  if (_isDBServer) {
    // Need to check that no sharding keys have changed:
    if (arangodb::shardKeysChanged(_logicalCollection, oldDoc, builder->slice(), false)) {
      return res.reset(TRI_ERROR_CLUSTER_MUST_NOT_CHANGE_SHARDING_ATTRIBUTES);
    }
  }

  VPackSlice const newDoc(builder->slice());

  auto state = RocksDBTransactionState::toState(trx);
  RocksDBSavePoint guard(trx, TRI_VOC_DOCUMENT_OPERATION_UPDATE);

  // add possible log statement under guard
  state->prepareOperation(_logicalCollection.id(), revisionId, TRI_VOC_DOCUMENT_OPERATION_UPDATE);
  res = updateDocument(trx, oldDocumentId, oldDoc, documentId, newDoc, options);

  if (res.ok()) {
    trackWaitForSync(trx, options);

    if (options.silent) {
      mdr.clear();
    } else {
      mdr.setManaged(newDoc.begin(), documentId);
      TRI_ASSERT(!mdr.empty());
    }

    bool hasPerformedIntermediateCommit = false;

    auto result = state->addOperation(_logicalCollection.id(), revisionId,
                                      TRI_VOC_DOCUMENT_OPERATION_UPDATE,
                                      hasPerformedIntermediateCommit);

    if (result.ok() && callbackDuringLock != nullptr) {
      result = callbackDuringLock();
    }

    if (result.fail()) {
      THROW_ARANGO_EXCEPTION(result);
    }

    guard.finish(hasPerformedIntermediateCommit);
  }

  return res;
}

Result RocksDBCollection::replace(transaction::Methods* trx,
                                  arangodb::velocypack::Slice const newSlice,
                                  ManagedDocumentResult& mdr, OperationOptions& options,
                                  TRI_voc_tick_t& resultMarkerTick, bool,
                                  TRI_voc_rid_t& prevRev, ManagedDocumentResult& previous,
                                  std::function<Result(void)> callbackDuringLock) {
  resultMarkerTick = 0;

  LocalDocumentId const documentId = LocalDocumentId::create();
  auto isEdgeCollection = (TRI_COL_TYPE_EDGE == _logicalCollection.type());

  // get the previous revision
  VPackSlice key = newSlice.get(StaticStrings::KeyString);

  Result res;
  if (key.isNone()) {
    return res.reset(TRI_ERROR_ARANGO_DOCUMENT_HANDLE_BAD);
  }

  // get the previous revision
  res = this->read(trx, key, previous, /*lock*/ false);
  if (res.fail()) {
    return res;
  }

  TRI_ASSERT(!previous.empty());
  LocalDocumentId const oldDocumentId = previous.localDocumentId();

  VPackSlice oldDoc(previous.vpack());
  TRI_voc_rid_t oldRevisionId = transaction::helpers::extractRevFromDocument(oldDoc);
  prevRev = oldRevisionId;

  // Check old revision:
  if (!options.ignoreRevs) {
    TRI_voc_rid_t expectedRev = 0;
    if (newSlice.isObject()) {
      expectedRev = TRI_ExtractRevisionId(newSlice);
    }

    res = checkRevision(trx, expectedRev, prevRev);
    if (res.fail()) {
      return res;
    }
  }

  // merge old and new values
  TRI_voc_rid_t revisionId;
  transaction::BuilderLeaser builder(trx);
  res = newObjectForReplace(trx, oldDoc, newSlice, isEdgeCollection,
                            *builder.get(), options.isRestore, revisionId);

  if (res.fail()) {
    return res;
  }

  if (_isDBServer) {
    // Need to check that no sharding keys have changed:
    if (arangodb::shardKeysChanged(_logicalCollection, oldDoc, builder->slice(), false)) {
      return res.reset(TRI_ERROR_CLUSTER_MUST_NOT_CHANGE_SHARDING_ATTRIBUTES);
    }
  }

  VPackSlice const newDoc(builder->slice());

  auto state = RocksDBTransactionState::toState(trx);
  RocksDBSavePoint guard(trx, TRI_VOC_DOCUMENT_OPERATION_REPLACE);

  // add possible log statement under guard
  state->prepareOperation(_logicalCollection.id(), revisionId, TRI_VOC_DOCUMENT_OPERATION_REPLACE);

  res = updateDocument(trx, oldDocumentId, oldDoc, documentId, newDoc, options);

  if (res.ok()) {
    trackWaitForSync(trx, options);

    if (options.silent) {
      mdr.clear();
    } else {
      mdr.setManaged(newDoc.begin(), documentId);
      TRI_ASSERT(!mdr.empty());
    }

    bool hasPerformedIntermediateCommit = false;

    auto result = state->addOperation(_logicalCollection.id(), revisionId,
                                      TRI_VOC_DOCUMENT_OPERATION_REPLACE,
                                      hasPerformedIntermediateCommit);

    if (result.ok() && callbackDuringLock != nullptr) {
      result = callbackDuringLock();
    }

    if (result.fail()) {
      THROW_ARANGO_EXCEPTION(result);
    }

    guard.finish(hasPerformedIntermediateCommit);
  }

  return res;
}

Result RocksDBCollection::remove(transaction::Methods& trx, velocypack::Slice slice,
                                 ManagedDocumentResult& previous, OperationOptions& options,
                                 TRI_voc_tick_t& resultMarkerTick, bool /*lock*/,
                                 TRI_voc_rid_t& prevRev, TRI_voc_rid_t& revisionId,
                                 KeyLockInfo* /*keyLockInfo*/,
                                 std::function<Result(void)> callbackDuringLock) {
  // store the tick that was used for writing the document
  // note that we don't need it for this engine
  resultMarkerTick = 0;
  prevRev = 0;
  revisionId = newRevisionId();

  VPackSlice key;
  if (slice.isString()) {
    key = slice;
  } else {
    key = slice.get(StaticStrings::KeyString);
  }
  TRI_ASSERT(!key.isNone());

  // get the previous revision
  Result res = this->read(&trx, key, previous, /*lock*/ false);

  if (res.fail()) {
    return res;
  }

  TRI_ASSERT(!previous.empty());
  LocalDocumentId const oldDocumentId = previous.localDocumentId();

  VPackSlice oldDoc(previous.vpack());
  TRI_voc_rid_t oldRevisionId =
      arangodb::transaction::helpers::extractRevFromDocument(oldDoc);
  prevRev = oldRevisionId;

  // Check old revision:
  if (!options.ignoreRevs && slice.isObject()) {
    TRI_voc_rid_t expectedRevisionId = TRI_ExtractRevisionId(slice);
    res = checkRevision(&trx, expectedRevisionId, oldRevisionId);

    if (res.fail()) {
      return res;
    }
  }

  auto state = RocksDBTransactionState::toState(&trx);
  RocksDBSavePoint guard(&trx, TRI_VOC_DOCUMENT_OPERATION_REMOVE);

  // add possible log statement under guard
  state->prepareOperation(_logicalCollection.id(), oldRevisionId,
                          TRI_VOC_DOCUMENT_OPERATION_REMOVE);
  res = removeDocument(&trx, oldDocumentId, oldDoc, options);

  if (res.ok()) {
    trackWaitForSync(&trx, options);

    bool hasPerformedIntermediateCommit = false;

    res = state->addOperation(_logicalCollection.id(), revisionId, TRI_VOC_DOCUMENT_OPERATION_REMOVE,
                              hasPerformedIntermediateCommit);

    if (res.ok() && callbackDuringLock != nullptr) {
      res = callbackDuringLock();
    }

    if (res.fail()) {
      THROW_ARANGO_EXCEPTION(res);
    }

    guard.finish(hasPerformedIntermediateCommit);
  }

  return res;
}

void RocksDBCollection::deferDropCollection(std::function<bool(LogicalCollection&)> const& /*callback*/
) {
  // nothing to do here
}

/// @brief return engine-specific figures
void RocksDBCollection::figuresSpecific(std::shared_ptr<arangodb::velocypack::Builder>& builder) {
  rocksdb::TransactionDB* db = rocksutils::globalRocksDB();
  RocksDBKeyBounds bounds = RocksDBKeyBounds::CollectionDocuments(_objectId);
  rocksdb::Range r(bounds.start(), bounds.end());

  uint64_t out = 0;
  db->GetApproximateSizes(RocksDBColumnFamily::documents(), &r, 1, &out,
                          static_cast<uint8_t>(
                              rocksdb::DB::SizeApproximationFlags::INCLUDE_MEMTABLES |
                              rocksdb::DB::SizeApproximationFlags::INCLUDE_FILES));

  builder->add("documentsSize", VPackValue(out));
  bool cacheInUse = useCache();
  builder->add("cacheInUse", VPackValue(cacheInUse));
  if (cacheInUse) {
    builder->add("cacheSize", VPackValue(_cache->size()));
    builder->add("cacheUsage", VPackValue(_cache->usage()));
    auto hitRates = _cache->hitRates();
    double rate = hitRates.first;
    rate = std::isnan(rate) ? 0.0 : rate;
    builder->add("cacheLifeTimeHitRate", VPackValue(rate));
    rate = hitRates.second;
    rate = std::isnan(rate) ? 0.0 : rate;
    builder->add("cacheWindowedHitRate", VPackValue(rate));
  } else {
    builder->add("cacheSize", VPackValue(0));
    builder->add("cacheUsage", VPackValue(0));
  }
}

Result RocksDBCollection::insertDocument(arangodb::transaction::Methods* trx,
                                         LocalDocumentId const& documentId,
                                         VPackSlice const& doc,
                                         OperationOptions& options) const {
  // Coordinator doesn't know index internals
  TRI_ASSERT(!ServerState::instance()->isCoordinator());
  TRI_ASSERT(trx->state()->isRunning());
  Result res;

  RocksDBKeyLeaser key(trx);
  key->constructDocument(_objectId, documentId);

  blackListKey(key->string().data(), static_cast<uint32_t>(key->string().size()));

  RocksDBMethods* mthds = RocksDBTransactionState::toMethods(trx);
  // disable indexing in this transaction if we are allowed to
  IndexingDisabler disabler(mthds, trx->isSingleOperationTransaction());

  TRI_ASSERT(key->containsLocalDocumentId(documentId));
  rocksdb::Status s =
      mthds->PutUntracked(RocksDBColumnFamily::documents(), key.ref(),
                          rocksdb::Slice(doc.startAs<char>(),
                                         static_cast<size_t>(doc.byteSize())));
  if (!s.ok()) {
    return res.reset(rocksutils::convertStatus(s, rocksutils::document));
  }

  READ_LOCKER(guard, _indexesLock);
  for (std::shared_ptr<Index> const& idx : _indexes) {
    RocksDBIndex* rIdx = static_cast<RocksDBIndex*>(idx.get());
    res = rIdx->insert(*trx, mthds, documentId, doc, options.indexOperationMode);

    if (res.fail()) {
      break;
    }
  }

  return res;
}

Result RocksDBCollection::removeDocument(arangodb::transaction::Methods* trx,
                                         LocalDocumentId const& documentId,
                                         VPackSlice const& doc,
                                         OperationOptions& options) const {
  // Coordinator doesn't know index internals
  TRI_ASSERT(!ServerState::instance()->isCoordinator());
  TRI_ASSERT(trx->state()->isRunning());
  TRI_ASSERT(_objectId != 0);
  Result res;

  RocksDBKeyLeaser key(trx);
  key->constructDocument(_objectId, documentId);

  blackListKey(key->string().data(), static_cast<uint32_t>(key->string().size()));

  RocksDBMethods* mthd = RocksDBTransactionState::toMethods(trx);

  // disable indexing in this transaction if we are allowed to
  IndexingDisabler disabler(mthd, trx->isSingleOperationTransaction());

  rocksdb::Status s = mthd->SingleDelete(RocksDBColumnFamily::documents(), key.ref());
  if (!s.ok()) {
    return res.reset(rocksutils::convertStatus(s, rocksutils::document));
  }

  /*LOG_TOPIC(ERR, Logger::ENGINES)
      << "Delete rev: " << revisionId << " trx: " << trx->state()->id()
      << " seq: " << mthd->sequenceNumber()
      << " objectID " << _objectId << " name: " << _logicalCollection->name();*/

  Result resInner;
  READ_LOCKER(guard, _indexesLock);
  for (std::shared_ptr<Index> const& idx : _indexes) {
    RocksDBIndex* ridx = static_cast<RocksDBIndex*>(idx.get());
    res = ridx->remove(*trx, mthd, documentId, doc, options.indexOperationMode);

    if (res.fail()) {
      break;
    }
  }

  return res;
}

Result RocksDBCollection::updateDocument(transaction::Methods* trx,
                                         LocalDocumentId const& oldDocumentId,
                                         VPackSlice const& oldDoc,
                                         LocalDocumentId const& newDocumentId,
                                         VPackSlice const& newDoc,
                                         OperationOptions& options) const {
  // Coordinator doesn't know index internals
  TRI_ASSERT(!ServerState::instance()->isCoordinator());
  TRI_ASSERT(trx->state()->isRunning());
  TRI_ASSERT(_objectId != 0);
  Result res;

  RocksDBMethods* mthd = RocksDBTransactionState::toMethods(trx);
  // disable indexing in this transaction if we are allowed to
  IndexingDisabler disabler(mthd, trx->isSingleOperationTransaction());

  RocksDBKeyLeaser key(trx);
  key->constructDocument(_objectId, oldDocumentId);
  blackListKey(key->string().data(), static_cast<uint32_t>(key->string().size()));

  rocksdb::Status s = mthd->SingleDelete(RocksDBColumnFamily::documents(), key.ref());
  if (!s.ok()) {
    return res.reset(rocksutils::convertStatus(s, rocksutils::document));
  }

  key->constructDocument(_objectId, newDocumentId);
  // simon: we do not need to blacklist the new documentId
  TRI_ASSERT(key->containsLocalDocumentId(newDocumentId));
  s = mthd->PutUntracked(RocksDBColumnFamily::documents(), key.ref(),
                         rocksdb::Slice(newDoc.startAs<char>(),
                                        static_cast<size_t>(newDoc.byteSize())));
  if (!s.ok()) {
    return res.reset(rocksutils::convertStatus(s, rocksutils::document));
  }

  READ_LOCKER(guard, _indexesLock);
  for (std::shared_ptr<Index> const& idx : _indexes) {
    RocksDBIndex* rIdx = static_cast<RocksDBIndex*>(idx.get());
    res = rIdx->update(*trx, mthd, oldDocumentId, oldDoc, newDocumentId, newDoc,
                       options.indexOperationMode);

    if (res.fail()) {
      break;
    }
  }

  return res;
}

arangodb::Result RocksDBCollection::lookupDocumentVPack(LocalDocumentId const& documentId,
                                                        transaction::Methods* trx,
                                                        arangodb::ManagedDocumentResult& mdr,
                                                        bool withCache) const {
  TRI_ASSERT(trx->state()->isRunning());
  TRI_ASSERT(_objectId != 0);
  Result res;

  RocksDBKeyLeaser key(trx);
  key->constructDocument(_objectId, documentId);

  bool lockTimeout = false;
  if (withCache && useCache()) {
    TRI_ASSERT(_cache != nullptr);
    // check cache first for fast path
    auto f = _cache->find(key->string().data(),
                          static_cast<uint32_t>(key->string().size()));
    if (f.found()) {
      std::string* value = mdr.setManaged(documentId);
      value->append(reinterpret_cast<char const*>(f.value()->value()),
                    f.value()->valueSize());
      return res;
    }
    if (f.result().errorNumber() == TRI_ERROR_LOCK_TIMEOUT) {
      // assuming someone is currently holding a write lock, which
      // is why we cannot access the TransactionalBucket.
      lockTimeout = true;  // we skip the insert in this case
    }
  }

  RocksDBMethods* mthd = RocksDBTransactionState::toMethods(trx);
  std::string* value = mdr.setManaged(documentId);
  rocksdb::Status s = mthd->Get(RocksDBColumnFamily::documents(), key->string(), value);

  if (s.ok()) {
    if (withCache && useCache() && !lockTimeout) {
      TRI_ASSERT(_cache != nullptr);
      // write entry back to cache
      auto entry =
          cache::CachedValue::construct(key->string().data(),
                                        static_cast<uint32_t>(key->string().size()),
                                        value->data(),
                                        static_cast<uint64_t>(value->size()));

      if (entry) {
        Result status = _cache->insert(entry);
        if (status.errorNumber() == TRI_ERROR_LOCK_TIMEOUT) {
          // the writeLock uses cpu_relax internally, so we can try yield
          std::this_thread::yield();
          status = _cache->insert(entry);
        }

        if (status.fail()) {
          delete entry;
        }
      }
    }
  } else {
    LOG_TOPIC(DEBUG, Logger::ENGINES)
        << "NOT FOUND rev: " << documentId.id() << " trx: " << trx->state()->id()
        << " objectID " << _objectId << " name: " << _logicalCollection.name();
    mdr.clear();
    res.reset(rocksutils::convertStatus(s, rocksutils::document));
  }

  return res;
}

arangodb::Result RocksDBCollection::lookupDocumentVPack(
    LocalDocumentId const& documentId, transaction::Methods* trx,
    IndexIterator::DocumentCallback const& cb, bool withCache) const {
  TRI_ASSERT(trx->state()->isRunning());
  TRI_ASSERT(_objectId != 0);
  Result res;

  RocksDBKeyLeaser key(trx);
  key->constructDocument(_objectId, documentId);

  bool lockTimeout = false;
  if (withCache && useCache()) {
    TRI_ASSERT(_cache != nullptr);
    // check cache first for fast path
    auto f = _cache->find(key->string().data(),
                          static_cast<uint32_t>(key->string().size()));
    if (f.found()) {
      cb(documentId, VPackSlice(reinterpret_cast<char const*>(f.value()->value())));
      return res;
    }
    if (f.result().errorNumber() == TRI_ERROR_LOCK_TIMEOUT) {
      // assuming someone is currently holding a write lock, which
      // is why we cannot access the TransactionalBucket.
      lockTimeout = true;  // we skip the insert in this case
    }
  }

  rocksdb::PinnableSlice ps;
  RocksDBMethods* mthd = RocksDBTransactionState::toMethods(trx);
  rocksdb::Status s = mthd->Get(RocksDBColumnFamily::documents(), key->string(), &ps);

  if (s.ok()) {
    if (withCache && useCache() && !lockTimeout) {
      TRI_ASSERT(_cache != nullptr);
      // write entry back to cache
      auto entry =
          cache::CachedValue::construct(key->string().data(),
                                        static_cast<uint32_t>(key->string().size()),
                                        ps.data(), static_cast<uint64_t>(ps.size()));
      if (entry) {
        auto status = _cache->insert(entry);
        if (status.errorNumber() == TRI_ERROR_LOCK_TIMEOUT) {
          // the writeLock uses cpu_relax internally, so we can try yield
          std::this_thread::yield();
          status = _cache->insert(entry);
        }
        if (status.fail()) {
          delete entry;
        }
      }
    }

    cb(documentId, VPackSlice(ps.data()));
  } else {
    LOG_TOPIC(DEBUG, Logger::ENGINES)
        << "NOT FOUND rev: " << documentId.id() << " trx: " << trx->state()->id()
        << " objectID " << _objectId << " name: " << _logicalCollection.name();
    res.reset(rocksutils::convertStatus(s, rocksutils::document));
  }
  return res;
}

/// may never be called unless recovery is finished
void RocksDBCollection::adjustNumberDocuments(TRI_voc_rid_t revId, int64_t adjustment) {
#ifdef ARANGODB_ENABLE_MAINTAINER_MODE
  RocksDBEngine* engine = static_cast<RocksDBEngine*>(EngineSelectorFeature::ENGINE);
  TRI_ASSERT(engine != nullptr);
  TRI_ASSERT(!engine->inRecovery());
#endif
  if (revId != 0) {
    _revisionId = revId;
  }
  if (adjustment < 0) {
    TRI_ASSERT(_numberDocuments >= static_cast<uint64_t>(-adjustment));
    _numberDocuments -= static_cast<uint64_t>(-adjustment);
  } else if (adjustment > 0) {
    _numberDocuments += static_cast<uint64_t>(adjustment);
  }
}

/// load the number of docs from storage, use careful
void RocksDBCollection::loadInitialNumberDocuments() {
  RocksDBCollectionMeta::DocCount count = _meta.currentCount();
  TRI_ASSERT(count._added >= count._removed);
  _numberDocuments = count._added - count._removed;
  _revisionId = count._revisionId;
}

/// @brief write locks a collection, with a timeout
int RocksDBCollection::lockWrite(double timeout) {
  uint64_t waitTime = 0;  // indicates that time is uninitialized
  double startTime = 0.0;

  while (true) {
    TRY_WRITE_LOCKER(locker, _exclusiveLock);

    if (locker.isLocked()) {
      // keep lock and exit loop
      locker.steal();
      return TRI_ERROR_NO_ERROR;
    }

    double now = TRI_microtime();

    if (waitTime == 0) {  // initialize times
      // set end time for lock waiting
      if (timeout <= 0.0) {
        timeout = defaultLockTimeout;
      }

      startTime = now;
      waitTime = 1;
    }

    if (now > startTime + timeout) {
      LOG_TOPIC(TRACE, arangodb::Logger::ENGINES)
          << "timed out after " << timeout << " s waiting for write-lock on collection '"
          << _logicalCollection.name() << "'";

      return TRI_ERROR_LOCK_TIMEOUT;
    }

    if (now - startTime < 0.001) {
      std::this_thread::yield();
    } else {
      std::this_thread::sleep_for(std::chrono::microseconds(waitTime));
      if (waitTime < 32) {
        waitTime *= 2;
      }
    }
  }
}

/// @brief write unlocks a collection
void RocksDBCollection::unlockWrite() { _exclusiveLock.unlockWrite(); }

/// @brief read locks a collection, with a timeout
int RocksDBCollection::lockRead(double timeout) {
  uint64_t waitTime = 0;  // indicates that time is uninitialized
  double startTime = 0.0;

  while (true) {
    TRY_READ_LOCKER(locker, _exclusiveLock);

    if (locker.isLocked()) {
      // keep lock and exit loop
      locker.steal();
      return TRI_ERROR_NO_ERROR;
    }

    double now = TRI_microtime();

    if (waitTime == 0) {  // initialize times
      // set end time for lock waiting
      if (timeout <= 0.0) {
        timeout = defaultLockTimeout;
      }

      startTime = now;
      waitTime = 1;
    }

    if (now > startTime + timeout) {
      LOG_TOPIC(TRACE, arangodb::Logger::ENGINES)
          << "timed out after " << timeout << " s waiting for read-lock on collection '"
          << _logicalCollection.name() << "'";

      return TRI_ERROR_LOCK_TIMEOUT;
    }

    if (now - startTime < 0.001) {
      std::this_thread::yield();
    } else {
      std::this_thread::sleep_for(std::chrono::microseconds(waitTime));

      if (waitTime < 32) {
        waitTime *= 2;
      }
    }
  }
}

/// @brief read unlocks a collection
void RocksDBCollection::unlockRead() { _exclusiveLock.unlockRead(); }

// rescans the collection to update document count
uint64_t RocksDBCollection::recalculateCounts() {
  RocksDBEngine* engine = rocksutils::globalRocksEngine();
  rocksdb::TransactionDB* db = engine->db();
  const rocksdb::Snapshot* snapshot = nullptr;
  // start transaction to get a collection lock
  TRI_vocbase_t& vocbase = _logicalCollection.vocbase();
  if (!vocbase.use()) {  // someone dropped the database
    return numberDocuments();
  }
  auto useGuard = scopeGuard([&] {
    if (snapshot) {
      db->ReleaseSnapshot(snapshot);
    }
    vocbase.release();
  });

  TRI_vocbase_col_status_e status;
  int res = vocbase.useCollection(&_logicalCollection, status);
  if (res != TRI_ERROR_NO_ERROR) {
    THROW_ARANGO_EXCEPTION(res);
  }
  auto collGuard =
      scopeGuard([&] { vocbase.releaseCollection(&_logicalCollection); });

  uint64_t snapNumberOfDocuments = 0;
  {
    // fetch number docs and snapshot under exclusive lock
    // this should enable us to correct the count later
    auto lockGuard = scopeGuard([this] { unlockWrite(); });
    res = lockWrite(transaction::Options::defaultLockTimeout);
    if (res != TRI_ERROR_NO_ERROR) {
      lockGuard.cancel();
      THROW_ARANGO_EXCEPTION(res);
    }

    snapNumberOfDocuments = numberDocuments();
    snapshot = engine->db()->GetSnapshot();
    TRI_ASSERT(snapshot);
  }

  // count documents
  auto bounds = RocksDBKeyBounds::CollectionDocuments(_objectId);
  rocksdb::Slice upper(bounds.end());

  rocksdb::ReadOptions ro;
  ro.snapshot = snapshot;
  ro.prefix_same_as_start = true;
  ro.iterate_upper_bound = &upper;
  ro.verify_checksums = false;
  ro.fill_cache = false;

  rocksdb::ColumnFamilyHandle* cf = bounds.columnFamily();
  std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(ro, cf));
  std::size_t count = 0;

  for (it->Seek(bounds.start()); it->Valid(); it->Next()) {
    TRI_ASSERT(it->key().compare(upper) < 0);
    ++count;
  }

  int64_t adjustment = snapNumberOfDocuments - count;
  if (adjustment != 0) {
    LOG_TOPIC(WARN, Logger::REPLICATION)
        << "inconsistent collection count detected, "
        << "an offet of " << adjustment << " will be applied";
    adjustNumberDocuments(static_cast<TRI_voc_rid_t>(0), adjustment);
  }

  return numberDocuments();
}

void RocksDBCollection::compact() {
  rocksdb::TransactionDB* db = rocksutils::globalRocksDB();
  rocksdb::CompactRangeOptions opts;
  RocksDBKeyBounds bounds = RocksDBKeyBounds::CollectionDocuments(_objectId);
  rocksdb::Slice b = bounds.start(), e = bounds.end();
  db->CompactRange(opts, bounds.columnFamily(), &b, &e);

  READ_LOCKER(guard, _indexesLock);
  for (std::shared_ptr<Index> i : _indexes) {
    RocksDBIndex* index = static_cast<RocksDBIndex*>(i.get());
    index->compact();
  }
}

void RocksDBCollection::estimateSize(velocypack::Builder& builder) {
  TRI_ASSERT(!builder.isOpenObject() && !builder.isOpenArray());

  rocksdb::TransactionDB* db = rocksutils::globalRocksDB();
  RocksDBKeyBounds bounds = RocksDBKeyBounds::CollectionDocuments(_objectId);
  rocksdb::Range r(bounds.start(), bounds.end());
  uint64_t out = 0, total = 0;
  db->GetApproximateSizes(RocksDBColumnFamily::documents(), &r, 1, &out,
                          static_cast<uint8_t>(
                              rocksdb::DB::SizeApproximationFlags::INCLUDE_MEMTABLES |
                              rocksdb::DB::SizeApproximationFlags::INCLUDE_FILES));
  total += out;

  builder.openObject();
  builder.add("documents", VPackValue(out));
  builder.add("indexes", VPackValue(VPackValueType::Object));

  READ_LOCKER(guard, _indexesLock);
  for (std::shared_ptr<Index> i : _indexes) {
    RocksDBIndex* index = static_cast<RocksDBIndex*>(i.get());
    out = index->memory();
    builder.add(std::to_string(index->id()), VPackValue(out));
    total += out;
  }
  builder.close();
  builder.add("total", VPackValue(total));
  builder.close();
}

void RocksDBCollection::createCache() const {
  if (!_cacheEnabled || _cachePresent || _logicalCollection.isAStub() ||
      ServerState::instance()->isCoordinator()) {
    // we leave this if we do not need the cache
    // or if cache already created
    return;
  }

  TRI_ASSERT(_cacheEnabled);
  TRI_ASSERT(_cache.get() == nullptr);
  TRI_ASSERT(CacheManagerFeature::MANAGER != nullptr);
  LOG_TOPIC(DEBUG, Logger::CACHE) << "Creating document cache";
  _cache = CacheManagerFeature::MANAGER->createCache(cache::CacheType::Transactional);
  _cachePresent = (_cache.get() != nullptr);
  TRI_ASSERT(_cacheEnabled);
}

void RocksDBCollection::destroyCache() const {
  if (!_cachePresent) {
    return;
  }
  TRI_ASSERT(CacheManagerFeature::MANAGER != nullptr);
  // must have a cache...
  TRI_ASSERT(_cache.get() != nullptr);
  LOG_TOPIC(DEBUG, Logger::CACHE) << "Destroying document cache";
  CacheManagerFeature::MANAGER->destroyCache(_cache);
  _cache.reset();
  _cachePresent = false;
}

// blacklist given key from transactional cache
void RocksDBCollection::blackListKey(char const* data, std::size_t len) const {
  if (useCache()) {
    TRI_ASSERT(_cache != nullptr);
    bool blacklisted = false;
    while (!blacklisted) {
      auto status = _cache->blacklist(data, static_cast<uint32_t>(len));
      if (status.ok()) {
        blacklisted = true;
      } else if (status.errorNumber() == TRI_ERROR_SHUTTING_DOWN) {
        destroyCache();
        break;
      }
    }
  }
}

void RocksDBCollection::trackWaitForSync(arangodb::transaction::Methods* trx,
                                         OperationOptions& options) {
  if (_logicalCollection.waitForSync() && !options.isRestore) {
    options.waitForSync = true;
  }

  if (options.waitForSync) {
    trx->state()->waitForSync(true);
  }
}

/// @brief can use non transactional range delete in write ahead log
bool RocksDBCollection::canUseRangeDeleteInWal() const {
  if (ServerState::instance()->isSingleServer()) {
    // disableWalFilePruning is used by createIndex
    return _numIndexCreations.load(std::memory_order_acquire) == 0;
  }
  return false;
}
