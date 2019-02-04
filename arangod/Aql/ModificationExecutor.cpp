////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2018 ArangoDB GmbH, Cologne, Germany
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

#include "ModificationExecutor.h"
#include "Aql/AqlValue.h"
#include "Aql/OutputAqlItemRow.h"
#include "Aql/Collection.h"
#include "Basics/Common.h"

#include <algorithm>

using namespace arangodb;
using namespace arangodb::aql;


ModificationExecutorInfos::ModificationExecutorInfos(boost::optional<RegisterId> inputRegister,
                                                     boost::optional<RegisterId> outputRegisterNew,
                                                     boost::optional<RegisterId> outputRegisterOld,
                                                     RegisterId nrInputRegisters,
                                                     RegisterId nrOutputRegisters,
                                                     std::unordered_set<RegisterId> registersToClear,
                                                     transaction::Methods* trx,
                                                     OperationOptions options,
                                                     aql::Collection const* aqlCollection,
                                                     bool producesResults,
                                                     bool doCount, bool returnInheritedResults)
    : ExecutorInfos(inputRegister.has_value() ? make_shared_unordered_set({inputRegister.get()}) : make_shared_unordered_set(),
                    make_shared_unordered_set({outputRegisterOld.get()}),
                    nrInputRegisters,
                    nrOutputRegisters,
                    std::unordered_set<RegisterId>{} /*to clear*/,  // std::move(registersToClear) // use this once register planning is fixed
                    std::unordered_set<RegisterId>{} /*to keep*/
                    ),
      _trx(trx),
      _options(options),
      _aqlCollection(aqlCollection),
      _producesResults(producesResults || !_options.silent),
      _inputRegisterId(inputRegister.get()),
      _outputRegisterId(outputRegisterOld.get()),
      _doCount(doCount),
      _returnInheritedResults(returnInheritedResults) {}


ModificationExecutorBase::ModificationExecutorBase(Fetcher& fetcher, Infos& infos)
    : _infos(infos), _fetcher(fetcher){};



void Insert::work(ModificationExecutor<Insert>& executor) {
    auto& infos = executor._infos;
//  size_t const count = countBlocksRows();
//
//  if (count == 0) {
//    return nullptr;
//  }
//
//  auto ep = ExecutionNode::castTo<InsertNode const*>(getPlanNode());
//  auto it = ep->getRegisterPlan()->varInfo.find(ep->_inVariable->id);
//  TRI_ASSERT(it != ep->getRegisterPlan()->varInfo.end());
//  RegisterId const registerId = it->second.registerId;
//
//

//
  std::unique_ptr<AqlItemBlock> result;
  if (infos._producesResults) {
//    result.reset(requestBlock(count, getNrOutputRegisters()));
  }
//
//  // loop over all blocks
//  size_t dstRow = 0;
//
//  for (auto it = _blocks.begin(); it != _blocks.end(); ++it) {
//    auto* res = it->get();
//
//    throwIfKilled();  // check if we were aborted
//
    _tempBuilder.clear();
    _tempBuilder.openArray();
//
//    size_t const n = res->size();
//
//    _operations.clear();
//    _operations.reserve(n);
//
//    for (size_t i = 0; i < n; ++i) {
//      AqlValue const& a = res->getValueReference(i, registerId);
//
//      if (!ep->_options.consultAqlWriteFilter ||
//          !_collection->getCollection()->skipForAqlWrite(a.slice(), StaticStrings::Empty)) {
//        _operations.push_back(APPLY_RETURN);
//        // TODO This may be optimized with externals
      //  _tempBuilder.add(a.slice());
//      } else {
//        // not relevant for ourselves... just pass it on to the next block
//        _operations.push_back(IGNORE_RETURN);
//      }
//    }
//
    _tempBuilder.close();

    VPackSlice toInsert = _tempBuilder.slice();
//
//    if (skipEmptyValues(toInsert, n, res, result.get(), dstRow)) {
//      it->release();
//      returnBlock(res);
//      continue;
//    }
//
    OperationResult opRes = infos._trx->insert(infos._aqlCollection->name(), toInsert, infos._options);
//
//    handleBabyResult(opRes.countErrorCodes, static_cast<size_t>(toInsert.length()),
//                     ep->_options.ignoreErrors);
//
//    if (opRes.fail()) {
//      THROW_ARANGO_EXCEPTION(opRes.result);
//    }
//
//    VPackSlice resultList = opRes.slice();
//
//    if (skipEmptyValues(resultList, n, res, result.get(), dstRow)) {
//      it->release();
//      returnBlock(res);
//      continue;
//    }
//
//    TRI_ASSERT(resultList.isArray());
//    auto iter = VPackArrayIterator(resultList);
//    for (size_t i = 0; i < n; ++i) {
//      TRI_ASSERT(i < _operations.size());
//      if (_operations[i] == APPLY_RETURN && result != nullptr) {
//        TRI_ASSERT(iter.valid());
//        auto elm = iter.value();
//        bool wasError =
//            arangodb::basics::VelocyPackHelper::getBooleanValue(elm, StaticStrings::Error, false);
//
//        if (!wasError) {
//          inheritRegisters(res, result.get(), i, dstRow);
//
//          if (producesNew) {
//            // store $NEW
//            result->emplaceValue(dstRow, _outRegNew, elm.get("new"));
//          }
//          if (producesOld) {
//            // store $OLD
//            auto slice = elm.get("old");
//            if (slice.isNone()) {
//              result->emplaceValue(dstRow, _outRegOld, VPackSlice::nullSlice());
//            } else {
//              result->emplaceValue(dstRow, _outRegOld, slice);
//            }
//          }
//          ++dstRow;
//        }
//        ++iter;
//      } else if (_operations[i] == IGNORE_RETURN) {
//        TRI_ASSERT(result != nullptr);
//        inheritRegisters(res, result.get(), i, dstRow);
//        ++dstRow;
//      }
//    }
//
//    // done with block. now unlink it and return it to block manager
//    it->release();
//    returnBlock(res);
//  }
//
//  trimResult(result, dstRow);
//  return result;
}



/// @brief skips over the taken rows if the input value is no
/// array or empty. updates dstRow in this case and returns true!
bool ModificationExecutorBase::skipEmptyValues(VPackSlice const& values, size_t n,
                                        AqlItemBlock const* src,
                                        AqlItemBlock* dst, size_t& dstRow) {
  //TRI_ASSERT(src != nullptr);
  //TRI_ASSERT(_operations.size() == n);

  //if (values.isArray() && values.length() > 0) {
  //  return false;
  //}

  //if (dst == nullptr) {
  //  // fast-track exit. we don't have any output to write, so we
  //  // better try not to copy any of the register values from src to dst
  //  return true;
  //}

  //for (size_t i = 0; i < n; ++i) {
  //  if (_operations[i] != IGNORE_SKIP) {
  //    inheritRegisters(src, dst, i, dstRow);
  //    ++dstRow;
  //  }
  //}

  return true;
}

void ModificationExecutorBase::trimResult(std::unique_ptr<AqlItemBlock>& result, size_t numRowsWritten) {
  //if (result == nullptr) {
  //  return;
  //}
  //if (numRowsWritten == 0) {
  //  AqlItemBlock* block = result.release();
  //  returnBlock(block);
  //} else if (numRowsWritten < result->size()) {
  //  result->shrink(numRowsWritten);
  //}
}

/// @brief determine the number of rows in a vector of blocks
size_t ModificationExecutorBase::countBlocksRows() const {
  size_t count = 0;
  //for (auto const& it : _blocks) {
  //  count += it->size();
  //}
  return count;
}



template <typename Modifier>
ModificationExecutor<Modifier>::ModificationExecutor(Fetcher& fetcher, Infos& infos)
    : ModificationExecutorBase(fetcher, infos){};

template <typename Modifier>
ModificationExecutor<Modifier>::~ModificationExecutor() = default;

template <typename Modifier>
std::pair<ExecutionState, typename ModificationExecutor<Modifier>::Stats> ModificationExecutor<Modifier>::produceRow(OutputAqlItemRow& output) {
  ExecutionState state;
  ModificationExecutor::Stats stats;
  InputAqlItemRow inputRow = InputAqlItemRow{CreateInvalidInputRowHint{}};
  std::tie(state, inputRow) = _fetcher.fetchRow();

  if (state == ExecutionState::WAITING) {
    TRI_ASSERT(!inputRow);
    return {state, std::move(stats)};
  }

  if (!inputRow) {
    TRI_ASSERT(state == ExecutionState::DONE);
    return {state, std::move(stats)};
  }

  if (_infos._returnInheritedResults) {
    output.copyRow(inputRow);
  } else {
    AqlValue const& val = inputRow.getValue(_infos._inputRegisterId);
    TRI_IF_FAILURE("ReturnBlock::getSome") {
      THROW_ARANGO_EXCEPTION(TRI_ERROR_DEBUG);
    }
    output.setValue(_infos._outputRegisterId, inputRow, val);
  }

  if (_infos._doCount) {
    stats.incrCounted();
  }
  return {state, std::move(stats)};
}
