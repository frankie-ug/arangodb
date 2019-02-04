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
#include "VocBase/LogicalCollection.h"

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
                                                     bool consultAqlWriteFilter,
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
      _consultAqlWriteFilter(consultAqlWriteFilter),
      _inputRegisterId(inputRegister.get()),
      _outputRegisterId(outputRegisterOld.get()),
      _doCount(doCount),
      _returnInheritedResults(returnInheritedResults) {}


ModificationExecutorBase::ModificationExecutorBase(Fetcher& fetcher, Infos& infos)
    : _infos(infos), _fetcher(fetcher){};


void Insert::work(ModificationExecutor<Insert>& executor) {
  auto& infos = executor._infos;

  executor._operations.clear();
  executor._tempBuilder.clear();
  executor._tempBuilder.isOpenArray();

  executor._fetcher.forRowinBlock([&executor, &infos](InputAqlItemRow&& row){
      auto const& inVal = row.getValue(infos._inputRegisterId);

      if (!infos._consultAqlWriteFilter || infos._aqlCollection->getCollection()->skipForAqlWrite(inVal.slice(), StaticStrings::Empty)) {
        executor._operations.push_back(ModificationExecutorBase::APPLY_RETURN);
        // TODO This may be optimized with externals
        executor._tempBuilder.add(inVal.slice());
      } else {
        // not relevant for ourselves... just pass it on to the next block
        executor._operations.push_back(ModificationExecutorBase::IGNORE_RETURN);
      }

  });

  executor._tempBuilder.close();


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
  std::tie(state, inputRow) = _fetcher.fetchBlock();

//  throwIfKilled();  // check if we were aborted

  auto& inVarValue = inputRow.getValue(_infos._inputRegisterId);

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
    TRI_IF_FAILURE("ReturnBlock::getSome") {
      THROW_ARANGO_EXCEPTION(TRI_ERROR_DEBUG);
    }

    return Modifier::work(*this, inputRow, output, inVarValue);

  }

  if (_infos._doCount) {
    stats.incrCounted();
  }
  return {state, std::move(stats)};
}
