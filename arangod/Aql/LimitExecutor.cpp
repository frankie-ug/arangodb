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
/// @author Tobias Goedderz
/// @author Michael Hackstein
/// @author Heiko Kernbach
/// @author Jan Christoph Uhde
////////////////////////////////////////////////////////////////////////////////

#include "LimitExecutor.h"

#include "Aql/AqlValue.h"
#include "Aql/ExecutorInfos.h"
#include "Aql/InputAqlItemRow.h"
#include "Aql/SingleRowFetcher.h"
#include "Basics/Common.h"

#include <lib/Logger/LogMacros.h>

#include <utility>

using namespace arangodb;
using namespace arangodb::aql;

LimitExecutorInfos::LimitExecutorInfos(RegisterId nrInputRegisters, RegisterId nrOutputRegisters,
                                       std::unordered_set<RegisterId> registersToClear,
                                       size_t offset, size_t limit, bool fullCount, size_t queryDepth)
        : ExecutorInfos(std::make_shared<std::unordered_set<RegisterId>>(),
                        std::make_shared<std::unordered_set<RegisterId>>(), nrInputRegisters,
                        nrOutputRegisters, std::move(registersToClear)),
          _remainingOffset(offset),
          _limit(limit),
          _queryDepth(queryDepth),
          _fullCount(fullCount) {}

LimitExecutor::LimitExecutor(Fetcher& fetcher, Infos& infos)
    : _infos(infos), _fetcher(fetcher){};
LimitExecutor::~LimitExecutor() = default;

std::pair<ExecutionState, LimitStats> LimitExecutor::produceRow(OutputAqlItemRow& output) {
  TRI_IF_FAILURE("LimitExecutor::produceRow") {
    THROW_ARANGO_EXCEPTION(TRI_ERROR_DEBUG);
  }
  LimitStats stats{};
  InputAqlItemRow input{CreateInvalidInputRowHint{}};

  if (_counter == _infos.getLimit() && !_infos.isFullCountEnabled()) {
    return {ExecutionState::DONE, stats};
  }

  ExecutionState state = ExecutionState::HASMORE;
  while (state != ExecutionState::DONE) {
    std::tie(state, input) = _fetcher.fetchRow(maxRowsLeftToFetch());

    if (state == ExecutionState::WAITING) {
      return {state, stats};
    }

    if (!input) {
      TRI_ASSERT(state == ExecutionState::DONE);
      return {state, stats};
    }
    TRI_ASSERT(input.isInitialized());

    if (_infos.getRemainingOffset() > 0) {
      _infos.decrRemainingOffset();

      if (_infos.isFullCountEnabled() && _infos.getQueryDepth() == 0) {
        stats.incrFullCount();
      }
      continue;
    }

    if (_counter < _infos.getLimit()) {
      output.copyRow(input);
      _counter++;
      if (_infos.getQueryDepth() == 0 && _infos.isFullCountEnabled()) {
        stats.incrFullCount();
      }

      if (_counter == _infos.getLimit() && !_infos.isFullCountEnabled()) {
        return {ExecutionState::DONE, stats};
      }
      return {state, stats};
    }
    TRI_ASSERT(_infos.isFullCountEnabled());

    if (_infos.getQueryDepth() == 0) {
      stats.incrFullCount();
    }
  }

  TRI_ASSERT(state == ExecutionState::DONE);
  return {state, stats};
}
