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

#include <lib/Logger/LogMacros.h>
#include "TestEmptyExecutorHelper.h"

#include "Basics/Common.h"

#include "Aql/InputAqlItemRow.h"
#include "Aql/AqlValue.h"
#include "Aql/ExecutorInfos.h"
#include "Aql/SingleRowFetcher.h"

#include <utility>

using namespace arangodb;
using namespace arangodb::aql;

TestEmptyExecutorHelper::TestEmptyExecutorHelper(Fetcher& fetcher, Infos& infos) : _infos(infos), _fetcher(fetcher){};
TestEmptyExecutorHelper::~TestEmptyExecutorHelper() = default;

std::pair<ExecutionState, FilterStats> TestEmptyExecutorHelper::produceRow(OutputAqlItemRow &output) {
  TRI_IF_FAILURE("TestEmptyExecutorHelper::produceRow") {
     THROW_ARANGO_EXCEPTION(TRI_ERROR_DEBUG);
  }
  ExecutionState state = ExecutionState::DONE;
  FilterStats stats{};

  return {state, stats};

  // will not reach this part of code, it is still here to prevent
  // a compile warning. We do not want to test the fetcher here. But it
  // must be included due template scheme.
  InputAqlItemRow input{CreateInvalidInputRowHint{}};
  std::tie(state, input) = _fetcher.fetchRow();
}

TestEmptyExecutorHelperInfos::TestEmptyExecutorHelperInfos(
    RegisterId inputRegister_, RegisterId nrInputRegisters,
    RegisterId nrOutputRegisters,
    std::unordered_set<RegisterId> registersToClear)
    : ExecutorInfos(
          std::make_shared<std::unordered_set<RegisterId>>(inputRegister_),
          nullptr, nrInputRegisters, nrOutputRegisters,
          std::move(registersToClear)),
      _inputRegister(inputRegister_) {}
