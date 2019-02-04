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

#include "SingleBlockFetcher.h"

#include "Aql/AqlItemBlock.h"
#include "Aql/BlockFetcher.h"
#include "Aql/InputAqlItemRow.h"
#include "Aql/SortExecutor.h"

using namespace arangodb;
using namespace arangodb::aql;

SingleBlockFetcher::SingleBlockFetcher(BlockFetcher<false>& executionBlock)
    : _blockFetcher(&executionBlock),
      _currentBlock(nullptr),
      _upstreamState(ExecutionState::HASMORE) {}

RegisterId SingleBlockFetcher::getNrInputRegisters() const {
  return _blockFetcher->getNrInputRegisters();
}

std::pair<ExecutionState, std::shared_ptr<AqlItemBlockShell>> SingleBlockFetcher::fetchBlock() {
  if (_upstreamState == ExecutionState::DONE) {
    TRI_ASSERT(_currentBlock == nullptr);
    return {_upstreamState, _currentBlock};
  }

  auto res = _blockFetcher->fetchBlock();
  _upstreamState = res.first;
  _currentBlock = res.second;
  return res;
}

InputAqlItemRow SingleBlockFetcher::accessRow(std::size_t index) {
  TRI_ASSERT(_currentBlock);
  TRI_ASSERT(index < _currentBlock->block().size());
  return InputAqlItemRow{_currentBlock, index};
}

void SingleBlockFetcher::forRowinBlock(std::function<void(InputAqlItemRow&&)> func) {
  TRI_ASSERT(_currentBlock);
  for (std::size_t index = 0; index < _currentBlock->block().size(); ++index) {
    func(InputAqlItemRow{_currentBlock, index});
  }
}
