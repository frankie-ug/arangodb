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
/// @author Tobias Gödderz
/// @author Michael Hackstein
////////////////////////////////////////////////////////////////////////////////

#include "MultiDependencyBlockFetcherMock.h"

#include <lib/Basics/Common.h>

#include "catch.hpp"

namespace arangodb {
namespace tests {
namespace aql {

using namespace arangodb::aql;

/* * * * *
 * Mocks
 * * * * */

// Note that _itemBlockManager gets passed first to the parent constructor,
// and only then gets instantiated. That is okay, however, because the
// constructor will not access it.
MultiDependencyBlockFetcherMock::MultiDependencyBlockFetcherMock(
    std::vector<ExecutionBlock*> const& dependencies,
    arangodb::aql::ResourceMonitor& monitor, ::arangodb::aql::RegisterId nrRegisters)
    : BlockFetcher(dependencies, _itemBlockManager,
                   std::shared_ptr<std::unordered_set<RegisterId>>(), nrRegisters),
      _deps(),
      _monitor(monitor),
      _itemBlockManager(&_monitor),
      _nrDependencies(dependencies.size()) {
  _deps.reserve(_nrDependencies);
  for (size_t i = 0; i < nrDependencies; ++i) {
    _deps.emplace_back(DepInfo{});
  }
}  // namespace aql

MultiDependencyBlockFetcherMock::DepInfo const& MultiDependencyBlockFetcherMock::getDep(size_t i) const {
  TRI_ASSERT(i < _nrDependencies);
  return _deps.at(i);
}

MultiDependencyBlockFetcherMock::DepInfo& MultiDependencyBlockFetcherMock::getDep(size_t i) {
  TRI_ASSERT(i < _nrDependencies);
  return _deps.at(i);
}

std::pair<ExecutionState, std::shared_ptr<InputAqlItemBlockShell>>
MultiDependencyBlockFetcherMock::fetchBlockFromDependency(size_t i) {
  auto& dep = getDep(i);
  dep._numFetchBlockCalls++;

  if (dep._itemsToReturn.empty()) {
    return {ExecutionState::DONE, nullptr};
  }

  std::pair<ExecutionState, std::shared_ptr<InputAqlItemBlockShell>> returnValue =
      std::move(dep._itemsToReturn.front());
  dep._itemsToReturn.pop_front();

  if (returnValue.second != nullptr) {
    auto blockPtr = reinterpret_cast<AqlItemBlockPtr>(returnValue.second.get());
    bool didInsert;
    std::tie(std::ignore, didInsert) = dep._fetchedBlocks.insert(blockPtr);
    // BlockFetcherMock::fetchBlock() should not return the same block twice:
    REQUIRE(didInsert);
  }

  return returnValue;
}

/* * * * * * * * * * * * *
 * Test helper functions
 * * * * * * * * * * * * */

MultiDependencyBlockFetcherMock& MultiDependencyBlockFetcherMock::shouldReturn(
    size_t i, ExecutionState state, std::unique_ptr<AqlItemBlock> block) {
  auto& dep = getDep(i);
  // Should only be called once on each instance
  TRI_ASSERT(dep._itemsToReturn.empty());

  return andThenReturn(i, state, std::move(block));
}

MultiDependencyBlockFetcherMock& MultiDependencyBlockFetcherMock::shouldReturn(
    size_t i, std::pair<ExecutionState, std::shared_ptr<InputAqlItemBlockShell>> firstReturnValue) {
  auto& dep = getDep(i);
  // Should only be called once on each instance
  TRI_ASSERT(dep._itemsToReturn.empty());

  return andThenReturn(i, std::move(firstReturnValue));
}

MultiDependencyBlockFetcherMock& MultiDependencyBlockFetcherMock::shouldReturn(
    size_t i,
    std::vector<std::pair<ExecutionState, std::shared_ptr<InputAqlItemBlockShell>>> firstReturnValues) {
  auto& dep = getDep(i);
  // Should only be called once on each instance
  TRI_ASSERT(dep._itemsToReturn.empty());

  return andThenReturn(i, std::move(firstReturnValues));
}

MultiDependencyBlockFetcherMock& MultiDependencyBlockFetcherMock::andThenReturn(
    size_t i, ExecutionState state, std::unique_ptr<AqlItemBlock> block) {
  auto inputRegisters = std::make_shared<std::unordered_set<RegisterId>>();
  // add all registers as input
  for (RegisterId i = 0; i < getNrInputRegisters(); i++) {
    inputRegisters->emplace(i);
  }
  std::shared_ptr<InputAqlItemBlockShell> blockShell;
  if (block != nullptr) {
    blockShell = std::make_shared<InputAqlItemBlockShell>(_itemBlockManager,
                                                          std::move(block), inputRegisters);
  }
  return andThenReturn(i, {state, std::move(blockShell)});
}

MultiDependencyBlockFetcherMock& MultiDependencyBlockFetcherMock::andThenReturn(
    size_t i, std::pair<ExecutionState, std::shared_ptr<InputAqlItemBlockShell>> additionalReturnValue) {
  auto& dep = getDep(i);
  dep._itemsToReturn.emplace_back(std::move(additionalReturnValue));

  return *this;
}

MultiDependencyBlockFetcherMock& MultiDependencyBlockFetcherMock::andThenReturn(
    size_t i,
    std::vector<std::pair<ExecutionState, std::shared_ptr<InputAqlItemBlockShell>>> additionalReturnValues) {
  for (auto& it : additionalReturnValues) {
    andThenReturn(i, std::move(it));
  }

  return *this;
}

bool MultiDependencyBlockFetcherMock::allBlocksFetched(size_t i) const {
  auto& dep = getDep(i);
  return dep._itemsToReturn.empty();
}
size_t MultiDependencyBlockFetcherMock::numFetchBlockCalls(size_t i) const {
  auto& dep = getDep(i);
  return dep._numFetchBlockCalls;
}
bool MultiDependencyBlockFetcherMock::allBlocksFetched() const {
  for (size_t i = 0; i < _nrDependencies; ++i) {
    if (!allBlocksFetched(i)) {
      return false;
    }
  }
  return true;
}

size_t MultiDependencyBlockFetcherMock::numFetchBlockCalls() const {
  size_t res = 0;
  for (size_t i = 0; i < _nrDependencies; ++i) {
    res += numFetchBlockCalls(i);
  }
  return res;
}

}  // namespace aql
}  // namespace tests
}  // namespace arangodb
