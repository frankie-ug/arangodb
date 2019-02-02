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

#ifndef ARANGOD_AQL_TESTS_MULTI_DEPENDENCY_BLOCK_FETCHER_MOCK_H
#define ARANGOD_AQL_TESTS_MULTI_DEPENDENCY_BLOCK_FETCHER_MOCK_H

#include "Aql/BlockFetcher.h"
#include "Aql/ExecutionState.h"
#include "Aql/types.h"

#include <stdint.h>

namespace arangodb {
namespace tests {
namespace aql {

class MultiDependencyBlockFetcherMock : public ::arangodb::aql::BlockFetcher {
 private:
  using AqlItemBlockPtr = uintptr_t;
  using FetchBlockReturnItem =
      std::pair<arangodb::aql::ExecutionState, std::shared_ptr<arangodb::aql::InputAqlItemBlockShell>>;
  struct DepInfo {
    std::deque<FetchBlockReturnItem> _itemsToReturn;
    std::unordered_set<AqlItemBlockPtr> _fetchedBlocks;
    size_t _numFetchBlockCalls;
    DepInfo() : _itemsToReturn(), _fetchedBlocks(), _numFetchBlockCalls(0) {}
  };

 public:
  // NOTE dependencies are allowed to be nullptr, but the size needs to be correct
  explicit MultiDependencyBlockFetcherMock(std::vector<ExecutionBlock*> const& dependencies,
                                           arangodb::aql::ResourceMonitor& monitor,
                                           arangodb::aql::RegisterId nrRegisters);

 public:
  // mock methods
  std::pair<arangodb::aql::ExecutionState, std::shared_ptr<arangodb::aql::InputAqlItemBlockShell>>
  fetchBlockFromDependency(size_t dependencyIndex) override;

 private:
  DepInfo const& getDep(size_t i) const;
  DepInfo& getDep(size_t i);

 public:
  // additional test methods
  MultiDependencyBlockFetcherMock& shouldReturn(size_t dependencyIndex,
                                                arangodb::aql::ExecutionState,
                                                std::unique_ptr<arangodb::aql::AqlItemBlock>);
  MultiDependencyBlockFetcherMock& shouldReturn(size_t dependencyIndex, FetchBlockReturnItem);
  MultiDependencyBlockFetcherMock& shouldReturn(size_t dependencyIndex,
                                                std::vector<FetchBlockReturnItem>);
  MultiDependencyBlockFetcherMock& andThenReturn(size_t dependencyIndex, FetchBlockReturnItem);
  MultiDependencyBlockFetcherMock& andThenReturn(size_t dependencyIndex,
                                                 arangodb::aql::ExecutionState,
                                                 std::unique_ptr<arangodb::aql::AqlItemBlock>);
  MultiDependencyBlockFetcherMock& andThenReturn(size_t dependencyIndex,
                                                 std::vector<FetchBlockReturnItem>);

  bool allBlocksFetched(size_t dependencyIndex) const;
  size_t numFetchBlockCalls(size_t dependencyIndex) const;

  bool allBlocksFetched() const;
  size_t numFetchBlockCalls() const;

 private:
  std::vector<DepInfo> _deps;

  ::arangodb::aql::ResourceMonitor& _monitor;
  ::arangodb::aql::AqlItemBlockManager _itemBlockManager;
  size_t _nrDependencies;
};

}  // namespace aql
}  // namespace tests
}  // namespace arangodb

#endif  // ARANGOD_AQL_TESTS_MULTI_DEPENDENCY_BLOCK_FETCHER_MOCK_H
