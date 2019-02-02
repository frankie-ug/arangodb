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
/// @author Heiko Kernbach
/// @author Jan Christoph Uhde
////////////////////////////////////////////////////////////////////////////////

#include "MultiDependencySingleRowFetcher.h"

#include "Aql/BlockFetcher.h"
#include "Aql/SingleRowFetcher.h"

using namespace arangodb;
using namespace arangodb::aql;

MultiDependencySingleRowFetcher::MultiDependencySingleRowFetcher(BlockFetcher& executionBlock)
    : _blockFetcher(&executionBlock),
      _nrDependencies(_blockFetcher->numberDependencies()) {
  _upstream.reserve(_nrDependencies);
  for (size_t i = 0; i < _nrDependencies; ++i) {
    _upstream.emplace_back(SingleRowFetcher{executionBlock, i});
  }
}

std::pair<ExecutionState, InputAqlItemRow> MultiDependencySingleRowFetcher::fetchRowForDependency(size_t depIndex) {
  SingleRowFetcher& fetcher = getUpstream(depIndex);
  return fetcher.fetchRow();
}

RegisterId MultiDependencySingleRowFetcher::getNrInputRegisters() const {
  return _blockFetcher->getNrInputRegisters();
}

SingleRowFetcher& MultiDependencySingleRowFetcher::getUpstream(size_t dep) {
  TRI_ASSERT(dep < _nrDependencies);
  TRI_ASSERT(_nrDependencies == _upstream.size());
  return _upstream.at(dep);
}