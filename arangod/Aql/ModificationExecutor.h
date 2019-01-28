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

#ifndef ARANGOD_AQL_MODIFICATION_EXECUTOR_H
#define ARANGOD_AQL_MODIFICATION_EXECUTOR_H

#include "Aql/ExecutionState.h"
#include "Aql/ExecutorInfos.h"
#include "Aql/ModificationNodes.h"
#include "Aql/ModificationOptions.h"
#include "Aql/SingleRowFetcher.h"
#include "Aql/Stats.h"
#include "Utils/OperationOptions.h"
#include "velocypack/Slice.h"
#include "velocypack/velocypack-aliases.h"


#include <boost/optional.hpp>

namespace arangodb {
namespace transaction {
class Methods;
}

namespace aql {

class AqlItemMatrix;
class ExecutorInfos;
class NoStats;
class OutputAqlItemRow;
struct SortRegister;

struct Insert;
struct Remove;
struct UpdateReplace;
struct Update;
struct Upsert;
struct Replace;

OperationOptions convertOptions(Insert&, ModificationOptions const& in,
                                ExecutionNode* outVariableOld) {
  OperationOptions out;

  // insert node
  // TO INFOS --  bool const ignoreDocumentNotFound = in.ignoreDocumentNotFound;
  bool const producesOutput = outVariableOld != nullptr;

  out.silent = !producesOutput;
  out.waitForSync = in.waitForSync;
  out.ignoreRevs = in.ignoreRevs;
  out.returnOld = producesOutput;
  out.isRestore = in.useIsRestore;

  return out;
}

class ModificationExecutorInfos : public ExecutorInfos {
 public:
  ModificationExecutorInfos(boost::optional<RegisterId> inputRegister,
                            boost::optional<RegisterId> outputRegistersNew,
                            boost::optional<RegisterId> outputRegistersOld,
                            RegisterId nrInputRegisters,
                            RegisterId nrOutputRegisters,
                            std::unordered_set<RegisterId> registersToClear,
                            bool doCount, bool returnInheritedResults);



  ModificationExecutorInfos() = delete;
  ModificationExecutorInfos(ModificationExecutorInfos&&) = default;
  ModificationExecutorInfos(ModificationExecutorInfos const&) = delete;
  ~ModificationExecutorInfos() = default;

  /// @brief the variable produced by Return
  Variable const* _inVariable;
  bool _count;
  RegisterId _inputRegisterId;
  RegisterId _outputRegisterId;
  bool _doCount;
  bool _returnInheritedResults;
};

struct ModificationExecutorBase {
  using Infos = ModificationExecutorInfos;
  using Fetcher = SingleRowFetcher;

  ModificationExecutorBase(Fetcher&, Infos&);

 protected:
  enum ModOperationType : uint8_t {
    IGNORE_SKIP = 0,    // do not apply, do not produce a result - used for
                        // skipping over suppressed errors
    IGNORE_RETURN = 1,  // do not apply, but pass the row to the next block -
                        // used for smart graphs and such
    APPLY_RETURN = 2,   // apply it and return the result, used for all
                        // non-UPSERT operations
    APPLY_UPDATE = 3,  // apply it and return the result, used only used for UPSERT
    APPLY_INSERT = 4,  // apply it and return the result, used only used for UPSERT
  };

  ModificationExecutorInfos& _infos;
  Fetcher& _fetcher;

  /// @brief skips over the taken rows if the input value is no
  /// array or empty. updates dstRow in this case and returns true!
  bool skipEmptyValues(VPackSlice const& values, size_t n, AqlItemBlock const* src,
                       AqlItemBlock* dst, size_t& dstRow);

  /// @brief processes the final result
  void trimResult(std::unique_ptr<AqlItemBlock>& result, size_t numRowsWritten);

  /// @brief extract a key from the AqlValue passed
  int extractKey(AqlValue const&, std::string& key);

  /// @brief extract a key and rev from the AqlValue passed
  int extractKeyAndRev(AqlValue const&, std::string& key, std::string& rev);

  /// @brief process the result of a data-modification operation
  void handleResult(int, bool, std::string const* errorMessage = nullptr);

  void handleBabyResult(std::unordered_map<int, size_t> const&, size_t,
                        bool ignoreAllErrors, bool ignoreDocumentNotFound = false);

  /// @brief determine the number of rows in a vector of blocks
  size_t countBlocksRows() const;
};

template <typename Modifier>
class ModificationExecutor : public ModificationExecutorBase {
  friend struct Insert;

 public:
  using Modification = Modifier;
  using Stats = CountStats;

  ModificationExecutor(Fetcher&, Infos&);
  ~ModificationExecutor();

  /**
   * @brief produce the next Row of Aql Values.
   *
   * @return ExecutionState,
   *         if something was written output.hasValue() == true
   */
  std::pair<ExecutionState, Stats> produceRow(OutputAqlItemRow& output);
};

struct Insert {
  void work(ModificationExecutor<Insert>& executor);
};

struct Remove {};
struct UpdateReplace {};
struct Update {};
struct Upsert {};
struct Replace {};

}  // namespace aql
}  // namespace arangodb

#endif
