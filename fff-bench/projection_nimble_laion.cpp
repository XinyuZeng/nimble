/// Check the stream sizes of the laion data

#include <velox/dwio/common/ColumnSelector.h>
#include <velox/vector/BaseVector.h>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <random>
#include <string> 
#include <unordered_map>
#include "dwio/nimble/velox/VeloxReader.h"
#include "velox/common/file/File.h"
#include "velox/common/memory/Memory.h"

using namespace facebook;
using namespace facebook::velox;
using namespace facebook::nimble;

int main(int argc, char** argv) {
  if (argc != 3) {
    std::cout << "Usage: " << argv[0] << " <input_file> <proj_idx>"
              << std::endl;
    return 1;
  }
  uint64_t proj_idx = std::stoul(argv[2]);
  // Define input Nimble file to read
  std::string input = argv[1];
  std::vector<uint64_t> selectedColumns = {proj_idx};
  // Initialize memory management
  velox::memory::MemoryManager::testingSetInstance({});
  auto rootPool = velox::memory::memoryManager()->addRootPool("NimbleReader");
  auto leafPool = rootPool->addLeafChild("leaf");

  // Create reader to get schema first
  auto readFile = std::make_shared<velox::LocalReadFile>(input);
  // A lot of time will be spent here if we do not pass in projection
  VeloxReader schemaReader(*leafPool, readFile.get());
  auto selector = std::make_shared<dwio::common::ColumnSelector>(
      schemaReader.type(), selectedColumns);
  std::cout << "Projecting columns: " << std::endl;
  for (auto col : selectedColumns) {
    std::cout << schemaReader.type()->names()[col] << std::endl;
  }
  // Process each Nimble file
  std::cout << "Reading " << input << std::endl;
  // Create new reader with selected columns
  VeloxReader reader(*leafPool, readFile.get(), selector);

  // Read batches and measure time
  constexpr int32_t batchSize = 64 * 1024;
  std::vector<velox::VectorPtr> batches;
  velox::VectorPtr batch = nullptr;
  size_t totalRows = 0;

  while (reader.next(batchSize, batch)) {
    if (batch) {
      totalRows += batch->size();
      batches.push_back(batch);
      batch = nullptr;
    }
  }

  std::cout << "Successfully read " << totalRows << " rows" << std::endl;

  return 0;
}