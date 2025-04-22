/// Check the stream sizes of the laion data using ORC

#include <velox/dwio/common/ColumnSelector.h>
#include <velox/dwio/orc/reader/OrcReader.h>
#include <velox/vector/BaseVector.h>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include "velox/common/file/File.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/dwio/common/ScanSpec.h"

using namespace facebook;
using namespace facebook::velox;
using namespace facebook::velox::dwio::common;

int main(int argc, char** argv) {
  if (argc != 3) {
    std::cout << "Usage: " << argv[0] << " <input_file> <proj_idx>"
              << std::endl;
    return 1;
  }

  std::string input = argv[1];
  uint64_t proj_idx = std::stoul(argv[2]);

  // Initialize memory management and register ORC reader
  velox::filesystems::registerLocalFileSystem();
  velox::orc::registerOrcReaderFactory();
  velox::memory::MemoryManager::testingSetInstance({});
  auto rootPool = velox::memory::memoryManager()->addRootPool("OrcReader");
  auto leafPool = rootPool->addLeafChild("leaf");

  // Create reader to get schema first
  auto factory = velox::dwio::common::getReaderFactory(
      velox::dwio::common::FileFormat::ORC);
  facebook::velox::dwio::common::ReaderOptions readerOpts{leafPool.get()};
  readerOpts.setFileFormat(velox::dwio::common::FileFormat::ORC);

  try {
    // Create reader to get schema first
    auto reader = factory->createReader(
        std::make_unique<BufferedInput>(
            std::make_shared<LocalReadFile>(input), readerOpts.memoryPool()),
        readerOpts);

    // Create scan spec for selected column
    auto scanSpec = std::make_shared<velox::common::ScanSpec>("");
    auto rowType = reader->rowType();

    // Check if projection index is valid
    if (proj_idx >= rowType->size()) {
      std::cerr << "Error: Column index " << proj_idx
                << " is out of range. Max index is " << (rowType->size() - 1)
                << std::endl;
      return 1;
    }

    scanSpec->addField(rowType->nameOf(proj_idx), proj_idx);

    std::cout << "Projecting column: " << rowType->nameOf(proj_idx)
              << std::endl;

    // Create reader with selected column
    reader = factory->createReader(
        std::make_unique<BufferedInput>(
            std::make_shared<LocalReadFile>(input), readerOpts.memoryPool()),
        readerOpts);

    // Create row reader with scan spec
    RowReaderOptions rowReaderOpts;
    rowReaderOpts.setScanSpec(scanSpec);
    auto vec = {proj_idx};
    auto columnSelector =
        std::make_shared<ColumnSelector>(reader->rowType(), vec);
    rowReaderOpts.select(columnSelector);
    auto rowReader = reader->createRowReader(rowReaderOpts);

    // Read batches and measure time
    constexpr int32_t batchSize = 64 * 1024;
    std::vector<velox::VectorPtr> batches;
    velox::VectorPtr batch =
        BaseVector::create(reader->rowType(), batchSize, leafPool.get());
    size_t totalRows = 0;

    while (rowReader->next(batchSize, batch)) {
      if (batch) {
        totalRows += batch->size();
        batches.push_back(batch);
        batch =
            BaseVector::create(reader->rowType(), batchSize, leafPool.get());
      }
    }

    std::cout << "Successfully read " << totalRows << " rows" << std::endl;

  } catch (const std::exception& e) {
    std::cerr << "Error reading file: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}