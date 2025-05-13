/// ORC file wide table projection benchmark

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

// Helper function to format duration to milliseconds
double toMilliseconds(const std::chrono::nanoseconds& duration) {
  return std::chrono::duration<double, std::milli>(duration).count();
}

// Helper function to calculate throughput in M rows/s
double calculateThroughput(size_t rows, double milliseconds) {
  return (rows / 1e6) / (milliseconds / 1000.0);
}

// Helper function to get random column indices
std::vector<uint64_t> getRandomColumns(
    uint32_t numColumns,
    uint32_t numToSelect) {
  std::vector<uint64_t> columns;
  columns.reserve(numToSelect);

  // Create a random number generator
  std::mt19937 gen(42);
  std::uniform_int_distribution<> dis(0, numColumns - 1);

  // Keep track of selected columns to avoid duplicates
  std::unordered_set<uint64_t> selectedColumns;

  while (selectedColumns.size() < numToSelect) {
    uint64_t col = dis(gen);
    if (selectedColumns.insert(col).second) {
      columns.push_back(col);
    }
  }

  std::sort(columns.begin(), columns.end());
  return columns;
}

int main(int argc, char* argv[]) {
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " <row_number> <file_path>"
              << std::endl;
    return 1;
  }

  std::string num = argv[1];
  std::string file_path = argv[2];
  uint32_t row_id = std::stoi(num);

  // Initialize memory management and register ORC reader
  velox::filesystems::registerLocalFileSystem();
  velox::orc::registerOrcReaderFactory();
  velox::memory::MemoryManager::testingSetInstance({});
  auto rootPool = velox::memory::memoryManager()->addRootPool("OrcReader");
  auto leafPool = rootPool->addLeafChild("leaf");

  // Open CSV file for writing results
  // std::ofstream csvFile("orc_selection_times_" + num + ".csv");
  // csvFile << "filename,row_id,read_time_ms,throughput_mrows_per_sec\n";

  try {
    std::cout << "Reading " << num << std::endl;

    // Create reader to get schema first
    auto factory = velox::dwio::common::getReaderFactory(
        velox::dwio::common::FileFormat::ORC);
    facebook::velox::dwio::common::ReaderOptions readerOpts{leafPool.get()};
    readerOpts.setFileFormat(velox::dwio::common::FileFormat::ORC);

    auto startTime = std::chrono::high_resolution_clock::now();

    // Create reader with selected columns
    auto reader = factory->createReader(
        std::make_unique<BufferedInput>(
            std::make_shared<LocalReadFile>(file_path),
            readerOpts.memoryPool()),
        readerOpts);

    // Create row reader with scan spec
    RowReaderOptions rowReaderOpts;
    auto rowReader = reader->createRowReader(rowReaderOpts);
    auto dwrfRowReader = dynamic_cast<dwrf::DwrfRowReader*>(rowReader.get());
    if (!dwrfRowReader) {
      std::cerr << "Failed to cast to DwrfRowReader" << std::endl;
      return 1;
    }
    dwrfRowReader->seekToRow(row_id);

    // Read batches and measure time
    constexpr int32_t batchSize = 1;
    std::vector<velox::VectorPtr> batches;
    velox::VectorPtr batch =
        BaseVector::create(reader->rowType(), batchSize, leafPool.get());
    size_t totalRows = 0;

    if (dwrfRowReader->next(batchSize, batch)) {
      if (batch) {
        totalRows += batch->size();
        batches.push_back(batch);
        batch =
            BaseVector::create(reader->rowType(), batchSize, leafPool.get());
      }
    } else {
      std::cerr << "Failed to read row " << row_id << std::endl;
      return 1;
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
        endTime - startTime);

    double ms = toMilliseconds(duration);
    double throughput = calculateThroughput(totalRows, ms);

    // Write results to CSV
    // csvFile << num << "," << row_id << "," << ms << "," << throughput <<
    // "\n";

    std::cerr << "Random access orc file took " << int(ms) << "ms" << std::endl;
    // std::cout << "Successfully read " << totalRows << " rows in " << ms
    //           << " ms (" << throughput << " M rows/s)" << std::endl;

  } catch (const std::exception& e) {
    std::cerr << "Error reading " << num << ": " << e.what() << std::endl;
    // csvFile << num << ",error,error,error,error\n";
  }

  // csvFile.close();
  // std::cout << "Results written to orc_selection_times_" << num << ".csv"
  //           << std::endl;
  return 0;
}