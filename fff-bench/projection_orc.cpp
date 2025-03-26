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

int main() {
  // Define input ORC files to read
  std::vector<std::string> fileNumbers = {// "10"};
                                          "2333",
                                          "10",
                                          "20",
                                          "100",
                                          "1000",
                                          "5000",
                                          "10000",
                                          "20000",
                                          "50000",
                                          "100000"};

  // Initialize memory management and register ORC reader
  velox::filesystems::registerLocalFileSystem();
  velox::orc::registerOrcReaderFactory();
  velox::memory::MemoryManager::testingSetInstance({});
  auto rootPool = velox::memory::memoryManager()->addRootPool("OrcReader");
  auto leafPool = rootPool->addLeafChild("leaf");

  // Open CSV file for writing results
  std::ofstream csvFile("orc_projection_times.csv");
  csvFile
      << "filename,num_columns,total_rows,read_time_ms,throughput_mrows_per_sec\n";
  std::unordered_map<int, std::shared_ptr<velox::common::ScanSpec>> scanSpecs;
  std::unordered_map<int, double> loadSchemaTimes;

  for (const auto& num : fileNumbers) {
    // Get total number of columns and select 10 random ones
    uint32_t totalColumns = std::stoi(num);
    auto selectedColumns = getRandomColumns(totalColumns, 10);
    std::cout << "Selected columns: ";
    for (auto col : selectedColumns) {
      std::cout << col << " ";
    }
    std::cout << std::endl;

    // Create reader to get schema first
    auto factory = velox::dwio::common::getReaderFactory(
        velox::dwio::common::FileFormat::ORC);
    facebook::velox::dwio::common::ReaderOptions readerOpts{leafPool.get()};
    readerOpts.setFileFormat(velox::dwio::common::FileFormat::ORC);

    auto startTime0 = std::chrono::high_resolution_clock::now();
    auto reader = factory->createReader(
        std::make_unique<BufferedInput>(
            std::make_shared<LocalReadFile>(
                "/home/xinyu/fff-devel/data/copy/" + num + ".orc"),
            readerOpts.memoryPool()),
        readerOpts);
    auto endTime0 = std::chrono::high_resolution_clock::now();
    std::cout << "Schema reader time: " << toMilliseconds(endTime0 - startTime0)
              << " ms" << std::endl;

    // Create scan spec for selected columns
    startTime0 = std::chrono::high_resolution_clock::now();
    auto scanSpec = std::make_shared<velox::common::ScanSpec>("");
    auto rowType = reader->rowType();
    for (auto col : selectedColumns) {
      scanSpec->addField(rowType->nameOf(col), col);
    }
    endTime0 = std::chrono::high_resolution_clock::now();
    std::cout << "Scan spec time: " << toMilliseconds(endTime0 - startTime0)
              << " ms" << std::endl;
    loadSchemaTimes[std::stoi(num)] = toMilliseconds(endTime0 - startTime0);
    scanSpecs[std::stoi(num)] = scanSpec;
  }

  // Process each ORC file
  for (const auto& num : fileNumbers) {
    try {
      std::cout << "Reading " << num << std::endl;
      auto num_int = std::stoi(num);
      auto startTime = std::chrono::high_resolution_clock::now();

      // Create reader with selected columns
      auto factory = velox::dwio::common::getReaderFactory(
          velox::dwio::common::FileFormat::ORC);
      facebook::velox::dwio::common::ReaderOptions readerOpts{leafPool.get()};
      readerOpts.setFileFormat(velox::dwio::common::FileFormat::ORC);

      auto reader = factory->createReader(
          std::make_unique<BufferedInput>(
              std::make_shared<LocalReadFile>(
                  "/home/xinyu/fff-devel/data_8rows/" + num + ".orc"),
              readerOpts.memoryPool()),
          readerOpts);

      // Create row reader with scan spec
      RowReaderOptions rowReaderOpts;
      rowReaderOpts.setScanSpec(scanSpecs[num_int]);
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

      auto endTime = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
          endTime - startTime);

      double ms = toMilliseconds(duration) + loadSchemaTimes[num_int];
      double throughput = calculateThroughput(totalRows, ms);

      // Write results to CSV
      csvFile << num << "," << num_int << "," << totalRows << "," << ms << ","
              << throughput << "\n";

      std::cout << "Successfully read " << totalRows << " rows in " << ms
                << " ms (" << throughput << " M rows/s)" << std::endl;

    } catch (const std::exception& e) {
      std::cerr << "Error reading " << num << ": " << e.what() << std::endl;
      csvFile << num << ",error,error,error,error\n";
    }
  }

  csvFile.close();
  std::cout << "Results written to orc_projection_times.csv" << std::endl;
  return 0;
}