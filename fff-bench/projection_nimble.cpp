#include <velox/dwio/common/ColumnSelector.h>
#include <velox/vector/BaseVector.h>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>
#include "dwio/nimble/velox/VeloxReader.h"
#include "velox/common/file/File.h"
#include "velox/common/memory/Memory.h"

using namespace facebook;
using namespace facebook::velox;
using namespace facebook::nimble;

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
  // Define input Nimble files to read
  std::vector<std::string> fileNumbers = {
      // "2333", "10", "20", "100", "1000", "10000", "50000", "100000"};
      "100000"};

  std::vector<std::string> inputs;
  for (const auto& num : fileNumbers) {
    inputs.push_back("/home/xinyu/fff-devel/data/" + num + ".nimble");
  }

  // Initialize memory management
  velox::memory::MemoryManager::testingSetInstance({});
  auto rootPool = velox::memory::memoryManager()->addRootPool("NimbleReader");
  auto leafPool = rootPool->addLeafChild("leaf");

  // Open CSV file for writing results
  std::ofstream csvFile("nimble_projection_times.csv");
  csvFile
      << "filename,num_columns,total_rows,read_time_ms,throughput_mrows_per_sec\n";

  // Process each Nimble file
  for (const auto& num : fileNumbers) {
    try {
      std::cout << "Reading " << num << std::endl;

      // Get total number of columns and select 10 random ones
      uint32_t totalColumns = std::stoi(num);
      auto selectedColumns = getRandomColumns(totalColumns, 10);
      std::cout << "Selected columns: ";
      for (auto col : selectedColumns) {
        std::cout << col << " ";
      }
      std::cout << std::endl;

      // Create reader to get schema first
      auto readFile = std::make_shared<velox::LocalReadFile>(
          "/home/xinyu/fff-devel/data/" + num + ".nimble");
      auto startTime0 = std::chrono::high_resolution_clock::now();
      // A lot of time will be spent here if we do not pass in projection
      VeloxReader schemaReader(*leafPool, readFile.get());
      auto loadSchemaTime = schemaReader.loadSchemaTime();
      auto endTime0 = std::chrono::high_resolution_clock::now();
      std::cout << "Schema reader time: "
                << toMilliseconds(endTime0 - startTime0) << " ms" << std::endl;
      auto selector = std::make_shared<dwio::common::ColumnSelector>(
          schemaReader.type(), selectedColumns);
      endTime0 = std::chrono::high_resolution_clock::now();
      std::cout << "Selector time: " << toMilliseconds(endTime0 - startTime0)
                << " ms" << std::endl;

      auto startTime = std::chrono::high_resolution_clock::now();

      // Create new reader with selected columns
      readFile = std::make_shared<velox::LocalReadFile>(
          "/home/xinyu/fff-devel/data/" + num + ".nimble");
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

      auto endTime = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
          endTime - startTime);

      double ms = toMilliseconds(duration) + loadSchemaTime;
      double throughput = calculateThroughput(totalRows, ms);

      // Write results to CSV
      csvFile << num << "," << selectedColumns.size() << "," << totalRows << ","
              << ms << "," << throughput << "\n";

      std::cout << "Successfully read " << totalRows << " rows in " << ms
                << " ms (" << throughput << " M rows/s)" << std::endl;

    } catch (const std::exception& e) {
      std::cerr << "Error reading " << num << ": " << e.what() << std::endl;
      csvFile << num << ",error,error,error,error\n";
    }
  }

  csvFile.close();
  std::cout << "Results written to nimble_projection_times.csv" << std::endl;
  return 0;
}