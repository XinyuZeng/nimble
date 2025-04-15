/// Profiling the time spent on loading schema and creating column selector

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

int main(int argc, char** argv) {
  // Define input Nimble files to read
  std::string num = argv[1];

  // Initialize memory management
  velox::memory::MemoryManager::testingSetInstance({});
  auto rootPool = velox::memory::memoryManager()->addRootPool("NimbleReader");
  auto leafPool = rootPool->addLeafChild("leaf");

  // Open CSV file for writing results
  // std::ofstream csvFile("nimble_projection_times.csv");
  // csvFile
  //     <<
  //     "filename,num_columns,total_rows,read_time_ms,throughput_mrows_per_sec\n";
  std::unordered_map<int, std::shared_ptr<dwio::common::ColumnSelector>>
      selectors;
  std::unordered_map<int, double> loadSchemaTimes;
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
      "/home/xinyu/fff-devel/data/copy/" + num + ".nimble");
  auto startTime0 = std::chrono::high_resolution_clock::now();
  // A lot of time will be spent here if we do not pass in projection
  VeloxReader schemaReader(*leafPool, readFile.get());
  auto endTime0 = std::chrono::high_resolution_clock::now();
  std::cout << "Schema reader time: " << toMilliseconds(endTime0 - startTime0)
            << " ms" << std::endl;
  loadSchemaTimes[std::stoi(num)] = schemaReader.loadSchemaTime();
  startTime0 = std::chrono::high_resolution_clock::now();
  auto selector = std::make_shared<dwio::common::ColumnSelector>(
      schemaReader.type(), selectedColumns);
  loadSchemaTimes[std::stoi(num)] += toMilliseconds(endTime0 - startTime0);
  selectors[std::stoi(num)] = selector;
  endTime0 = std::chrono::high_resolution_clock::now();
  std::cout << "Selector time: " << toMilliseconds(endTime0 - startTime0)
            << " ms" << std::endl;

  // Process each Nimble file
  try {
    // std::cout << "Reading " << num << std::endl;
    auto num_int = std::stoi(num);
    auto startTime = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < 1000; i++) {
      // Create new reader with selected columns
      auto readFile = std::make_shared<velox::LocalReadFile>(
          "/home/xinyu/fff-devel/data_8rows/" + num + ".nimble");
      VeloxReader schemaReader(*leafPool, readFile.get());
      auto type = schemaReader.type();
      VeloxReader reader(*leafPool, readFile.get(), selectors[num_int]);

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
    }
    auto endTime = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
        endTime - startTime);

    // double ms = toMilliseconds(duration) + loadSchemaTimes[num_int];
    // double throughput = calculateThroughput(totalRows, ms);

    // Write results to CSV
    // csvFile << num << "," << num_int << "," << totalRows << "," << ms <<
    // ","
    //         << throughput << "\n";

    // std::cout << "Successfully read " << totalRows << " rows in " << ms
    //           << " ms (" << throughput << " M rows/s)" << std::endl;

  } catch (const std::exception& e) {
    std::cerr << "Error reading " << num << ": " << e.what() << std::endl;
    // csvFile << num << ",error,error,error,error\n";
  }

  // csvFile.close();
  // std::cout << "Results written to nimble_projection_times.csv" << std::endl;
  return 0;
}