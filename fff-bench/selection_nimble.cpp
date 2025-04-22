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

int main(int argc, char* argv[]) {
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <row_id>" << std::endl;
    return 1;
  }

  std::string num = argv[1];
  uint32_t row_id = std::stoi(num);

  // Initialize memory management
  velox::memory::MemoryManager::testingSetInstance({});
  auto rootPool = velox::memory::memoryManager()->addRootPool("NimbleReader");
  auto leafPool = rootPool->addLeafChild("leaf");

  try {
    std::cout << "Reading row " << row_id << " from nimble file" << std::endl;

    auto startTime = std::chrono::high_resolution_clock::now();

    // Create reader
    auto readFile = std::make_shared<velox::LocalReadFile>(
        "/mnt/nvme0n1/xinyu/tpch/nimble_uncomp/lineitem_duckdb_double.nimble");

    // Create a reader that reads all columns
    VeloxReader reader(*leafPool, readFile.get());

    reader.seekToRow(row_id);

    // Now read the target row
    velox::VectorPtr batch = nullptr;
    bool success = reader.next(1, batch);

    auto endTime = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
        endTime - startTime);

    double ms = toMilliseconds(duration);

    if (success && batch) {
      std::cerr << "Random access Nimble file took " << int(ms) << "ms"
                << std::endl;
    } else {
      std::cerr << "Failed to read row " << row_id << std::endl;
      return 1;
    }

  } catch (const std::exception& e) {
    std::cerr << "Error accessing row " << row_id << ": " << e.what()
              << std::endl;
    return 1;
  }

  return 0;
}