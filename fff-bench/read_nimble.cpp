#include <velox/vector/BaseVector.h>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
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

int main() {
  std::string output_dir = "/nimble/";
  // std::string output_dir = "/nimble_uncomp/";
  // Define input Nimble files to read
  std::vector<std::string> inputs = {
      // CFB
      "/mnt/nvme0n1/xinyu/data" + output_dir + "core.nimble",
      "/mnt/nvme0n1/xinyu/data" + output_dir + "bi.nimble",
      "/mnt/nvme0n1/xinyu/data" + output_dir + "classic.nimble",
      "/mnt/nvme0n1/xinyu/data" + output_dir + "geo.nimble",
      "/mnt/nvme0n1/xinyu/data" + output_dir + "log.nimble",
      "/mnt/nvme0n1/xinyu/data" + output_dir + "ml.nimble",
      // TPCH
      "/mnt/nvme0n1/xinyu/tpch" + output_dir + "lineitem_duckdb_double.nimble",
      // Clickbench
      "/mnt/nvme0n1/xinyu/clickbench" + output_dir + "hits_8M.nimble",

  };

  // Initialize memory management
  velox::memory::MemoryManager::testingSetInstance({});
  auto rootPool = velox::memory::memoryManager()->addRootPool("NimbleReader");
  auto leafPool = rootPool->addLeafChild("leaf");

  // Open CSV file for writing results
  // std::ofstream csvFile("nimbleUncomp_read_times.csv");
  std::ofstream csvFile("nimble_read_times.csv");
  csvFile << "filename,total_rows,read_time_ms,throughput_mrows_per_sec\n";

  // Process each Nimble file
  for (const auto& inputFile : inputs) {
    try {
      std::cout << "Reading " << inputFile << std::endl;

      // Create reader
      auto readFile = std::make_shared<velox::LocalReadFile>(inputFile);
      VeloxReader reader(*leafPool, readFile.get());

      // Read batches and measure time
      constexpr int32_t batchSize = 64 * 1024;
      std::vector<velox::VectorPtr> batches;
      velox::VectorPtr batch = nullptr;
      size_t totalRows = 0;

      auto startTime = std::chrono::high_resolution_clock::now();

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
      double ms = toMilliseconds(duration);
      double throughput = calculateThroughput(totalRows, ms);

      // Write results to CSV
      csvFile << std::filesystem::path(inputFile).filename().stem().string()
              << "," << totalRows << "," << ms << "," << throughput << "\n";

      std::cout << "Successfully read " << totalRows << " rows in " << ms
                << " ms (" << throughput << " M rows/s)" << std::endl;

    } catch (const std::exception& e) {
      std::cerr << "Error reading " << inputFile << ": " << e.what()
                << std::endl;
      csvFile << inputFile << ",error,error,error\n";
    }
  }

  csvFile.close();
  std::cout << "Results written to nimble_read_times.csv" << std::endl;
  return 0;
}