#include <velox/dwio/orc/reader/OrcReader.h>
#include <velox/vector/BaseVector.h>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include "velox/common/file/File.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/dwio/common/ScanSpec.h"
#include "velox/vector/BaseVector.h"

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

// Helper function to create a scan spec for reading all fields
std::shared_ptr<velox::common::ScanSpec> makeScanSpec(
    const RowTypePtr& rowType) {
  auto scanSpec = std::make_shared<velox::common::ScanSpec>("");
  scanSpec->addAllChildFields(*rowType);
  return scanSpec;
}

int main() {
  // Define input ORC files to read
  std::vector<std::string> inputs = {
      // Add your ORC files here
      "/mnt/nvme0n1/xinyu/data/orc_cpp/core.orc",
      "/mnt/nvme0n1/xinyu/data/orc_cpp/bi.orc",
      "/mnt/nvme0n1/xinyu/data/orc_cpp/classic.orc",
      "/mnt/nvme0n1/xinyu/data/orc_cpp/geo.orc",
      "/mnt/nvme0n1/xinyu/data/orc_cpp/log.orc",
      "/mnt/nvme0n1/xinyu/data/orc_cpp/ml.orc",
      // TPCH
      "/mnt/nvme0n1/xinyu/tpch/orc_cpp/lineitem_duckdb_double.orc",
      // Clickbench
      "/mnt/nvme0n1/xinyu/clickbench/orc_cpp/hits_8M.orc",
  };

  // Initialize memory management and register ORC reader
  velox::filesystems::registerLocalFileSystem();
  velox::orc::registerOrcReaderFactory();
  velox::memory::MemoryManager::testingSetInstance({});
  auto rootPool = velox::memory::memoryManager()->addRootPool("OrcReader");
  auto leafPool = rootPool->addLeafChild("leaf");

  // Open CSV file for writing results
  std::ofstream csvFile("orc_read_times.csv");
  csvFile << "filename,total_rows,read_time_ms,throughput_mrows_per_sec\n";

  // Process each ORC file
  for (const auto& inputFile : inputs) {
    try {
      std::cout << "Reading " << inputFile << std::endl;

      // Create reader
      auto factory = velox::dwio::common::getReaderFactory(
          velox::dwio::common::FileFormat::ORC);
      facebook::velox::dwio::common::ReaderOptions readerOpts{leafPool.get()};
      readerOpts.setFileFormat(velox::dwio::common::FileFormat::ORC);

      auto reader = factory->createReader(
          std::make_unique<BufferedInput>(
              std::make_shared<LocalReadFile>(inputFile),
              readerOpts.memoryPool()),
          readerOpts);

      // Get the schema
      auto rowType = reader->rowType();

      // Create row reader
      RowReaderOptions rowReaderOpts;
      rowReaderOpts.setScanSpec(makeScanSpec(rowType));
      auto rowReader = reader->createRowReader(rowReaderOpts);

      // Read batches and measure time
      constexpr int32_t batchSize = 64 * 1024;
      std::vector<velox::VectorPtr> batches;
      velox::VectorPtr batch =
          BaseVector::create(rowType, batchSize, leafPool.get());
      // velox::VectorPtr batch = nullptr;
      size_t totalRows = 0;

      auto startTime = std::chrono::high_resolution_clock::now();

      while (rowReader->next(batchSize, batch)) {
        if (batch) {
          totalRows += batch->size();
          batches.push_back(batch);
          batch = BaseVector::create(rowType, batchSize, leafPool.get());
          // std::cout << batch->toString() << std::endl;
          // batch = nullptr;
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
      csvFile << std::filesystem::path(inputFile).filename().stem().string()
              << ",error,error,error\n";
    }
  }

  csvFile.close();
  std::cout << "Results written to orc_read_times.csv" << std::endl;
  return 0;
}