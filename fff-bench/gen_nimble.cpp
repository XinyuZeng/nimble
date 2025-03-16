#include <velox/dwio/parquet/RegisterParquetReader.h>
#include <velox/vector/BaseVector.h>
#include <filesystem>
#include <iostream>
#include "dwio/nimble/velox/VeloxReader.h"
#include "dwio/nimble/velox/VeloxWriter.h"
#include "velox/common/file/File.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/dwio/common/ScanSpec.h"

using namespace facebook;
using namespace facebook::velox;
using namespace facebook::velox::dwio::common;
using namespace facebook::nimble;
namespace fs = std::filesystem;

std::shared_ptr<velox::common::ScanSpec> makeScanSpec(
    const RowTypePtr& rowType) {
  auto scanSpec = std::make_shared<velox::common::ScanSpec>("");
  scanSpec->addAllChildFields(*rowType);
  return scanSpec;
}

void convertParquetToNimble(
    const std::string& inputParquetFile,
    const std::string& outputNimbleFile,
    memory::MemoryPool* rootPool,
    memory::MemoryPool* leafPool) {
  try {
    // Read from Parquet
    auto pqFactory = velox::dwio::common::getReaderFactory(
        velox::dwio::common::FileFormat::PARQUET);
    velox::dwio::common::ReaderOptions readerOpts{leafPool};
    readerOpts.setFileFormat(velox::dwio::common::FileFormat::PARQUET);
    auto pqReader = pqFactory->createReader(
        std::make_unique<BufferedInput>(
            std::make_shared<LocalReadFile>(inputParquetFile),
            readerOpts.memoryPool()),
        readerOpts);

    // Get the schema from Parquet reader
    auto rowType = pqReader->rowType();

    // Create Nimble writer with write file
    auto writeFile = std::make_unique<velox::LocalWriteFile>(outputNimbleFile);
    VeloxWriter writer(*rootPool, rowType, std::move(writeFile), {});

    // Create row reader
    RowReaderOptions rowReaderOpts;
    rowReaderOpts.setScanSpec(makeScanSpec(rowType));
    auto rowReader = pqReader->createRowReader(rowReaderOpts);

    // Read and write batches
    constexpr int32_t batchSize = 64 * 1024;
    velox::VectorPtr batch = BaseVector::create(rowType, 0, leafPool);
    while (rowReader->next(batchSize, batch)) {
      if (batch) {
        writer.write(batch);
      }
    }

    writer.close();
    std::cout << "Successfully converted " << inputParquetFile << " to "
              << outputNimbleFile << std::endl;

  } catch (const std::exception& e) {
    std::cerr << "Error processing " << inputParquetFile << ": " << e.what()
              << std::endl;
  }
}

int main() {
  // Define input and output directories

  // Proj exp data
  // std::vector<std::string> inputs = {"/home/xinyu/fff-devel/data"};
  // comp exp data
  std::vector<std::string> inputs = {
      "/mnt/nvme0n1/xinyu/data/",
      "/mnt/nvme0n1/xinyu/tpch/",
      "/mnt/nvme0n1/xinyu/clickbench/"};

  // Initialize memory management
  velox::parquet::registerParquetReaderFactory();
  velox::memory::MemoryManager::testingSetInstance({});
  auto rootPool =
      velox::memory::memoryManager()->addRootPool("ParquetToNimble");
  auto leafPool = rootPool->addLeafChild("leaf");

  auto output_dir = "nimble/";
  // auto output_dir = "nimble_uncomp/";
  // Create output directory if it doesn't exist
  for (const auto& input : inputs) {
    fs::create_directories(input + output_dir);
  }

  // for (const auto& input : inputs) {
  //   for (const auto& entry : fs::directory_iterator(input)) {
  //     if (entry.path().extension() == ".parquet") {
  //       std::string inputPath = entry.path().string();
  //       std::string outputPath =
  //           input + "/" + entry.path().stem().string() + ".nimble";

  //       std::cout << "Converting " << inputPath << " to " << outputPath
  //                 << std::endl;
  //       convertParquetToNimble(
  //           inputPath, outputPath, rootPool.get(), leafPool.get());
  //     }
  //   }
  // }

  // Process each Parquet file in the input directory
  for (const auto& input : inputs) {
    auto inputPath = input + "parquet/";
    for (const auto& entry : fs::directory_iterator(inputPath)) {
      if (entry.path().extension() == ".parquet") {
        std::string inputPath = entry.path().string();
        std::string outputPath =
            input + output_dir + entry.path().stem().string() + ".nimble";

        std::cout << "Converting " << inputPath << " to " << outputPath
                  << std::endl;
        convertParquetToNimble(
            inputPath, outputPath, rootPool.get(), leafPool.get());
      }
    }
  }

  std::cout << "Finished converting all Parquet files to Nimble format!"
            << std::endl;
  return 0;
}