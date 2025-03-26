/// Generate Nimble files for wide table projection

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
  std::vector<std::string> inputs = {"/home/xinyu/fff-devel/data_8rows"};

  // Initialize memory management
  velox::parquet::registerParquetReaderFactory();
  velox::memory::MemoryManager::testingSetInstance({});
  auto rootPool =
      velox::memory::memoryManager()->addRootPool("ParquetToNimble");
  auto leafPool = rootPool->addLeafChild("leaf");

  // Proj exp data
  for (const auto& input : inputs) {
    for (const auto& entry : fs::directory_iterator(input)) {
      if (entry.path().extension() == ".parquet") {
        std::string inputPath = entry.path().string();
        std::string outputPath =
            input + "/" + entry.path().stem().string() + ".nimble";

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