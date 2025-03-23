/// For testing the memory usage of Nimble during writing.
#include <velox/dwio/orc/reader/OrcReader.h>
#include <velox/dwio/parquet/RegisterParquetReader.h>
#include <velox/vector/BaseVector.h>
#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include "dwio/nimble/velox/VeloxReader.h"
#include "dwio/nimble/velox/VeloxWriter.h"
#include "velox/common/file/File.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/dwio/common/ScanSpec.h"

using namespace facebook;
using namespace facebook::velox;
using namespace facebook::velox::dwio::common;
using namespace facebook::nimble;

std::shared_ptr<velox::common::ScanSpec> makeScanSpec(
    const RowTypePtr& rowType) {
  auto scanSpec = std::make_shared<velox::common::ScanSpec>("");
  scanSpec->addAllChildFields(*rowType);
  return scanSpec;
}

void convertParquetToNimbleWithMem(
    const std::string& inputParquetFile,
    const std::string& outputNimbleFile,
    memory::MemoryPool* rootPool,
    memory::MemoryPool* leafPool) {
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
  auto writeFile =
      std::make_unique<velox::LocalWriteFile>(outputNimbleFile, true, false);
  VeloxWriter writer(*rootPool, rowType, std::move(writeFile), {});

  // Create row reader
  RowReaderOptions rowReaderOpts;
  rowReaderOpts.setScanSpec(makeScanSpec(rowType));
  auto rowReader = pqReader->createRowReader(rowReaderOpts);

  // Read and write batches
  constexpr int32_t batchSize = 64 * 1024;
  velox::VectorPtr batch = BaseVector::create(rowType, 0, leafPool);
  uint64_t max_mem = 0;
  uint64_t mem_sum = 0;
  uint64_t cnt = 0;
  while (rowReader->next(batchSize, batch)) {
    if (batch) {
      writer.write(batch);
    }
    max_mem = std::max(max_mem, writer.mem_used());
    if (writer.mem_used() != 0) {
      mem_sum += writer.mem_used();
      cnt++;
    }
  }
  std::cout << "Max mem used: " << max_mem << std::endl;
  std::cout << "Avg mem used: " << mem_sum / cnt << std::endl;

  writer.close();
  std::cout << "Successfully converted " << inputParquetFile << " to "
            << outputNimbleFile << std::endl;
}

void convertOrcToNimbleWithMem(
    const std::string& inputParquetFile,
    const std::string& outputNimbleFile,
    memory::MemoryPool* rootPool,
    memory::MemoryPool* leafPool) {
  // Read from Parquet
  auto pqFactory = velox::dwio::common::getReaderFactory(
      velox::dwio::common::FileFormat::ORC);
  velox::dwio::common::ReaderOptions readerOpts{leafPool};
  readerOpts.setFileFormat(velox::dwio::common::FileFormat::ORC);
  auto pqReader = pqFactory->createReader(
      std::make_unique<BufferedInput>(
          std::make_shared<LocalReadFile>(inputParquetFile),
          readerOpts.memoryPool()),
      readerOpts);

  // Get the schema from Parquet reader
  auto rowType = pqReader->rowType();

  // Create Nimble writer with write file
  auto writeFile =
      std::make_unique<velox::LocalWriteFile>(outputNimbleFile, true, false);
  VeloxWriter writer(*rootPool, rowType, std::move(writeFile), {});

  // Create row reader
  RowReaderOptions rowReaderOpts;
  rowReaderOpts.setScanSpec(makeScanSpec(rowType));
  auto rowReader = pqReader->createRowReader(rowReaderOpts);

  // Read and write batches
  constexpr int32_t batchSize = 64 * 1024;
  velox::VectorPtr batch = BaseVector::create(rowType, 0, leafPool);
  uint64_t max_mem = 0;
  uint64_t mem_sum = 0;
  uint64_t cnt = 0;
  while (rowReader->next(batchSize, batch)) {
    if (batch) {
      writer.write(batch);
    }
    max_mem = std::max(max_mem, writer.mem_used());
    if (writer.mem_used() != 0) {
      mem_sum += writer.mem_used();
      cnt++;
    }
  }
  std::cout << "Max mem used: " << max_mem << std::endl;
  std::cout << "Avg mem used: " << mem_sum / cnt << std::endl;

  writer.close();
  std::cout << "Successfully converted " << inputParquetFile << " to "
            << outputNimbleFile << std::endl;
}

int main() {
  std::string input = "/mnt/nvme0n1/xinyu/laion/orc/merged_8M.orc";
  std::string output = "/mnt/nvme0n1/xinyu/laion/nimble/merged_8M.nimble";

  // std::string input = "/mnt/nvme0n1/xinyu/data/parquet/core.parquet";
  // std::string output = "/mnt/nvme0n1/xinyu/data/nimble/core.nimble";
  // Initialize memory management
  velox::dwio::common::LocalFileSink::registerFactory();
  velox::filesystems::registerLocalFileSystem();
  velox::parquet::registerParquetReaderFactory();
  velox::orc::registerOrcReaderFactory();
  velox::memory::MemoryManager::initialize({});
  // velox::memory::MemoryManager::testingSetInstance({});
  auto rootPool =
      velox::memory::memoryManager()->addRootPool("ParquetToNimble");
  auto leafPool = rootPool->addLeafChild("leaf");

  // convertParquetToNimbleWithMem(input, output, rootPool.get(),
  // leafPool.get());
  convertOrcToNimbleWithMem(input, output, rootPool.get(), leafPool.get());

  std::cout << "Finished converting all Parquet files to Nimble format!"
            << std::endl;
  return 0;
}