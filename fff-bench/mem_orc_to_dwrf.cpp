/// For testing the memory usage of Velox during Parquet to ORC conversion.
#include <velox/dwio/dwrf/RegisterDwrfWriter.h>
#include <velox/dwio/orc/reader/OrcReader.h>
#include <velox/dwio/dwrf/writer/Writer.h>
#include <velox/vector/BaseVector.h>
#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <iostream>
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

std::shared_ptr<velox::common::ScanSpec> makeScanSpec(
    const RowTypePtr& rowType) {
  auto scanSpec = std::make_shared<velox::common::ScanSpec>("");
  scanSpec->addAllChildFields(*rowType);
  return scanSpec;
}

void convertOrctToDwrfWithMem(
    const std::string& inputParquetFile,
    const std::string& outputOrcFile,
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

  // Create ORC writer
  velox::dwio::common::LocalFileSink::registerFactory();
  auto sink = FileSink::create(outputOrcFile, {.pool = rootPool});
  auto writerOptions = facebook::velox::dwio::common::getWriterFactory(
                           facebook::velox::dwio::common::FileFormat::DWRF)
                           ->createWriterOptions();
  writerOptions->memoryPool = rootPool; // Use root pool for writer itself
  writerOptions->schema = rowType;
  auto writer = facebook::velox::dwio::common::getWriterFactory(
                    facebook::velox::dwio::common::FileFormat::DWRF)
                    ->createWriter(
                        std::move(sink),
                        std::shared_ptr<dwio::common::WriterOptions>(
                            std::move(writerOptions)));

  // Create row reader
  RowReaderOptions rowReaderOpts;
  rowReaderOpts.setScanSpec(makeScanSpec(rowType));
  auto rowReader = pqReader->createRowReader(rowReaderOpts);

  // Read and write batches
  constexpr int32_t batchSize = 64 * 1024;
  velox::VectorPtr batch =
      BaseVector::create(rowType, 0, leafPool); // Use leaf pool for batches
  uint64_t max_mem = 0;
  uint64_t mem_sum = 0;
  uint64_t cnt = 0;
  while (rowReader->next(batchSize, batch)) {
    if (batch && batch->size() > 0) { // Check if batch has data
      writer->write(batch);
    }
    // Track memory usage from the leaf pool used for batches/reading
    // uint64_t current_mem = leafPool->usedBytes();
    // max_mem = std::max(max_mem, current_mem);
    // if (current_mem != 0) {
    //   mem_sum += current_mem;
    //   cnt++;
    // }
  }

  writer->close();

  //   std::cout << "Max mem used (Leaf Pool): " << max_mem << std::endl;
  //   if (cnt > 0) {
  //     std::cout << "Avg mem used (Leaf Pool): " << mem_sum / cnt <<
  //     std::endl;
  //   } else {
  //     std::cout << "Avg mem used (Leaf Pool): 0 (No batches processed)"
  //               << std::endl;
  //   }

  std::cout << "Successfully converted " << inputParquetFile << " to "
            << outputOrcFile << std::endl;
}

int main(int argc, char** argv) {
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0]
              << " <input_parquet_file> <output_orc_file>" << std::endl;
    return 1;
  }
  std::string input = argv[1];
  std::string output = argv[2];

  // Example usage paths (replace with argv):
  // std::string input = "/mnt/nvme0n1/xinyu/laion/parquet/merged_8M.parquet";
  // // Example Input std::string output =
  // "/mnt/nvme0n1/xinyu/laion/orc_output/merged_8M.orc"; // Example Output

  // Initialize memory management and factories
  velox::dwio::common::LocalFileSink::registerFactory();
  velox::filesystems::registerLocalFileSystem();
  velox::orc::registerOrcReaderFactory();
  velox::dwrf::registerDwrfWriterFactory(); // Register ORC writer factory
  velox::memory::MemoryManager::initialize({});
  auto rootPool = velox::memory::memoryManager()->addRootPool("ParquetToOrc");
  auto leafPool = rootPool->addLeafChild("leaf");

  convertOrctToDwrfWithMem(input, output, rootPool.get(), leafPool.get());

  std::cout << "Finished converting Parquet file to ORC format!" << std::endl;
  return 0;
}