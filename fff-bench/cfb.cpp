#include <velox/dwio/parquet/RegisterParquetReader.h>
#include <velox/vector/BaseVector.h>
#include <iostream>
#include "dwio/nimble/velox/VeloxReader.h"
#include "dwio/nimble/velox/VeloxWriter.h"
#include "velox/common/file/File.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/dwio/common/ScanSpec.h"
// #include "velox/dwio/parquet/reader/ParquetReader.h"

using namespace facebook;
using namespace facebook::velox;
using namespace facebook::velox::dwio::common;
using namespace facebook::nimble;

// void assertReadWithReaderAndExpected(
//     std::shared_ptr<const RowType> outputType,
//     dwio::common::RowReader& reader,
//     RowVectorPtr expected,
//     memory::MemoryPool& memoryPool) {
//   uint64_t total = 0;
//   VectorPtr result = BaseVector::create(outputType, 0, &memoryPool);
//   while (total < expected->size()) {
//     auto part = reader.next(1000, result);
//     if (part > 0) {
//       assertEqualVectorPart(expected, result, total);
//       total += result->size();
//     } else {
//       break;
//     }
//   }
//   EXPECT_EQ(total, expected->size());
//   EXPECT_EQ(reader.next(1000, result), 0);
// }

std::shared_ptr<velox::common::ScanSpec> makeScanSpec(
    const RowTypePtr& rowType) {
  auto scanSpec = std::make_shared<velox::common::ScanSpec>("");
  scanSpec->addAllChildFields(*rowType);
  return scanSpec;
}

int main(int argc, char** argv) {
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0]
              << " <input_parquet_file> <output_nimble_file>" << std::endl;
    return 1;
  }

  const std::string inputParquetFile = argv[1];
  const std::string outputNimbleFile = argv[2];

  // Initialize memory management
  velox::parquet::registerParquetReaderFactory();
  velox::memory::MemoryManager::testingSetInstance({});
  auto rootPool =
      velox::memory::memoryManager()->addRootPool("ParquetToNimble");
  auto leafPool = rootPool->addLeafChild("leaf");

  try {
    // Read from Parquet
    auto pqFactory = velox::dwio::common::getReaderFactory(
        velox::dwio::common::FileFormat::PARQUET);
    velox::dwio::common::ReaderOptions readerOpts{leafPool.get()};
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
    // rowReaderOpts.select(
    //     std::make_shared<facebook::velox::dwio::common::ColumnSelector>(
    //         rowType, rowType->names()));
    rowReaderOpts.setScanSpec(makeScanSpec(rowType));
    auto rowReader = pqReader->createRowReader(rowReaderOpts);

    // Read and write batches
    constexpr int32_t batchSize = 64 * 1024;
    velox::VectorPtr batch = BaseVector::create(rowType, 0, leafPool.get());
    while (rowReader->next(batchSize, batch)) {
      if (batch) {
        writer.write(batch);
      }
    }
    writer.close();

    // Now read back from Nimble file
    auto readFile = std::make_shared<velox::LocalReadFile>(outputNimbleFile);
    VeloxReader reader(*leafPool, readFile.get());

    velox::VectorPtr nimbleResult = nullptr;
    while (reader.next(batchSize, nimbleResult)) {
      if (nimbleResult) {
        std::cout << "Successfully read batch from Nimble file with "
                  << nimbleResult->size() << " rows" << std::endl;
      }
    }

    std::cout << "Successfully converted Parquet to Nimble and read it back!"
              << std::endl;
  } catch (const std::exception& e) {
    std::cerr << "Error: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}