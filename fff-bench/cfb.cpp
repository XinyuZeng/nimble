#include <velox/dwio/parquet/RegisterParquetReader.h>
#include <velox/vector/BaseVector.h>
#include <iostream>
#include "dwio/nimble/velox/VeloxWriter.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"

using namespace facebook;

int main() {
  velox::parquet::registerParquetReaderFactory();
  auto pq_factory = velox::dwio::common::getReaderFactory(
      velox::dwio::common::FileFormat::PARQUET);
  auto rootPool_ = velox::memory::memoryManager()->addRootPool("E2EWriterTest");
  auto leafPool_ = rootPool_->addLeafChild("leaf");
  auto pq_reader = pq_factory->createReader(
      nullptr, velox::dwio::common::ReaderOptions(leafPool_.get()));
  auto row_reader = pq_reader->createRowReader();
  velox::VectorPtr result = nullptr;
  row_reader->next(1000000, result);
  std::cout << "Hello, World!" << std::endl;
  return 0;
}