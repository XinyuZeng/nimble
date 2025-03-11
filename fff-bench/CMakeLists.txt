add_executable(cfb cfb.cpp)
target_link_libraries(cfb PRIVATE 
  velox_dwio_common 
  velox_dwio_parquet_reader 
  velox_memory
  velox_vector
  nimble_velox_writer 
  nimble_velox_reader
  nimble_velox_common
  nimble_velox_field_writer
  nimble_velox_layout_planner
)