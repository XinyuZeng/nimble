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

add_executable(gen_nimble gen_nimble.cpp)
target_link_libraries(gen_nimble PRIVATE 
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

add_executable(mem_nimble mem_nimble.cpp)
target_link_libraries(mem_nimble PRIVATE 
  velox_dwio_common 
  velox_dwio_parquet_reader 
  velox_dwio_orc_reader 
  velox_memory
  velox_vector
  nimble_velox_writer 
  nimble_velox_reader
  nimble_velox_common
  nimble_velox_field_writer
  nimble_velox_layout_planner
)

add_executable(read_nimble read_nimble.cpp)
target_link_libraries(read_nimble PRIVATE 
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

add_executable(read_orc read_orc.cpp)
target_link_libraries(read_orc PRIVATE 
  velox_dwio_common 
  velox_dwio_orc_reader
  velox_memory
  velox_vector
)

add_executable(projection_nimble projection_nimble.cpp)
target_link_libraries(projection_nimble PRIVATE 
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
