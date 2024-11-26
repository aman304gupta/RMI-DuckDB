cmake_minimum_required(VERSION 3.12)

# Set extension name
set(TARGET_NAME pgm)

# DuckDB's extension distribution supports vcpkg. Dependencies can be added in ./vcpkg.json
find_package(OpenSSL REQUIRED)

# Project settings
project(${TARGET_NAME})
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Include directories
include_directories(src/include)         # For DuckDB-specific headers
include_directories(src/PGM/include)     # For PGM-index headers

# Source files for the extension
set(EXTENSION_SOURCES src/pgm_extension.cpp)

# Create static extension
build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})

# Create loadable extension
build_loadable_extension(${TARGET_NAME} "" ${EXTENSION_SOURCES})

# Explicitly link OpenSSL libraries after targets are created
if(TARGET ${TARGET_NAME}_extension)
    target_link_libraries(${TARGET_NAME}_extension OpenSSL::SSL OpenSSL::Crypto)
endif()

if(TARGET ${TARGET_NAME}_loadable_extension)
    target_link_libraries(${TARGET_NAME}_loadable_extension OpenSSL::SSL OpenSSL::Crypto)
endif()

# Install the extension
install(
  TARGETS ${TARGET_NAME}_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"
)
