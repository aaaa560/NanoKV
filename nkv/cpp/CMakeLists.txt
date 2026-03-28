cmake_minimum_required(VERSION 3.14)
project(parser)

# ========== OTIMIZAÇÃO (ADICIONA ISSO) ==========
set(CMAKE_BUILD_TYPE Release)  # ← Release mode
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -march=native -DNDEBUG")  # ← Máxima otimização

# ========== PyBind11 ==========
find_package(pybind11 REQUIRED)
find_package(Python COMPONENTS Interpreter Development REQUIRED)

pybind11_add_module(nkv_parser
        MODULE
        parser.cpp
)

# ========== Output ==========
set_target_properties(
        nkv_parser
        PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_SOURCE_DIR}"
)

target_link_libraries(nkv_parser PRIVATE pybind11::embed)

set(PYBIND11_FINDPYTHON ON)
