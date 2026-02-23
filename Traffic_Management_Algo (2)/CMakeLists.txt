cmake_minimum_required(VERSION 3.20)
project(traffic-intelligence-platform
        VERSION 1.0.0
        DESCRIPTION "Scalable Adaptive Traffic Signal Control System"
        LANGUAGES CXX
)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
add_compile_options(-Wall -Wextra -Wpedantic -Werror)
include_directories(${PROJECT_SOURCE_DIR}/include)
set(SOURCES
        src/ble/BLEPriorityManager.cpp
        src/engine/PhaseBuilder.cpp
        src/engine/TrafficEngine.cpp
        src/model/ConflictMatrix.cpp
        src/coordination/CorridorCoordinator.cpp
        src/rl/RLAgent.cpp
)
add_library(tip_core STATIC ${SOURCES})
target_include_directories(tip_core PUBLIC ${PROJECT_SOURCE_DIR}/include)
add_executable(tip_main main.cpp)
target_link_libraries(tip_main PRIVATE tip_core)
install(TARGETS tip_main DESTINATION bin)
install(TARGETS tip_core DESTINATION lib)
install(DIRECTORY include/ DESTINATION include)
