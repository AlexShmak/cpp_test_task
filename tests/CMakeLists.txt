find_package(GTest REQUIRED)
include(GoogleTest)

add_executable(kv_storage_tests tests.cpp)
target_link_libraries(kv_storage_tests PRIVATE kv_storage GTest::gtest_main)

gtest_discover_tests(kv_storage_tests)
