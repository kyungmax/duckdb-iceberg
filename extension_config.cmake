# This file is included by DuckDB's build system. It specifies which extension to load

duckdb_extension_load(avro
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb_avro
        GIT_TAG 489fa49ce3b6f6b2d5f67bf75f6b4622197fc94a
)

# Extension from this repo
duckdb_extension_load(iceberg
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

duckdb_extension_load(tpch)


################## AWS
if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(aws
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG main
    )
endif ()

duckdb_extension_load(httpfs
        GIT_URL https://github.com/duckdb/duckdb-httpfs
        GIT_TAG main
        INCLUDE_DIR extension/httpfs/include
)
