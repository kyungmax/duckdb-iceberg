#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/common/file_system.hpp"
#include "iceberg_metadata.hpp"
#include "iceberg_utils.hpp"
#include "iceberg_functions.hpp"
#include "yyjson.hpp"

#include <iostream>
#include <vector>

namespace duckdb {
    struct OptimizedIcebergScanBindData : public TableFunctionData {
        string iceberg_path;
        vector<IcebergColumnDefinition> schema;
    };

    struct OptimizedIcebergScanGlobalTableFunctionState : public GlobalTableFunctionState {
    public:
        static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
            std::cout << "OptimizedIcebergScanGlobalTableFunctionState::Init called" << std::endl;
            auto global_state = make_uniq<OptimizedIcebergScanGlobalTableFunctionState>();
            auto bind_data = input.bind_data->Cast<OptimizedIcebergScanBindData>();
            global_state->column_ids = input.column_ids;
            global_state->table_filter_set = input.filters;
            return std::move(global_state);
        }

    public:
        std::vector<column_t> column_ids;
        optional_ptr<TableFilterSet> table_filter_set;
    };

    static void OptimizedIcebergScanBindFunction(ClientContext &context,
                                                 TableFunctionInput &input,
                                                 DataChunk &output) {
        std::cout << "OptimizedIcebergScanBindFunction called" << std::endl;
        auto &global_state = input.global_state->Cast<OptimizedIcebergScanGlobalTableFunctionState>();
        auto &column_ids = global_state.column_ids;
        for (auto column_id: column_ids) {
            std::cout << "Column ID: " << column_id << std::endl;
        }
        output.SetCardinality(0); // mock: no rows
    }

    static unique_ptr<FunctionData> OptimizedIcebergScanBind(ClientContext &context, TableFunctionBindInput &input,
                                                             vector<LogicalType> &return_types,
                                                             vector<string> &names) {
        std::cout << "OptimizedIcebergScanBind called" << std::endl;
        FileSystem &fs = FileSystem::GetFileSystem(context);
        //
        string iceberg_path = input.inputs[0].ToString();
        string mode = "default";
        bool allow_moved_paths = false;
        bool skip_schema_inference = false;
        string metadata_compression_codec = "none";
        string table_version = DEFAULT_TABLE_VERSION;
        string version_name_format = DEFAULT_TABLE_VERSION_FORMAT;
        //
        for (auto &kv: input.named_parameters) {
            auto loption = StringUtil::Lower(kv.first);
            if (loption == "allow_moved_paths") {
                allow_moved_paths = BooleanValue::Get(kv.second);
            } else if (loption == "skip_schema_inference") {
                skip_schema_inference = BooleanValue::Get(kv.second);
            } else if (loption == "metadata_compression_codec") {
                metadata_compression_codec = StringValue::Get(kv.second);
            } else if (loption == "version") {
                table_version = StringValue::Get(kv.second);
            } else if (loption == "version_name_format") {
                version_name_format = StringValue::Get(kv.second);
            }
        }

        auto iceberg_meta_path = IcebergSnapshot::GetMetaDataPath(context, iceberg_path, fs, metadata_compression_codec,
                                                                  table_version, version_name_format);
        IcebergSnapshot snapshot_to_scan;
        if (input.inputs.size() > 1) {
            if (input.inputs[1].type() == LogicalType::UBIGINT) {
                snapshot_to_scan = IcebergSnapshot::GetSnapshotById(iceberg_meta_path, fs,
                                                                    input.inputs[1].GetValue<uint64_t>(),
                                                                    metadata_compression_codec, skip_schema_inference);
            } else if (input.inputs[1].type() == LogicalType::TIMESTAMP) {
                snapshot_to_scan =
                        IcebergSnapshot::GetSnapshotByTimestamp(iceberg_meta_path, fs,
                                                                input.inputs[1].GetValue<timestamp_t>(),
                                                                metadata_compression_codec, skip_schema_inference);
            } else {
                throw InvalidInputException("Unknown argument type in OptimizedIcebergScanBind.");
            }
        } else {
            snapshot_to_scan = IcebergSnapshot::GetLatestSnapshot(iceberg_meta_path, fs, metadata_compression_codec,
                                                                  skip_schema_inference);
        }

        auto &schema = snapshot_to_scan.schema;
        if (schema.empty()) {
            throw InvalidInputException("No schema found in iceberg metadata");
        }
        for (auto &col: schema) {
            return_types.push_back(col.ToDuckDBType());
            names.push_back(col.name);
        }

        auto bind_data = make_uniq<OptimizedIcebergScanBindData>();
        bind_data->iceberg_path = input.inputs[0].ToString();
        bind_data->schema = schema;
        return bind_data;
    }

    TableFunctionSet IcebergFunctions::GetOptimizedIcebergScanFunction() {
        TableFunctionSet function_set("optimized_iceberg_scan");

        auto fun = TableFunction({LogicalType::VARCHAR}, OptimizedIcebergScanBindFunction, OptimizedIcebergScanBind,
                                 OptimizedIcebergScanGlobalTableFunctionState::Init);
        fun.function = OptimizedIcebergScanBindFunction;
        fun.projection_pushdown = true;
        fun.filter_pushdown = true;
        fun.named_parameters["skip_schema_inference"] = LogicalType::BOOLEAN;
        fun.named_parameters["allow_moved_paths"] = LogicalType::BOOLEAN;
        fun.named_parameters["mode"] = LogicalType::VARCHAR;
        fun.named_parameters["metadata_compression_codec"] = LogicalType::VARCHAR;
        fun.named_parameters["version"] = LogicalType::VARCHAR;
        fun.named_parameters["version_name_format"] = LogicalType::VARCHAR;
        function_set.AddFunction(fun);
        return function_set;
    }
} // namespace duckdb
