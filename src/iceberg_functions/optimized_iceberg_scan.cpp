#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/common/enums/joinref_type.hpp"
#include "duckdb/common/enums/tableref_type.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "iceberg_metadata.hpp"
#include "iceberg_utils.hpp"
#include "iceberg_functions.hpp"
#include "yyjson.hpp"

#include <iostream>
#include <vector>

namespace duckdb {
    struct IcebergBindData : public FunctionData {
        string iceberg_path;
        vector<IcebergColumnDefinition> schema;

        unique_ptr<FunctionData> Copy() const override {
            auto copy = make_uniq<IcebergBindData>();
            copy->iceberg_path = iceberg_path;
            copy->schema = schema;
            return copy;
        }

        bool Equals(const FunctionData &other_p) const override {
            return true;
        }
    };

    struct OptimizedIcebergScanGlobalTableFunctionState : public GlobalTableFunctionState {
    public:
        static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
            return make_uniq<GlobalTableFunctionState>();
        }
    };

    static void OptimizedIcebergScanBindFunction(ClientContext &context,
                                                 TableFunctionInput &input,
                                                 DataChunk &output) {
        output.SetCardinality(0); // mock: no rows
    }

    static unique_ptr<FunctionData> OptimizedIcebergScanBind(ClientContext &context, TableFunctionBindInput &input,
                                                             vector<LogicalType> &return_types,
                                                             vector<string> &names) {
        std::cout << "???" << std::endl;
        // FileSystem &fs = FileSystem::GetFileSystem(context);
        //
        // string iceberg_path = input.inputs[0].ToString();
        // bool allow_moved_paths = false;
        // bool skip_schema_inference = false;
        // string metadata_compression_codec = "none";
        // string table_version = DEFAULT_TABLE_VERSION;
        // string version_name_format = DEFAULT_TABLE_VERSION_FORMAT;
        //
        // for (auto &kv: input.named_parameters) {
        //     auto loption = StringUtil::Lower(kv.first);
        //     if (loption == "allow_moved_paths") {
        //         allow_moved_paths = BooleanValue::Get(kv.second);
        //     } else if (loption == "skip_schema_inference") {
        //         skip_schema_inference = BooleanValue::Get(kv.second);
        //     } else if (loption == "metadata_compression_codec") {
        //         metadata_compression_codec = StringValue::Get(kv.second);
        //     } else if (loption == "version") {
        //         table_version = StringValue::Get(kv.second);
        //     } else if (loption == "version_name_format") {
        //         version_name_format = StringValue::Get(kv.second);
        //     }
        // }
        // //
        // // auto iceberg_meta_path = IcebergSnapshot::GetMetaDataPath(context, iceberg_path, fs, metadata_compression_codec, table_version, version_name_format);
        // // IcebergSnapshot snapshot_to_scan = IcebergSnapshot::GetLatestSnapshot(iceberg_meta_path, fs, metadata_compression_codec, skip_schema_inference);
        //
        // // IcebergSnapshot snapshot_to_scan = IcebergSnapshot::
        //
        // // Return schema to DuckDB planner
        // // for (auto &col : snapshot_to_scan.schema) {
        // return_types.push_back(LogicalType::DECIMAL);
        // names.push_back("dummy");
        // // }
        return_types.push_back(LogicalType::VARCHAR);
        names.push_back("mock_column");

        auto bind_data = make_uniq<IcebergBindData>();
        bind_data->iceberg_path = input.inputs[0].ToString();

        // Dummy column metadata (if needed later)
        IcebergColumnDefinition mock_col;
        mock_col.name = "mock_column";
        mock_col.type = LogicalType::VARCHAR;
        mock_col.id = 0;
        mock_col.default_value = Value();

        bind_data->schema.push_back(mock_col);
        return bind_data;
    }

    TableFunctionSet IcebergFunctions::GetOptimizedIcebergScanFunction() {
        TableFunctionSet function_set("optimized_iceberg_scan");

        auto fun = TableFunction({LogicalType::VARCHAR}, nullptr, nullptr,
                                 OptimizedIcebergScanGlobalTableFunctionState::Init);
        fun.bind = OptimizedIcebergScanBind;
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
