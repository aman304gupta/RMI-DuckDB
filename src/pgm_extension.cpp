#define DUCKDB_EXTENSION_MAIN

#include "pgm_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/materialized_query_result.hpp"


#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/types/vector.hpp"

#include "pgm/pgm_index.hpp"
#include "pgm/pgm_index_dynamic.hpp"

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>
#include <climits>

#define DOUBLE_KEY_TYPE double
#define INDEX_PAYLOAD_TYPE double

namespace duckdb {

// alex::Alex<DOUBLE_KEY_TYPE, INDEX_PAYLOAD_TYPE> double_alex_index;

// pgm::PGMIndex<double, epsilon> index(data);
pgm::DynamicPGMIndex<double, double> double_dynamic_index;

int load_end_point = 0;
std::vector<vector<unique_ptr<Base> > > results;

inline void PgmScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "Pgm "+name.GetString()+" 🐥");;
        });
}

inline void PgmOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "Pgm " + name.GetString() +
                                                     ", my linked OpenSSL version is " +
                                                     OPENSSL_VERSION_TEXT );;
        });
}


template <typename K,typename P>
void bulkLoadIntoIndex(duckdb::Connection & con,std::string table_name,int column_index){
    std::cout<<"General Function with no consequence.\n"; 
}

template<>
void bulkLoadIntoIndex<double,INDEX_PAYLOAD_TYPE>(duckdb::Connection & con,std::string table_name,int column_index){
/*
    Phase 1: Load the data from the table.
    */
    string query = "SELECT * FROM "+table_name+";";
    unique_ptr<MaterializedQueryResult> result = con.Query(query);
    results = result->getContents();
    int num_keys = results.size();
    //std::cout<<"Num Keys : "<<num_keys<<"\n";

   /*
    Phase 2: Bulk load the data from the results vector into the pair array that goes into the index.
   */
   std::pair<double,INDEX_PAYLOAD_TYPE>* bulk_load_values = new std::pair<double,INDEX_PAYLOAD_TYPE>[num_keys];  
    int max_key = INT_MIN;
    for (int i=0;i<results.size();i++){
        int row_id = i;
        //std::cout<<"before key"<<"\n";
        auto rrr = results[i][column_index].get();
        
        double key_ = dynamic_cast<DoubleData*>(rrr)->value;
        double value_ = dynamic_cast<DoubleData*>(results[i][column_index+1].get())->value;
        
        //std::cout<<"after key"<<"\n";
        bulk_load_values[i] = {key_,value_};
    }
    /**
     Phase 3: Sort the bulk load values array based on the key values.
    */

    auto start_time = std::chrono::high_resolution_clock::now();
    std::sort(bulk_load_values,bulk_load_values+num_keys,[](auto const& a, auto const& b) { return a.first < b.first; });

    /*
    Phase 4: Bulk load the sorted values into the index.
    */


    
    // double_alex_index.bulk_load(bulk_load_values, num_keys);
    // auto end_time = std::chrono::high_resolution_clock::now();
    // std::chrono::duration<double> elapsed_seconds = end_time - start_time;
    // std::cout << "Time taken to bulk load: " << elapsed_seconds.count() << " seconds\n\n\n";
    // print_stats<DOUBLE_KEY_TYPE>();

    double_dynamic_index.insert(bulk_load_values, num_keys);
}



static QualifiedName GetQualifiedName(ClientContext &context, const string &qname_str) {
	auto qname = QualifiedName::Parse(qname_str);
	if (qname.schema == INVALID_SCHEMA) {
		qname.schema = ClientData::Get(context).catalog_search_path->GetDefaultSchema(qname.catalog);
	}
	return qname;
}

void createPGMIndexPragmaFunction(ClientContext &context, const FunctionParameters &parameters){
    string table_name = parameters.values[0].GetValue<string>();
    string column_name = parameters.values[1].GetValue<string>();

    QualifiedName qname = GetQualifiedName(context, table_name);
    // CheckIfTableExists(context, qname);
    auto &table = Catalog::GetEntry<TableCatalogEntry>(context, qname.catalog, qname.schema, qname.name);
    auto &columnList = table.GetColumns(); 

    vector<string>columnNames = columnList.GetColumnNames();
    vector<LogicalType>columnTypes = columnList.GetColumnTypes();
    int count = 0;
    int column_index = -1;
    LogicalType column_type;
    int col_i = 0;
    for(col_i=0;col_i<columnNames.size();col_i++){
        string curr_col_name = columnNames[col_i];
        LogicalType curr_col_type = columnTypes[col_i];
        if(curr_col_name == column_name){
            column_index = count;
            column_type = curr_col_type;
        }
        count++;
    }

    if(column_index == -1){
        std::cout<<"Column not found "<<"\n";
    }
    else{
        duckdb::Connection con(*context.db);
        // std::cout<<"Column found at index "<<column_index<<"\n";
        // std::cout<<"Creating an alex index for this column"<<"\n";
        // std::cout<<"Column Type "<<typeid(column_type).name()<<"\n";
        // std::cout<<"Column Type "<<typeid(double).name()<<"\n";
        // std::cout<<"Column type to string "<<column_type.ToString()<<"\n";
        std::string columnTypeName = column_type.ToString();
        if(columnTypeName == "DOUBLE"){
            bulkLoadIntoIndex<DOUBLE_KEY_TYPE,INDEX_PAYLOAD_TYPE>(con,table_name,column_index);
            index_type_table_name_map.insert({"double",{table_name,column_name}});
        }
        // else if(columnTypeName == "BIGINT"){
        //     bulkLoadIntoIndex<INT64_KEY_TYPE,INDEX_PAYLOAD_TYPE>(con,table_name,column_index);
        //     index_type_table_name_map.insert({"bigint",{table_name,column_name}});
        // }
        // else if(columnTypeName == "UBIGINT"){
        //     bulkLoadIntoIndex<UNSIGNED_INT64_KEY_TYPE,INDEX_PAYLOAD_TYPE>(con,table_name,column_index);
        //     index_type_table_name_map.insert({"ubigint",{table_name,column_name}});
        // }
        // else if(columnTypeName == "INTEGER"){
        //     bulkLoadIntoIndex<INT_KEY_TYPE,INDEX_PAYLOAD_TYPE>(con,table_name,column_index);
        //     index_type_table_name_map.insert({"int",{table_name,column_name}});
        // }
        else{
            std::cout<<"Unsupported column type for alex indexing (for now) "<<"\n";
        }
        //bulkLoadIntoIndex<typeid(column_type).name(),INDEX_PAYLOAD_TYPE>(con,table_name,column_index);
    }
}

static void LoadInternal(DatabaseInstance &instance) {
    // Register a scalar function
    auto pgm_scalar_function = ScalarFunction("pgm", {LogicalType::VARCHAR}, LogicalType::VARCHAR, PgmScalarFun);
    ExtensionUtil::RegisterFunction(instance, pgm_scalar_function);

    // Register another scalar function
    auto pgm_openssl_version_scalar_function = ScalarFunction("pgm_openssl_version", {LogicalType::VARCHAR},
                                                LogicalType::VARCHAR, PgmOpenSSLVersionScalarFun);


    auto create_alex_index_function = PragmaFunction::PragmaCall("create_pgm_index", createPGMIndexPragmaFunction, {LogicalType::VARCHAR, LogicalType::VARCHAR},{});
    ExtensionUtil::RegisterFunction(instance, create_alex_index_function);


    ExtensionUtil::RegisterFunction(instance, pgm_openssl_version_scalar_function);
}

void PgmExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string PgmExtension::Name() {
	return "pgm";
}

std::string PgmExtension::Version() const {
#ifdef EXT_VERSION_PGM
	return EXT_VERSION_PGM;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void pgm_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::PgmExtension>();
}

DUCKDB_EXTENSION_API const char *pgm_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
