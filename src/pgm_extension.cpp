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
#include "utils.h"
#include<iomanip>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>
#include <climits>

#define DOUBLE_KEY_TYPE double
#define INDEX_PAYLOAD_TYPE double
#define GENERAL_PAYLOAD_TYPE double

#define INT64_KEY_TYPE int64_t
#define INT_KEY_TYPE int
#define UNSIGNED_INT64_KEY_TYPE uint64_t
#define PAYLOAD_TYPE double

#define HUNDRED_MILLION 100000000
#define TEN_MILLION 10000000

namespace duckdb {

// alex::Alex<DOUBLE_KEY_TYPE, INDEX_PAYLOAD_TYPE> double_alex_index;
// alex::Alex<INT64_KEY_TYPE, INDEX_PAYLOAD_TYPE> big_int_alex_index;
// alex::Alex<UNSIGNED_INT64_KEY_TYPE, INDEX_PAYLOAD_TYPE> unsigned_big_int_alex_index;
// alex::Alex<INT_KEY_TYPE, INDEX_PAYLOAD_TYPE> index;

// pgm::PGMIndex<double, epsilon> index(data);
pgm::DynamicPGMIndex<double, double> double_dynamic_index;
pgm::DynamicPGMIndex<INT64_KEY_TYPE, INDEX_PAYLOAD_TYPE> big_int_dynamic_index;
pgm::DynamicPGMIndex<UNSIGNED_INT64_KEY_TYPE, INDEX_PAYLOAD_TYPE> unsigned_big_int_dynamic_index;
pgm::DynamicPGMIndex<INT_KEY_TYPE, INDEX_PAYLOAD_TYPE> int_dynamic_index;

int load_end_point = 0;
std::vector<vector<unique_ptr<Base> > > results;
map<string,pair<string,string>> index_type_table_name_map;

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

template <typename K>
void print_stats(){
    if(typeid(K)==typeid(DOUBLE_KEY_TYPE)){
        std::cout << "Total size in bytes: " << double_dynamic_index.size_in_bytes() << " bytes\n";
        std::cout << "Index size in bytes: " << double_dynamic_index.index_size_in_bytes() << " bytes\n";
        std::cout << "Number of elements: " << double_dynamic_index.size() << "\n";

    }
    else if(typeid(K)==typeid(UNSIGNED_INT64_KEY_TYPE)){
        std::cout << "Total size in bytes: " << unsigned_big_int_dynamic_index.size_in_bytes() << " bytes\n";
        std::cout << "Index size in bytes: " << unsigned_big_int_dynamic_index.index_size_in_bytes() << " bytes\n";
        std::cout << "Number of elements: " << unsigned_big_int_dynamic_index.size() << "\n";
    }
    else if(typeid(K)==typeid(INT_KEY_TYPE)){
        std::cout << "Total size in bytes: " << int_dynamic_index.size_in_bytes() << " bytes\n";
        std::cout << "Index size in bytes: " << int_dynamic_index.index_size_in_bytes() << " bytes\n";
        std::cout << "Number of elements: " << int_dynamic_index.size() << "\n";
    }
    else{ //big int
        std::cout << "Total size in bytes: " << big_int_dynamic_index.size_in_bytes() << " bytes\n";
        std::cout << "Big Int Dynamic Index size in bytes: " << big_int_dynamic_index.index_size_in_bytes() << " bytes\n";
        std::cout << "Number of elements: " << big_int_dynamic_index.size() << "\n";
    }

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
   std::vector<std::pair<double,INDEX_PAYLOAD_TYPE>> bulk_load_values;
   bulk_load_values.reserve(num_keys);

 
    int max_key = INT_MIN;
    for (int i=0;i<results.size();i++){
        int row_id = i;
        //std::cout<<"before key"<<"\n";
        auto *data = dynamic_cast<Base *>(results[i][column_index].get());
        auto *data1 = dynamic_cast<Base *>(results[i][column_index + 1].get());
        
        double key_ = static_cast<double_t>(static_cast<DoubleData *>(data)->value);
        double value_ = static_cast<double_t>(static_cast<DoubleData *>(data1)->value);
        
        //std::cout<<"after key"<<"\n";
        bulk_load_values.emplace_back(key_,value_);
    }
    /**
     Phase 3: Sort the bulk load values array based on the key values.
    */

    auto start_time = std::chrono::high_resolution_clock::now();
    std::sort(bulk_load_values.begin(),bulk_load_values.end(),[](auto const& a, auto const& b) { return a.first < b.first; });

    /*
    Phase 4: Bulk load the sorted values into the index.
    */


     for (const auto &pair : bulk_load_values) {
        double_dynamic_index.insert_or_assign(pair.first, pair.second);
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_seconds = end_time - start_time;
    std::cout << "Time taken to bulk load: " << elapsed_seconds.count() << " seconds\n\n\n";
    print_stats<DOUBLE_KEY_TYPE>();
}

template<>
void bulkLoadIntoIndex<int64_t,INDEX_PAYLOAD_TYPE>(duckdb::Connection & con,std::string table_name,int column_index){
/*
    Phase 1: Load the data from the table.
    */
    string query = "SELECT * FROM "+table_name+";";
    unique_ptr<MaterializedQueryResult> result = con.Query(query);
    results = result->getContents();
    int num_keys = results.size();
    std::cout<<"Num Keys : "<<num_keys<<"\n";

   /*
    Phase 2: Bulk load the data from the results vector into the pair array that goes into the index.
   */
//    std::pair<int64_t,INDEX_PAYLOAD_TYPE>* bulk_load_values = new std::pair<int64_t,INDEX_PAYLOAD_TYPE>[num_keys];

   std::vector<std::pair<double,INDEX_PAYLOAD_TYPE>> bulk_load_values;
   bulk_load_values.reserve(num_keys);

    std::cout<<"Col index "<<column_index<<"\n";    
    int max_key = INT_MIN;
    for (int i=0;i<results.size();i++){
        int row_id = i;
        std::cout<<"before key"<<"\n";
        auto *data = dynamic_cast<Base *>(results[i][column_index].get());
        auto *data1 = dynamic_cast<Base *>(results[i][column_index + 1].get());
        
        int64_t key_ = static_cast<int64_t>(static_cast<BigIntData *>(data)->value);
        double value_ = static_cast<double_t>(static_cast<DoubleData *>(data1)->value);
        
        // std::cout<<"after key: "<<key_<<" Value: "<<value_<<"\n";

        bulk_load_values.emplace_back(key_,value_);

        std::cout<<"Bulk Load values : "<<bulk_load_values[i].first<<" "<<bulk_load_values[i].second<<"\n";
    }

    std::cout<<"Bulk Load Values Size Before Sort: "<<bulk_load_values.size()<<"\n";

    /**
     Phase 3: Sort the bulk load values array based on the key values.
    */
   //Measure time 
    
    auto start_time = std::chrono::high_resolution_clock::now();
    std::sort(bulk_load_values.begin(),bulk_load_values.end(),[](auto const& a, auto const& b) { return a.first < b.first; });
    
    /*
    Phase 4: Bulk load the sorted values into the index.
    */

    std::cout<<"Bulk Load Values Size : "<<bulk_load_values.size()<<"\n";
    
    // big_int_dynamic_index.bulk_load(bulk_load_values, num_keys);

    for (const auto &pair : bulk_load_values) {
        std::cout<<"Key : "<<pair.first<<" Value : "<<pair.second<<"\n";
        big_int_dynamic_index.insert_or_assign(pair.first, pair.second);
        std::cout<<"Index Size : "<<big_int_dynamic_index.size()<<"\n";
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_seconds = end_time - start_time;
    std::cout << "Time taken to bulk load: " << elapsed_seconds.count() <<" seconds\n\n\n";
    print_stats<INT64_KEY_TYPE>();
}

template<>
void bulkLoadIntoIndex<UNSIGNED_INT64_KEY_TYPE,INDEX_PAYLOAD_TYPE>(duckdb::Connection & con,std::string table_name,int column_index){
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
//    std::pair<UNSIGNED_INT64_KEY_TYPE,INDEX_PAYLOAD_TYPE>* bulk_load_values = new std::pair<UNSIGNED_INT64_KEY_TYPE,INDEX_PAYLOAD_TYPE>[num_keys];
    //std::cout<<"Col index "<<column_index<<"\n";    
    std::vector<std::pair<double,INDEX_PAYLOAD_TYPE>> bulk_load_values;
    bulk_load_values.reserve(num_keys);

    int max_key = INT_MIN;
    for (int i=0;i<results.size();i++){
        int row_id = i;
        //std::cout<<"before key"<<"\n";
        auto *data = dynamic_cast<Base *>(results[i][column_index].get());
        auto *data1 = dynamic_cast<Base *>(results[i][column_index + 1].get());
        
        UNSIGNED_INT64_KEY_TYPE key_ = static_cast<u_int64_t>(static_cast<UBigIntData *>(data)->value);
        double value_ = static_cast<double_t>(static_cast<DoubleData *>(data1)->value);
        
        //std::cout<<"after key"<<"\n";
        bulk_load_values.emplace_back(key_,value_);
    }
    /**
     Phase 3: Sort the bulk load values array based on the key values.
    */
   //Measure time 
    
    auto start_time = std::chrono::high_resolution_clock::now();
    std::sort(bulk_load_values.begin(),bulk_load_values.end(),[](auto const& a, auto const& b) { return a.first < b.first; });
    
    /*
    Phase 4: Bulk load the sorted values into the index.
    */
    
    // unsigned_big_int_dynamic_index.bulk_load(bulk_load_values, num_keys);

    for (const auto &pair : bulk_load_values) {
        unsigned_big_int_dynamic_index.insert_or_assign(pair.first, pair.second);
    }


    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_seconds = end_time - start_time;
    std::cout << "Time taken to bulk load: " << elapsed_seconds.count() <<" seconds\n\n\n";
    print_stats<UNSIGNED_INT64_KEY_TYPE>();
}

template<>
void bulkLoadIntoIndex<INT_KEY_TYPE,INDEX_PAYLOAD_TYPE>(duckdb::Connection & con,std::string table_name,int column_index){
/*
    Phase 1: Load the data from the table.
    */
    string query = "SELECT * FROM "+table_name+";";
    unique_ptr<MaterializedQueryResult> result = con.Query(query);
    results = result->getContents();
    int num_keys = results.size();
    std::cout<<"Num Keys : "<<num_keys<<"\n";

   /*
    Phase 2: Bulk load the data from the results vector into the pair array that goes into the index.
   */
//    std::pair<INT_KEY_TYPE,INDEX_PAYLOAD_TYPE>* bulk_load_values = new std::pair<INT_KEY_TYPE,INDEX_PAYLOAD_TYPE>[num_keys];
    std::cout<<"Col index "<<column_index<<"\n";    
    std::vector<std::pair<INT_KEY_TYPE,INDEX_PAYLOAD_TYPE>> bulk_load_values;
    bulk_load_values.reserve(num_keys);


    int max_key = INT_MIN;
    for (int i=0;i<results.size();i++){
        int row_id = i;
        if(results[i][column_index]) {
        std::cout<<"before key"<<"\n";
        auto *data = dynamic_cast<Base *>(results[i][column_index].get());
        auto *data1 = dynamic_cast<Base *>(results[i][column_index + 1].get());
        //auto rrr = results[i][column_index].get();
        
        INT_KEY_TYPE key_ = static_cast<int32_t>(static_cast<IntData *>(data)->value);
        double value_ = static_cast<double_t>(static_cast<DoubleData *>(data1)->value);

        std::cout<<"after key"<<"\n";
        bulk_load_values.emplace_back(key_,  static_cast<INDEX_PAYLOAD_TYPE>(i));
        std::cout<<"Key : "<<key_<<" Value : "<<value_<<"\n";
        }
    }
    /**
     Phase 3: Sort the bulk load values array based on the key values.
    */
   //Measure time 
    
    auto start_time = std::chrono::high_resolution_clock::now();
    std::sort(bulk_load_values.begin(),bulk_load_values.end(),[](auto const& a, auto const& b) { return a.first < b.first; });
    
    /*
    Phase 4: Bulk load the sorted values into the index.
    */
    
    // index.bulk_load(bulk_load_values, num_keys);

     for (const auto &pair : bulk_load_values) {
        std::cout<<"Key : "<<pair.first<<" Value : "<<pair.second<<"\n";
        int_dynamic_index.insert_or_assign(pair.first, pair.second);
    }



    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_seconds = end_time - start_time;
    std::cout << "Time taken to bulk load: " << elapsed_seconds.count() <<" seconds\n\n\n";
    print_stats<INT_KEY_TYPE>();
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
        std::cout<<"Column found at index "<<column_index<<"\n";
        std::cout<<"Creating an pgm index for this column"<<"\n";
        // std::cout<<"Column Type "<<typeid(column_type).name()<<"\n";
        // std::cout<<"Column Type "<<typeid(double).name()<<"\n";
        std::cout<<"Column type to string "<<column_type.ToString()<<"\n";
        std::string columnTypeName = column_type.ToString();
        if(columnTypeName == "DOUBLE"){
            bulkLoadIntoIndex<DOUBLE_KEY_TYPE,INDEX_PAYLOAD_TYPE>(con,table_name,column_index);
            index_type_table_name_map.insert({"double",{table_name,column_name}});
        }
        else if(columnTypeName == "BIGINT"){
            bulkLoadIntoIndex<INT64_KEY_TYPE,INDEX_PAYLOAD_TYPE>(con,table_name,column_index);
            index_type_table_name_map.insert({"bigint",{table_name,column_name}});
        }
        else if(columnTypeName == "UBIGINT"){
            bulkLoadIntoIndex<UNSIGNED_INT64_KEY_TYPE,INDEX_PAYLOAD_TYPE>(con,table_name,column_index);
            index_type_table_name_map.insert({"ubigint",{table_name,column_name}});
        }
        else if(columnTypeName == "INTEGER"){
            bulkLoadIntoIndex<INT_KEY_TYPE,INDEX_PAYLOAD_TYPE>(con,table_name,column_index);
            index_type_table_name_map.insert({"int",{table_name,column_name}});
        }
        else{
            std::cout<<"Unsupported column type for alex indexing (for now) "<<"\n";
        }
        //bulkLoadIntoIndex<typeid(column_type).name(),INDEX_PAYLOAD_TYPE>(con,table_name,column_index);
    }
}

void executeQuery(duckdb::Connection& con,string QUERY){
    unique_ptr<MaterializedQueryResult> result = con.Query(QUERY);
    if(result->HasError()){
        std::cout<<"Query execution failed "<<"\n";
    }
    else{
        std::cout<<"Query execution successful! "<<"\n";
    }
}

/**
 * Loading Benchmark into the tables of DuckDB.
 * 
*/

template <typename K,typename P>
void load_benchmark_data_into_table(std::string benchmarkFile,std::string benchmarkFileType,duckdb::Connection& con,std::string tableName,int NUM_KEYS,int num_batches_insert,int per_batch){
    //This function will load a key and payload type agnostic data into the database.
    int starting = 0;
    int ending = 0;

    auto keys = new K[NUM_KEYS];
    bool res = load_binary_data(keys,NUM_KEYS,benchmarkFile);

    std::cout<<"Res of loading from benchmark file "<<res<<"\n"; 

    string query = "INSERT INTO "+tableName+" VALUES ";


    for(int i=0;i<num_batches_insert;i++){
        std::cout<<"Inserting batch "<<i<<"\n";
        
        //KeyType batch_keys[per_batch];  // Replace KeyType with the actual type of keys
        starting = i*per_batch;

        ending = starting + per_batch;
        string tuple_string = "";

        // std::cout<<"Starting "<<starting<<" Ending "<<ending<<"\n";
        
        auto values = new std::pair<K, P>[per_batch];
        std::mt19937_64 gen_payload(std::random_device{}());


        for (int vti = starting; vti < ending; vti++) {
            //values[vti].first = keys[vti];
            K key = keys[vti];
            P random_payload = static_cast<P>(gen_payload());
            //std::cout<<"dae key "<<key<<"\n";
            std::ostringstream stream;
            if(typeid(K)==typeid(DOUBLE_KEY_TYPE)){
                stream << std::setprecision(std::numeric_limits<K>::max_digits10) << key;
            }
            else{
                stream << key;
            }
            std::string ressy = stream.str();
            tuple_string = tuple_string + "(" + ressy + "," + std::to_string(random_payload) + ")";
            //std::cout<<"Tuple string "<<tuple_string<<"\n";
            if(vti!=ending-1){
                tuple_string = tuple_string + ",";
            }
        }
        string to_execute_query = query + tuple_string + ";";

        auto res = con.Query(to_execute_query);
        if(!res->HasError()){
            std::cout<<"Batch inserted successfully "<<"\n";
        }else{
            std::cout<<"Error inserting batch "<<i<<"\n";
        }
    }
}

void functionLoadBenchmark(ClientContext &context, const FunctionParameters &parameters){
    std::string tableName = parameters.values[0].GetValue<string>();
    std::string benchmarkName = parameters.values[1].GetValue<string>();
    int benchmark_size = parameters.values[2].GetValue<int>();
    int num_batches_insert = parameters.values[3].GetValue<int>();

    std::cout<<"Loading benchmark data - "<<benchmarkName<<"into table "<<tableName<<"\n";
    std::cout<<"The schema of the table will be {key,payload}\n";
    std::cout<<"Number of keys  "<<benchmark_size<<"\n";
    
    load_end_point = benchmark_size;
    std::string benchmarkFile = "";
    std::string benchmarkFileType = "";
    const int NUM_KEYS = benchmark_size;

    //Establish a connection with the Database.
    duckdb::Connection con(*context.db);

    /**
     * Create a table with the table name.
    */
    std::string CREATE_QUERY = "";
    
    //int num_batches_insert = 1000;
    int per_batch = NUM_KEYS/num_batches_insert;
    std::cout<<"Per batch insertion "<<per_batch<<"\n";
    
    std::cout<<"Benchmark name "<<benchmarkName<<"\n";
    if(benchmarkName.compare("lognormal")==0){
        // benchmarkFile = "/Users/bhargavkrish/Desktop/USC/Duck_Extension/trial-3/intelligent-duck/src/lognormal-190M.bin";
        benchmarkFile = "/home/aman304gupta/Documents/RMI-DuckDB/src/lognormal-190M.bin";
        benchmarkFileType = "binary";
        CREATE_QUERY = "CREATE TABLE "+tableName+"(key BIGINT, payload double);";
        executeQuery(con,CREATE_QUERY);
        load_benchmark_data_into_table<INT64_KEY_TYPE,GENERAL_PAYLOAD_TYPE>(benchmarkFile,benchmarkFileType,con,tableName,NUM_KEYS,num_batches_insert,per_batch);
    }
    else if(benchmarkName.compare("longitudes")==0){
        // benchmarkFile = "/Users/bhargavkrish/Desktop/USC/Duck_Extension/trial-3/intelligent-duck/src/longitudes-200M.bin";
        benchmarkFile = "/home/aman304gupta/Documents/RMI-DuckDB/src/longitudes-200M.bin";
        benchmarkFileType = "binary";
        CREATE_QUERY = "CREATE TABLE "+tableName+"(key double, payload double);";
        executeQuery(con,CREATE_QUERY);
        load_benchmark_data_into_table<DOUBLE_KEY_TYPE,GENERAL_PAYLOAD_TYPE>(benchmarkFile,benchmarkFileType,con,tableName,NUM_KEYS,num_batches_insert,per_batch);
    }
    else if(benchmarkName.compare("longlat")==0){
        // benchmarkFile = "/Users/bhargavkrish/Desktop/USC/Duck_Extension/trial-3/intelligent-duck/src/longlat-200M.bin";
        benchmarkFile = "/home/aman304gupta/Documents/RMI-DuckDB/src/longlat-200M.bin";
        benchmarkFileType = "binary";
        CREATE_QUERY = "CREATE TABLE "+tableName+"(key double, payload double);";
        executeQuery(con,CREATE_QUERY);
        load_benchmark_data_into_table<DOUBLE_KEY_TYPE,GENERAL_PAYLOAD_TYPE>(benchmarkFile,benchmarkFileType,con,tableName,NUM_KEYS,num_batches_insert,per_batch);
    }
    else if(benchmarkName.compare("ycsb")==0){
        // benchmarkFile = "/Users/bhargavkrish/Desktop/USC/Duck_Extension/trial-3/intelligent-duck/src/ycsb-200M.bin";
        benchmarkFile = "/home/aman304gupta/Documents/RMI-DuckDB/src/ycsb-200M.bin";
        benchmarkFileType = "binary";
        std::cout<<"Table name "<<tableName<<"\n";
        CREATE_QUERY = "CREATE TABLE "+tableName+"(key UBIGINT , payload double);";
        executeQuery(con,CREATE_QUERY);
        //Args: Benchmark Key Type, Benchmark Payload Type, Benchmark File, Benchmark File Type, conn object, table name, NUM_KEYS,num_batches_insert, per_batch
        load_benchmark_data_into_table<UNSIGNED_INT64_KEY_TYPE,GENERAL_PAYLOAD_TYPE>(benchmarkFile,benchmarkFileType,con,tableName,NUM_KEYS,num_batches_insert,per_batch);
    }

}

/*
Run the Benchmarks on different indexes.
*/

template <typename K>
void runLookupBenchmarkPgm(K *keys);

template <>
void runLookupBenchmarkPgm(double *keys){

    if(double_dynamic_index.size()==0){
        std::cout<<"Index is empty. Please load the data into the index first."<<"\n";
        return;
    }

    std::cout<<"Running benchmark workload "<<"\n";
    int init_num_keys = load_end_point;
    int total_num_keys = 40000;
    int batch_size = 10000;
    double insert_frac = 0.5;
    string lookup_distribution = "zipf";
    int i = init_num_keys;
    long long cumulative_inserts = 0;
    long long cumulative_lookups = 0;
    int num_inserts_per_batch = static_cast<int>(batch_size * insert_frac);
    int num_lookups_per_batch = batch_size - num_inserts_per_batch;
    double cumulative_insert_time = 0;
    double cumulative_lookup_time = 0;
    double time_limit = 0.1;
    bool print_batch_stats = true;
    


    auto workload_start_time = std::chrono::high_resolution_clock::now();
    int batch_no = 0;
    INDEX_PAYLOAD_TYPE sum = 0;
    std::cout << std::scientific;
    std::cout << std::setprecision(3);

    while (true) {
        batch_no++;

        // Do lookups
        double batch_lookup_time = 0.0;
        if (i > 0) {
        double* lookup_keys = nullptr;
        if (lookup_distribution == "uniform") {
            lookup_keys = get_search_keys(keys, i, num_lookups_per_batch);
        } else if (lookup_distribution == "zipf") {
            lookup_keys = get_search_keys_zipf(keys, i, num_lookups_per_batch);
        } else {
            std::cerr << "--lookup_distribution must be either 'uniform' or 'zipf'"
                    << std::endl;
            //return 1;
        }
        auto lookups_start_time = std::chrono::high_resolution_clock::now();
        for (int j = 0; j < num_lookups_per_batch; j++) {
            double key = lookup_keys[j];
            // INDEX_PAYLOAD_TYPE* payload = double_index.get_payload(key); pointer returned
            // INDEX_PAYLOAD_TYPE payload = double_dynamic_index.find(key)->second;
            // std::cout<<"Key "<<key<<" Payload "<<*payload<<"\n";
            if (double_dynamic_index.find(key) != double_dynamic_index.find(key)) {
                std::cout<<"Payload is there! "<<"\n";
                sum += double_dynamic_index.find(key)->second;
            }
            else{
                std::cout<<"Payload is not here!! "<<"\n";
            }
        }
        auto lookups_end_time = std::chrono::high_resolution_clock::now();
        batch_lookup_time = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                lookups_end_time - lookups_start_time)
                                .count();
        cumulative_lookup_time += batch_lookup_time;
        cumulative_lookups += num_lookups_per_batch;
        delete[] lookup_keys;
        }
        double workload_elapsed_time =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::high_resolution_clock::now() - workload_start_time)
                .count();
        if (workload_elapsed_time > time_limit * 1e9 * 60) {
        break;
        }
        if (print_batch_stats) {
        int num_batch_operations = num_lookups_per_batch;
        double batch_time = batch_lookup_time;
        long long cumulative_operations = cumulative_lookups;
        double cumulative_time = cumulative_lookup_time;
        std::cout << "Batch " << batch_no
                    << ", cumulative ops: " << cumulative_operations
                    << "\n\tbatch throughput:\t"
                    << num_lookups_per_batch / batch_lookup_time * 1e9
                    << " lookups/sec,\t"
                    << cumulative_lookups / cumulative_lookup_time * 1e9
                    << " lookups/sec,\t"
                    << cumulative_operations / cumulative_time * 1e9 << " ops/sec"
                    << std::endl;
        }
    }
    long long cumulative_operations = cumulative_lookups + cumulative_inserts;
    double cumulative_time = cumulative_lookup_time + cumulative_insert_time;
    std::cout << "Cumulative stats: " << batch_no << " batches, "
                << cumulative_operations << " ops (" << cumulative_lookups
                << " lookups, " << cumulative_inserts << " inserts)"
                << "\n\tcumulative throughput:\t"
                << cumulative_lookups / cumulative_lookup_time * 1e9
                << " lookups/sec,\t"
                << cumulative_inserts / cumulative_insert_time * 1e9
                << " inserts/sec,\t"
                << cumulative_operations / cumulative_time * 1e9 << " ops/sec"
                << std::endl;

    delete[] keys;
}



template <>
void runLookupBenchmarkPgm(INT64_KEY_TYPE *keys){


    if(big_int_dynamic_index.size()==0){
        std::cout<<"Index is empty. Please load the data into the index first."<<"\n";
        return;
    }

    std::cout<<"Running benchmark workload "<<"\n";

    int init_num_keys = load_end_point;
    int total_num_keys = 400000;
    int batch_size = 100000;
    double insert_frac = 0.5;
    string lookup_distribution = "zipf";
    int i = init_num_keys;
    long long cumulative_inserts = 0;
    long long cumulative_lookups = 0;
    int num_lookups_per_batch = batch_size;
    double cumulative_lookup_time = 0;
    double time_limit = 0.1;
    bool print_batch_stats = true;
    double elapsed_time_seconds = 0;

    std::cout<<"Num lookups per batch "<<num_lookups_per_batch<<"\n";

    
    auto workload_start_time = std::chrono::high_resolution_clock::now();
    int batch_no = 0;
    INDEX_PAYLOAD_TYPE sum = 0;
    std::cout << std::scientific;
    std::cout << std::setprecision(3);

    while (true) {
        batch_no++;

        // Do lookups
        double batch_lookup_time = 0.0;
        if (i > 0) {
        int64_t* lookup_keys = nullptr;
        if (lookup_distribution == "uniform") {
            lookup_keys = get_search_keys(keys, i, num_lookups_per_batch);
        } else if (lookup_distribution == "zipf") {
            lookup_keys = get_search_keys_zipf(keys, i, num_lookups_per_batch);
        } else {
            std::cerr << "--lookup_distribution must be either 'uniform' or 'zipf'"
                    << std::endl;
            //return 1;
        }
        auto lookups_start_time = std::chrono::high_resolution_clock::now();
        for (int j = 0; j < num_lookups_per_batch; j++) {
            int64_t key = lookup_keys[j];
            // INDEX_PAYLOAD_TYPE* payload = big_int_dynamic_index.get_payload(key);
            //std::cout<<"Key "<<key<<" Payload "<<*payload<<"\n";
            if (big_int_dynamic_index.find(key) != big_int_dynamic_index.end())  {
                //std::cout<<"Payload is there! "<<"\n";
                sum += big_int_dynamic_index.find(key)->second;
            }
            else{
                std::cout<<"Payload is not here!! "<<"\n";
            }
        }
        auto lookups_end_time = std::chrono::high_resolution_clock::now();

        auto elapsed_seconds = lookups_end_time - lookups_start_time;
        elapsed_time_seconds = elapsed_seconds.count();

        batch_lookup_time = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                lookups_end_time - lookups_start_time)
                                .count();
        cumulative_lookup_time += batch_lookup_time;
        cumulative_lookups += num_lookups_per_batch;
        delete[] lookup_keys;
        }
        double workload_elapsed_time =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::high_resolution_clock::now() - workload_start_time)
                .count();
        if (workload_elapsed_time > time_limit * 1e9 * 60) {
            break;
        }
        if (print_batch_stats) {
            int num_batch_operations = num_lookups_per_batch;
            double batch_time = batch_lookup_time;
            long long cumulative_operations = cumulative_lookups;
            double cumulative_time = cumulative_lookup_time;
            std::cout << "Batch " << batch_no
                        <<"Batch lookup time "<<elapsed_time_seconds<<"\n"
                        << ", cumulative ops: " << cumulative_operations
                        << "\n\tbatch throughput:\t"
                        << num_lookups_per_batch / batch_lookup_time * 1e9
                        << " lookups/sec,\t"
                        << cumulative_lookups / cumulative_lookup_time * 1e9
                        << " lookups/sec,\t"
                        << cumulative_operations / cumulative_time * 1e9 << " ops/sec"
                        << std::endl;
            }
    }
    // long long avg_lookup_time_per_batch = cumulative_lookups/batch_no;
    // std::cout<<"Average number of lookups per batch "<<avg_lookup_operations_per_batch<<"\n";
    long long cummulative_time = cumulative_lookup_time;
    std::cout<<"Cumulative time "<<cummulative_time<<"\n";
    std::cout<<"Cumulative lookups "<<cumulative_lookups<<"\n";
    std::cout<<"Throughput : "<< cumulative_lookups / cumulative_lookup_time * 1e9<<"ops/sec\n";
    // long long cumulative_operations = cumulative_lookups + cumulative_inserts;
    // double cumulative_time = cumulative_lookup_time + cumulative_insert_time;
    // std::cout << "Cumulative stats: " << batch_no << " batches, "
    //             << cumulative_operations << " ops (" << cumulative_lookups
    //             << " lookups, " << cumulative_inserts << " inserts)"
    //             << "\n\tcumulative throughput:\t"
    //             << cumulative_lookups / cumulative_lookup_time * 1e9
    //             << " lookups/sec,\t"
    //             << cumulative_inserts / cumulative_insert_time * 1e9
    //             << " inserts/sec,\t"
    //             << cumulative_operations / cumulative_time * 1e9 << " ops/sec"
    //             << std::endl;

    delete[] keys;
}


void functionRunLookupBenchmark(ClientContext &context, const FunctionParameters &parameters){
    std::cout<<"Running lookup benchmark"<<"\n";
    std::string benchmarkName = parameters.values[0].GetValue<string>();
    std::string index = parameters.values[1].GetValue<string>();
    duckdb::Connection con(*context.db);

    std::string keys_file_path = "";
    if(benchmarkName == "lognormal"){
        // keys_file_path = "/Users/bhargavkrish/Desktop/USC/Duck_Extension/trial-3/intelligent-duck/src/lognormal-190M.bin";
        keys_file_path = '/home/aman304gupta/Documents/RMI-DuckDB/src/lognormal-190M.bin';
        auto keys = new INT64_KEY_TYPE[load_end_point];
        std::cout<<"Loading binary data "<<std::endl;
        load_binary_data(keys, load_end_point, keys_file_path);
        if(index == "pgm"){
            runLookupBenchmarkPgm<INT64_KEY_TYPE>(keys);
        }
        // else{
        //     runLookupBenchmarkArt<INT64_KEY_TYPE>(keys,con,benchmarkName);
        // }
    }
    else if(benchmarkName == "longlat"){
        // keys_file_path = "/Users/bhargavkrish/Desktop/USC/Duck_Extension/trial-3/intelligent-duck/src/longlat-200M.bin";
        keys_file_path = '/home/aman304gupta/Documents/RMI-DuckDB/src/longlat-200M.bin';
        auto keys = new DOUBLE_KEY_TYPE[load_end_point];
        std::cout<<"Loading binary data "<<std::endl;
        load_binary_data(keys, load_end_point, keys_file_path);
        if(index == "pgm"){
            runLookupBenchmarkPgm<DOUBLE_KEY_TYPE>(keys);
        }
        // else{
        //     runLookupBenchmarkArt<DOUBLE_KEY_TYPE>(keys,con,benchmarkName);
        // }
    }
    else if(benchmarkName=="ycsb"){
        // keys_file_path = "/Users/bhargavkrish/Desktop/USC/Duck_Extension/trial-3/intelligent-duck/src/ycsb-200M.bin";
        keys_file_path = '/home/aman304gupta/Documents/RMI-DuckDB/src/ycsb-200M.bin';
        auto keys = new INT64_KEY_TYPE[load_end_point];
        std::cout<<"Loading binary data "<<std::endl;
        load_binary_data(keys, load_end_point, keys_file_path);
        if(index == "pgm"){
            runLookupBenchmarkPgm<INT64_KEY_TYPE>(keys);
        }
        // else{
        //     runLookupBenchmarkArt<INT64_KEY_TYPE>(keys,con,benchmarkName);
        // }
    }
    else if(benchmarkName == "longitudes"){
        // keys_file_path = "/Users/bhargavkrish/Desktop/USC/Duck_Extension/trial-3/intelligent-duck/src/longitudes-200M.bin";
        keys_file_path = '/home/aman304gupta/Documents/RMI-DuckDB/src/longitudes-200M.bin';
        auto keys = new DOUBLE_KEY_TYPE[load_end_point];
        std::cout<<"Loading binary data "<<std::endl;
        load_binary_data(keys, load_end_point, keys_file_path); 
        if(index == "pgm"){
            runLookupBenchmarkPgm<DOUBLE_KEY_TYPE>(keys);
        }
        // else{
        //     runLookupBenchmarkArt<DOUBLE_KEY_TYPE>(keys,con,benchmarkName);
        // }
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

    // The arguments for the load benchmark data function are the table name, benchmark name and the number of elements to bulk load.
    auto loadBenchmarkData = PragmaFunction::PragmaCall("load_benchmark",functionLoadBenchmark,{LogicalType::VARCHAR,LogicalType::VARCHAR,LogicalType::INTEGER,LogicalType::INTEGER},{});
    ExtensionUtil::RegisterFunction(instance,loadBenchmarkData);

    auto runBenchmarkWorkload = PragmaFunction::PragmaCall("run_lookup_benchmark",functionRunLookupBenchmark,{LogicalType::VARCHAR,LogicalType::VARCHAR},{});
    ExtensionUtil::RegisterFunction(instance,runBenchmarkWorkload);

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
