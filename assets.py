from dagster import asset, OpExecutionContext, define_asset_job, AssetSelection, Output
from datetime import datetime
import os 
import pandas as pd  
import sqlalchemy
from sqlalchemy import create_engine, inspect, MetaData, Column, Table
#from clickhouse_sqlalchemy import engines

#Job configuration. Dictionary defines the configuration for each table to be replicated. Target table name, if_exists strategy, optional parameters chunksize and tgt_table.
config_dict = {
        #Success
        "countries": { "if_exists": "append"},
        
        #Success. Ran for 1minute, 48 seconds and wrote the file. 
        "domain_info":{"if_exists": "append", "chunk_size": 100000},
        
        #Ran for 13 seconds. Row count: 10000. Error: 'float' object has no attribute 'encode'. Column mismatch. Need to fix target db data types from str to dec.
        "new_user_performance_by_day":{"if_exists":"append"},
        
        #Column mismatch. Need to fix target db tables from str to decimal(12,2)
        "new_user_performance_by_day_domain":{"if_exists":"append",
                                              "tgt_table":"new_user_performance_by_day_domain_currentyear_shadow"},
        
        #decimal(12,2) in source db is defined as a str in target.
        "parking_visit_earnings":{"if_exists":"append"},

        #decimal(12,2) in source db is defined as a str in target.
        "parking_visits_summary":{"if_exists":"append"},
        
        #Success, 150 million records need to handle.
        "s2s_log":{"if_exists":"append", "chunksize": 100000},
        
        #Success. Row count: 0. This shadow table is not in Clickhouse.
        "user_estimate_performance_by_day":{"if_exists":"append"},
        
        #Success. Row count: 0. This shadow table is not in Clickhouse.
        "user_estimate_performance_by_day_domain":{"if_exists":"append"},
        
        #Successful. 25 million records.
        "user_folder_summary":{"if_exists":"append", "chunksize": 100000},
        
        #Error: 'int' object has no attribute 'encode'
        "users":{"if_exists":"append"}
    }

#Define the load_asset_factory. Generates an asset function for each table specified in the config_dict. Establish database connectivity to MySQL and CH. MySQL query to fetch data from the source table in chunks. Iterate over the chunks of data using pd.read_sql. Logs the chunk count and column data types. Push the data to a temporary table in CH using df.to_sql. Return Output object with metadata containing the row count.
def load_asset_factory(tgt_table, table_name: str, if_exists: str = "append", chunksize: int = 10000, max_chunk: int = 1000000):
    @asset(name=f"{table_name}_load", group_name="mysql_to_clickhouse")
    def mysql_to_clickhouse_asset(context: OpExecutionContext):
        # Stash these secrets

        mysql_username = ("data_taylor_read_only") 
        mysql_password = ("hBb51assR0LhI!CttofE8f5Rc3aidofF")
        mysql_host = ("192.168.12.50")
        mysql_database = ("bodis_myisam")

        clickhouse_username = ("data_taylor_rw")
        clickhouse_password = ("IxAt0Ph07tMD6pinwGO9TyFzuWoRz8HQ")
        clickhouse_host = ("192.168.12.52")
        clickhouse_database = ("bodis_reporting")

        #Establish database connectivity
        mysql_connection_url = f"mysql+pymysql://{mysql_username}:{mysql_password}@{mysql_host}/{mysql_database}"
        clickhouse_connection_url = f"clickhouse+native://{clickhouse_username}:{clickhouse_password}@{clickhouse_host}/{clickhouse_database}"
        
        mysql_engine = create_engine(mysql_connection_url)
        clickhouse_engine = create_engine(clickhouse_connection_url)
        context.log.debug("clickhouse_engine:\n%s",clickhouse_engine)
        #Initialize variables
        
        mysql_query = f"""
        SELECT * FROM {table_name} 
        LIMIT {max_chunk}
        ;
        """
        chunk_count = 0

        

        # Ingest data from source
        try:
            for df in pd.read_sql(mysql_query, mysql_engine, chunksize=chunksize):
                if "calling_code" in df.columns:
                    df["calling_code"] = df["calling_code"].fillna(1).astype(int)
                if "google_drid_has_address" in df.columns:
                    df["google_drid_has_address"] = pd.to_numeric(df['google_drid_has_address'], errors='coerce').fillna(1).astype(int)
                if "zc_partner_id" in df.columns:
                    df["zc_partner_id"] = pd.to_numeric(df["zc_partner_id"], errors='coerce').fillna(0).astype(int)
                if 'time_added_to_system' in df.columns:
                    df['time_added_to_system'] = pd.to_datetime(df['time_added_to_system'])
                if 'template_id' in df.columns:
                    df['template_id'] = pd.to_numeric(df['template_id'], errors='coerce').fillna(0).astype(int)
                if 'zc_winner_bid' in df.columns:
                    df['zc_winner_bid'] = pd.to_numeric(df['zc_winner_bid'], errors='coerce').fillna(0).astype(int)
                if 'ip_address' in df.columns:
                    df['ip_address'] = df['ip_address'].astype(str)
                
                
                # Establish a chunk_count
                chunk_count += len(df)
                context.log.debug(df)
                for column in df.columns:
                    context.log.debug(f"Column: {column}, Data type: {df[column].dtype}")
                context.log.debug('row count: %s', chunk_count)

                # If no records, skip
                if chunk_count == 0:
                    context.log.info("No records to process.")
                else:
                    # Push data to temporary table in ClickHouse
                    if tgt_table is None:
                        tgt_table_name = f"{table_name}_shadow"
                    else:
                        tgt_table_name = tgt_table
                    df.to_sql(tgt_table_name, clickhouse_engine, index=False, if_exists=if_exists)
        except Exception as e:
            context.log.error("Error:\n%s", e)
            raise e

        
        return Output(
            None,
            metadata={"row_count": chunk_count}
        )
    
    return mysql_to_clickhouse_asset

# define the validation asset factory. Generate validation asset function for each table. Take the table_name as a parameter and log a success message.

def validation_asset_factory(table_name: str):
    @asset(name=f"{table_name}_validate", group_name="mysql_to_clickhouse", deps=f"{table_name}_load")
    def validation_asset(context: OpExecutionContext):
        context.log.info("That Worked")
    return validation_asset

#this generates load assets by iterating over the config_dict. For each table extract table name, if_exists strategy, chunksize, and tgt_table. Call load_asset_factory to create a load asset function for each table and append it to the load_asset_list
def create_load_assets():
    load_asset_list = []
    for table_key, table_config in config_dict.items():
        table_name = table_key.lower()
        if_exists = table_config.get("if_exists", None)
        chunksize = table_config.get("chunksize", 10000)
        tgt_table = table_config.get("tgt_table", None)
        load_asset = load_asset_factory(tgt_table=tgt_table,table_name=table_name, if_exists=if_exists, chunksize=chunksize)
        load_asset_list.append(load_asset)
    return load_asset_list

#generates validation assets by iterating over the config_dict. For each table, extract the table name and tgt_table from configuration. Call the load_asset_factory to create a validation asset function for each table and append it to the validation_asset_list.
def create_validation_assets():
    validation_asset_list = []
    for table_key, table_config in config_dict.items():
        table_name = table_key.lower()
        tgt_table = table_config.get("tgt_table", None)
        load_asset = load_asset_factory(tgt_table=tgt_table, table_name=table_name)
        validation_asset_list.append(load_asset)
    return validation_asset_list

mysql_to_clickhouse_assets = create_load_assets()

validation_assets=create_validation_assets()

#create the job using define_asset_job. Job is configured to materialize the assets in the mysql_to_clickhouse group
mysql_to_clickhouse_job = define_asset_job(
    name="mysql_to_clickhouse_job",
    description="Materializes the MySQL to Clickhouse asset.",
    selection=AssetSelection.groups("mysql_to_clickhouse")
)



