# Airflow and Standard Library Includes
from airflow.sdk import dag, task, task_group
from airflow.exceptions import AirflowFailException
import logging, time, os
# For every database you connect to using Airflow you must install the
# provider.
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
# Airflow Operator for executing SQL from a file
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# Pull in our custom operators
from includes.operators.MsSqlCopyOperator import MsSqlCopyOperator


# Notice the template_searchpath directive, it is used by the SQLExecuteQueryOperator
# to find your SQL files.
# For our airflow Docker install os.getcwd() returns '/opt/airflow'
@dag(
    schedule=None,
    catchup=False,
    tags=["IST346", "testing"],
    template_searchpath=os.path.join(os.getcwd(), 'dags', 'includes', 'sql')
)

def load_dimensions():
    """
    Create remaining dimensions
    """
    dim_tables = ['dim_staff', 'dim_customer', 'dim_store']
    @task_group(group_id="dwh_load")
    def load():

        # We must drop the foreign key constraints that exist on the fact table
        # before we can truncate and then load the dimensions
        drop_fk_constraints = SQLExecuteQueryOperator(
                             task_id='drop_fact_fks',
                             conn_id='cisat_sql',
                             sql='drop_fact_fks_staff_customer_store.sql',
                             split_statements=True
                             )
        
        # Recreate the foreign key relationships between the fact table
        # and dim_staff, dim_customer, and dim_store
        create_fk_constraints = SQLExecuteQueryOperator(
                             task_id='create_fact_fks',
                             conn_id='cisat_sql',
                             sql='create_fact_fks_staff_customer_store.sql',
                             split_statements=True
                             )

        @task
        def truncate_dest():
            logger = logging.getLogger(__name__)
            # Get the connection to the database that we configured in Airflow
            hook_db = MsSqlHook(mssql_conn_id="cisat_sql")
            conn = hook_db.get_conn()
            # Get the cursor
            cursor = conn.cursor()
            # First truncate the [dim_date] table
            for table in dim_tables:
                cursor.execute(f"truncate table [sakila_dwh].[{table}]")
                # Reset the IDENTITY column
                cursor.execute(f"DBCC CHECKIDENT ('[sakila_dwh].[{table}]', RESEED, 1)")
                conn.commit()
                logger.info(f"finished truncating [sakila_dwh].[{table}] and resetting the IDENTITY column.")

        # In sakila_dwh dim_film has an identity column film_key, since it auto populates MS SQL seems to know
        # not to try to insert a values, and so the insert statement with no column names still works. However,
        # it would probably be best to rewrite the operator to specify column names, at least optionally.
        load_dim_customer = MsSqlCopyOperator(task_id = "load_dim_customer",
                                      src_conn_id = "cisat_sql", src_schema = "stage", src_table = "dim_customer",
                                      dest_conn_id = "cisat_sql", dest_schema = "sakila_dwh", dest_table = "dim_customer",
                                      batch_size = 5000, bulk_copy = False)
        load_dim_staff = MsSqlCopyOperator(task_id = "load_dim_staff",
                                      src_conn_id = "cisat_sql", src_schema = "stage", src_table = "dim_staff",
                                      dest_conn_id = "cisat_sql", dest_schema = "sakila_dwh", dest_table = "dim_staff",
                                      batch_size = 5000, bulk_copy = False)
        load_dim_store = MsSqlCopyOperator(task_id = "load_dim_store",
                                      src_conn_id = "cisat_sql", src_schema = "stage", src_table = "dim_store",
                                      dest_conn_id = "cisat_sql", dest_schema = "sakila_dwh", dest_table = "dim_store",
                                      batch_size = 5000, bulk_copy = False)

        drop_fk_constraints >> truncate_dest() >> [load_dim_customer, load_dim_staff, load_dim_store] >> create_fk_constraints
        


    @task_group(group_id="extract")
    def extract():
        extract_inventory_table = MsSqlCopyOperator(task_id = "extract_inventory_table",
                                        src_conn_id = "Sakila_DB", src_schema = "dbo", src_table = "inventory",
                                        dest_conn_id = "cisat_sql", dest_schema = "stage", dest_table = "inventory",
                                        batch_size = 5000, bulk_copy = True)
        extract_rental_table = MsSqlCopyOperator(task_id = "extract_rental_table",
                                        src_conn_id = "Sakila_DB", src_schema = "dbo", src_table = "rental",
                                        dest_conn_id = "cisat_sql", dest_schema = "stage", dest_table = "rental",
                                        batch_size = 5000, bulk_copy = True)
        extract_payment_table = MsSqlCopyOperator(task_id = "extract_payment_table",
                                        src_conn_id = "Sakila_DB", src_schema = "dbo", src_table = "payment",
                                        dest_conn_id = "cisat_sql", dest_schema = "stage", dest_table = "payment",
                                        batch_size = 5000, bulk_copy = True)
        extract_store_table = MsSqlCopyOperator(task_id = "extract_store_table",
                                        src_conn_id = "Sakila_DB", src_schema = "dbo", src_table = "store",
                                        dest_conn_id = "cisat_sql", dest_schema = "stage", dest_table = "store",
                                        batch_size = 5000, bulk_copy = True)
        extract_staff_table = MsSqlCopyOperator(task_id = "extract_staff_table",
                                        src_conn_id = "Sakila_DB", src_schema = "dbo", src_table = "staff",
                                        dest_conn_id = "cisat_sql", dest_schema = "stage", dest_table = "staff",
                                        batch_size = 5000, bulk_copy = True)
        extract_customer_table = MsSqlCopyOperator(task_id = "extract_customer_table",
                                        src_conn_id = "Sakila_DB", src_schema = "dbo", src_table = "customer",
                                        dest_conn_id = "cisat_sql", dest_schema = "stage", dest_table = "customer",
                                        batch_size = 5000, bulk_copy = True)
        extract_address_table = MsSqlCopyOperator(task_id = "extract_address_table",
                                        src_conn_id = "Sakila_DB", src_schema = "dbo", src_table = "address",
                                        dest_conn_id = "cisat_sql", dest_schema = "stage", dest_table = "address",
                                        batch_size = 5000, bulk_copy = True)
        extract_city_table = MsSqlCopyOperator(task_id = "extract_city_table",
                                        src_conn_id = "Sakila_DB", src_schema = "dbo", src_table = "city",
                                        dest_conn_id = "cisat_sql", dest_schema = "stage", dest_table = "city",
                                        batch_size = 5000, bulk_copy = True)
        extract_country_table = MsSqlCopyOperator(task_id = "extract_country_table",
                                        src_conn_id = "Sakila_DB", src_schema = "dbo", src_table = "country",
                                        dest_conn_id = "cisat_sql", dest_schema = "stage", dest_table = "country",
                                        batch_size = 5000, bulk_copy = True)        


        # call our operator like it is a task in the DAG
        # everything on its own line will be executed in parallel by Airflow
        # everything that is seperated by the >> operator will be executed in series by airflow
        # we don't want to start up too many parallel jobs at the same time here
        extract_inventory_table >> extract_store_table >> extract_staff_table >> extract_payment_table
        extract_address_table >> extract_city_table >> extract_country_table >> extract_customer_table >> extract_rental_table

        
    
    @task_group(group_id='transform')
    def transform():
        
        transform_customer = SQLExecuteQueryOperator(
                             task_id='transform_customer',
                             conn_id='cisat_sql',
                             sql='transform_dim_customer.sql',
                             )
        transform_staff = SQLExecuteQueryOperator(
                          task_id='transform_staff',
                          conn_id='cisat_sql',
                          sql='transform_dim_staff.sql',
                          )
        transform_store = SQLExecuteQueryOperator(
                          task_id='transform_store',
                          conn_id='cisat_sql',
                          sql='transform_dim_store.sql',
                          )

        @task
        def truncate():
            logger = logging.getLogger(__name__)
            # Get the connection to the database that we configured in Airflow
            hook_db = MsSqlHook(mssql_conn_id="cisat_sql")
            conn = hook_db.get_conn()
            # Get the cursor
            cursor = conn.cursor()

            # truncate each of the dim tables in stage
            for table in dim_tables:
                cursor.execute(f"truncate table [stage].[{table}]")
                conn.commit()                
                logger.info(f"Truncated [stage].[{table}]")

        @task
        def verify():
            logger = logging.getLogger(__name__)
            # Get the connection to the database that we configured in Airflow
            hook_db = MsSqlHook(mssql_conn_id="cisat_sql")
            conn = hook_db.get_conn()
            # Get the cursor
            cursor = conn.cursor()
            # In this case the transform is not adding any rows to the table
            # so a very simple (and not really sufficient) verification is to just
            # count the number of rows and compare them.
            verification_src = "select count(*) from [stage].[film]"
            verification_dest = "select count(*) from [stage].[dim_film]"
            cursor.execute(verification_src)
            row = cursor.fetchone()
            src_rows = row[0]
            cursor.execute(verification_dest)
            row = cursor.fetchone()
            dest_rows = row[0]
            if src_rows != dest_rows:
                raise AirflowFailException("Transform failed, [stage].[dim_film] does not have the correct number of rows.")
            logger.info("Transform Verified Successfully")
        
        # first we truncate our stage table, then do the transform that loads dim_fact, then verify the transform
        truncate() >> [transform_customer, transform_staff, transform_store]
    
    # first extract, then transform
    #extract() >> transform() >> load()
    extract() >> transform() >> load()

load_dimensions()