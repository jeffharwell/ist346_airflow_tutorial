from airflow.sdk.bases.operator import BaseOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.exceptions import AirflowFailException
import time

class MsSqlCopyOperator(BaseOperator):
    """
    The pymssql implementation of executemany has a tendency to silently drop rows. This can
    happen if a batch is too large or if too many batches have been submitted and a commit is
    called before all of the batches have processed.

    This operator implements the batch copy of table from one Microsoft SQL Server 
    connection to another which can either use the bulk copy method or the executemany method.

    Bulk copy is more reliable, and considerably faster. However, a bulk copy must insert all columns
    in a table and does not work with IDENTITY columns. So when inserting into a table with an IDENTIY
    column, executemany() must be used.

    This code is based on the best practices document from Astronomer.
    https://www.astronomer.io/docs/learn/airflow-importing-custom-hooks-operators#create-a-custom-operator

    This operator assumes that the destination table already exists in the destination schema
    and that it is identical to the source table in terms of column names.
    """

    def __init__(self, src_conn_id, src_schema, src_table, dest_conn_id, dest_schema, 
                 dest_table, batch_size, bulk_copy, *args, **kwargs):
        """
        :param task_id: the string to use for the task id of the operator in the Airflow UI
        :param src_name: the hook of the source database
        :param src_schema: the schema of the source table
        :param src_table: the name of the source table
        :param dest_hook: the hook of the destination database
        :param dest_schema: the schema of the destination table
        :param dest_table: the 
        :param batch_size: the batch size used to extract from the source table
        :param bulk_copy: boolean, instruct the operator to use the bulk copy method, otherwise
                          the executemany method is used to insert data.
        """
        # We are inheriting from the parent class, BaseOperator, so start by calling
        # the BaseOperator constructor
        super().__init__(*args, **kwargs)

        # Now process any of the custom arguments that were passed
        self.src_hook = MsSqlHook(mssql_conn_id=src_conn_id)
        self.src_schema = src_schema
        self.src_table = src_table
        self.dest_hook = MsSqlHook(mssql_conn_id=dest_conn_id)
        self.dest_schema = dest_schema
        self.dest_table = dest_table
        self.batch_size = batch_size
        self.bulk_copy = bulk_copy
        # I determined this value experimentally, the actual limit is known at the moment
        # technically this should be bytes in a specific encoding as per
        # https://github.com/pymssql/pymssql/blob/master/src/pymssql/_mssql.pyx#L676
        # ,not characters, but since we are just estimating ... this is about the right
        # number of characters.
        self.max_sql_server_query_string_size = 65000
    
    def pre_execute(self, context):
        # airflow calls this method before execution
        self.log.info("Pre-execution complete")

    def estimate_max_batch_size(self, data_set, query = ''):
        """
        If the batch size is too large, executemany seems to just silently discard some of the 
        data.

        https://github.com/pymssql/pymssql/blob/master/src/pymssql/_mssql.pyx#L1297 seems to indicate
        that we are concatenating the statements into a single large string and then doing an exec 
        of that string using the following code:
            dbcmd(self.dbproc, query_string_cstr)
            rtc = db_sqlexec(self.dbproc)

        # https://www.sqlservercentral.com/forums/topic/max-string-length-for-exec-command seems to indicate
        # that there is a maximum string length for the execute command in SQL Server. The amount of data
        # that can successfully be inserted seems to vary based on the size of the data set that is being inserted,
        # so my guess is that we are hitting this limit, and that if we stay under the limit all data will be inserted
        # successfully.

        # This function uses the data in data_set, and our insert query, to estimate the maximum batch size that we can 
        # use to do our inserts

        The bulk copy operation does not use a query, in which case the default '' is used and the query
        is not considered in the estimation of the batch size.

        :param data_set: a list of tuples representing data we want to insert into the MS SQL Database
        :param query: the string with the insert query that will be used, default ''
        """

        # Calculate all of our lengths for all the data in our data set and the length of our query
        data_lengths = [len("', '".join([str(x) for x in d])) for d in data_set]
        query_length = len(query)
        statement_length = max(data_lengths) + query_length
        if query_length > 0:
            self.log.info(f"Max data length: {max(data_lengths)}, query_length: {query_length}, statement length: {statement_length}")
        else:
            self.log.info(f"Max data length: {max(data_lengths)}, statement length: {statement_length}")
        # Batch size is calculated at 90% of the number of statements that would fit within
        # our max_sql_server_query_string_size.
        insert_batch_size = int(.9*(self.max_sql_server_query_string_size/statement_length))
        if insert_batch_size == 0:
            # we have a lot of data, just do it one at a time
            insert_batch_size = 1
        self.log.info(f"Estimated max insert batch size is {insert_batch_size}")

        return insert_batch_size

    def execute(self, context):
        """
        Copies all rows from the source table to destination table. This function deletes all data in the
        destination table.
        """
        msg = f"Copying from [{self.src_schema}].[{self.src_table}] to [{self.dest_schema}].[{self.dest_table}]"
        self.log.info(f"{msg}")

        if self.bulk_copy:
            self.log.info("Inserting data using the bulk_copy() method.")
        else:
            self.log.info("Inserting data using executemany() method.")

        def get_sql(schema_name, table_name, num_columns):
            """
            Creates the insert query, this is only necessary when using
            the executemany() method.
            """
            bind_vars_string = ", ".join(['%s' for i in range(0,num_columns)])
            sql = f"""
                insert into [{schema_name}].[{table_name}] values ({bind_vars_string})
            """
            # remove all whitespace
            return sql.strip()

        # Set up our destination connection and cursor
        dest_conn = self.dest_hook.get_conn()
        self.dest_hook.set_autocommit(dest_conn, True)  # commit after every batch is written
        dest_cursor = dest_conn.cursor()
        
        # Set up our source connection and cursor
        src_conn = self.src_hook.get_conn()
        src_cursor = src_conn.cursor()

        # Clear our destination table
        dest_cursor.execute(f"truncate table [{self.dest_schema}].[{self.dest_table}]")

        # Boolean to keep of track of whether we have completed our first insert
        first_insert = True

        # The insert query
        insert_query = None

        # Our batch size
        insert_batch_size = None

        # Execute the copy
        # Get our data, set up our insert query, and then copy all the rows
        # from the source table to the destination table
        select_query = f"select * from [{self.src_schema}].[{self.src_table}]"
        src_cursor.execute(select_query)
        data_set = src_cursor.fetchmany(self.batch_size)
        rows_copied = 0

        while data_set:
            num_results = len(data_set)
            rows_copied += num_results
            self.log.info(f"Fetched {num_results} from [{self.src_schema}].[{self.src_table}]")
            if first_insert:
                # This is our first insert, we need to do some setup before we can proceed.

                if self.bulk_copy:
                    # For the bulk_copy() method we don't need a query, estimate the batch
                    # size directly based on the size of the data retrieved.
                    insert_batch_size = self.estimate_max_batch_size(data_set = data_set)  
                else:
                    # Since this is not a bulk_insert, we need to create a insert query to match 
                    # the row that we just retrieved.

                    # See how many columns we got from our '*' select
                    num_columns = len(data_set[0])
                    # Build and log the insert query
                    insert_query = get_sql(self.dest_schema, self.dest_table, num_columns)
                    self.log.info(insert_query)
                    # now that we have our insert query, we can estimate our maximum insert batch size
                    insert_batch_size = self.estimate_max_batch_size(data_set = data_set, query = insert_query)

                # Log our first row of data
                self.log.info(data_set[0])

                first_insert = False

            if self.bulk_copy:
                # Do the insert using the bulk_copy() method
                dest_conn.bulk_copy(f"[{self.dest_schema}].[{self.dest_table}]", data_set)
            else:
                # do the insert using the executemany() method
                dest_cursor.executemany(insert_query, data_set, batch_size=insert_batch_size)

            # fetch the next batch
            data_set = src_cursor.fetchmany(self.batch_size)

        if not self.bulk_copy:
            # There is an 'executemany' method for pymssql, but it will silently discard
            # inserts under some circumstances.
            # See bug: https://github.com/pymssql/pymssql/issues/952
            #
            # Sleeping for 10 seconds before committing allows more time for the database
            # to write any in-flight inserts so that they are not silently discarded.
            time.sleep(10)
        
        dest_conn.commit()

        # Verify that our copy was successful. The number of rows in the destination table
        # should be equal to the number of rows in the source table.
        src_verification_sql = f"select count(*) from [{self.src_schema}].[{self.src_table}]"
        dest_verification_sql= f"select count(*) from [{self.dest_schema}].[{self.dest_table}]"
        src_cursor.execute(src_verification_sql)
        src_rows = src_cursor.fetchone()
        dest_cursor.execute(dest_verification_sql)
        dest_rows = dest_cursor.fetchone()

        if src_rows[0] != dest_rows[0]:
            # The copy didn't work, fail the task with an error.
            msg = f"Copy Failed: Rows in source table, {src_rows[0]} do not equal rows in destination table, {dest_rows[0]}, copy failed."
            self.log.error(msg)
            raise AirflowFailException(msg)

        # clean up by closing our connections
        dest_conn.close()
        src_conn.close()
        
        msg = f"Copied {rows_copied} rows from [{self.src_schema}].[{self.src_table}] to [{self.dest_schema}].[{self.dest_table}]"
        self.log.info(msg)
        # the return value of '.execute()' will be pushed to XCom by default
        return rows_copied
    
    # define the .post_execute() method that runs after the execute method (optional)
    # result is the return value of the execute method
    def post_execute(self, context, result=None):
        self.log.info("Post-execution complete")