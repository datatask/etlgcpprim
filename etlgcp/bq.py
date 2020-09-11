import os, sys
import time
import json
from google.cloud import bigquery, logging
from google.cloud.bigquery.schema import SchemaField
from google.cloud.exceptions import Conflict
import uuid
import concurrent.futures


class Etlbq(bigquery.Client):
    """ Class to manage bq etl primitive transformations
    """

    def __init__(self, project_id, logger, labels={},timeout=180):
        super().__init__(project=project_id)
        self.logger = logger
        self.labels = labels
        self.timeout = timeout

    def createwait_table(self, dataset_name, table_name, schema_bq, retry=0):
        """
        Function to create a table in a specific dataset on Google Big Query
        Input :
            dataset_name : the name of the dataset
            table_name : the name of the desired table on GBQ
            schema_bq : the schema of the table (GBQ schema)
        Output :
            True if the table has been created, False otherwise
        """
        try:
            dataset_ref = self.dataset(dataset_name)
            table_ref = dataset_ref.table(table_name)
            # Creation of the table
            bigquery_table = bigquery.Table(table_ref, schema_bq)
            self.create_table(table=bigquery_table)
            tableExist = False
            while not tableExist:
                try:
                    self.get_table(table_ref)
                    tableExist = True
                except:
                    time.sleep(5)
                    tableExist = False
            self.logger.log_text(
                text=f"Table {table_name} created in BQ",
                severity="INFO",
                labels=self.labels,
            )
            return True
        except Conflict:
            self.logger.log_text(
                text=f"Table {table_name} already exists in BQ",
                severity="INFO",
                labels=self.labels,
            )
            return True
        except Exception as e:
            if retry < 3:
                self.logger.log_text(
                    text=f"Error in create table : {dataset_name} {table_name}. Attempt {retry + 1}",
                    severity="WARNING",
                    labels=self.labels,
                )
                time.sleep(5)
                return self.createwait_table(
                    dataset_name, table_name, schema_bq, retry=retry + 1
                )
            else:
                self.logger.log_text(
                    text=f"Error in create table : {dataset_name} {table_name} : {e}. Max number of attempts exceeded",
                    severity="ERROR",
                    labels=self.labels,
                )
                return False

    def build_schema_from_list(self, schema_list, mode_fields="NULLABLE"):
        """
        Function to build a schema in Big Query syntax from a list of tuples
        Input :
            schema_list : a list of tuples (name of the column,type of the column,field mode(optional))
            mode_fields : mode of the field (set to none for individual field mode)
        Output :
            schema_bq : the GBQ schema
        """
        schema_bq = []
        try:
            schema_bq = [
                bigquery.SchemaField(
                    field[0],
                    field[1],
                    mode=field[2] if mode_fields is None else mode_fields,
                )
                for field in schema_list
            ]
            self.logger.log_text(
                text="BQ schema build from list", severity="INFO", labels=self.labels
            )
            return schema_bq
        except Exception as e:
            self.logger.log_text(
                text=f"Error in build schema from list :: error {e}",
                severity="ERROR",
                labels=self.labels,
            )
            return schema_bq

    def build_schema_from_dict(self, schema_dict, mode_fields="NULLABLE"):
        """
        Function to build a schema in Big Query syntaxt from a dictionnary
        Input :
            schema_dict : a dictionnary of the schema (key : name of the column, value : type of the column)
            mode_fields : mode of the field
        Output :
            schema_bq : the GBQ schema
        """
        schema_bq = []
        try:
            for k, v in schema_dict.items():
                if isinstance(v, str) or isinstance(v, unicode):
                    schema_bq.append(bigquery.SchemaField(k, v, mode_fields))
                elif isinstance(v, dict):
                    schema_bq.append(
                        bigquery.SchemaField(
                            k,
                            "RECORD",
                            fields=self.build_schema_from_dict(v, mode_fields),
                        )
                    )
                elif isinstance(v, list):
                    for val in v:
                        if isinstance(val, str) or isinstance(val, unicode):
                            schema_bq.append(
                                bigquery.SchemaField(k, val, "REPEATED", fields=())
                            )
                        else:
                            schema_bq.append(
                                bigquery.SchemaField(
                                    k,
                                    "RECORD",
                                    "REPEATED",
                                    fields=self.build_schema_from_dict(v, mode_fields),
                                )
                            )
                else:
                    self.logger.log_text(
                        text=f"Unusual value used in schema {v} :: type {type(v)}",
                        severity="ERROR",
                        labels=self.labels,
                    )
            self.logger.log_text(
                text="BQ schema build from dict", severity="INFO", labels=self.labels
            )
            return schema_bq
        except Exception as e:
            self.logger.log_text(
                text=f"Error in build BQ schema from dict :: error {e}",
                severity="ERROR",
                labels=self.labels,
            )
            return schema_bq

    def insert_file(
        self,
        file_path,
        dataset_name,
        table_name,
        schema_bq,
        format_input,
        write_disposition="WRITE_EMPTY",
        field_delimiter=",",
        quote_character='"',
        leading_rows=0,
        max_bad_records=0,
        retry=0,
    ):
        """
        Function to load a file stored on GCS into a GBQ table
        Input :
            file_path : local or gs path to the file to load
            dataset_name : name of the dataset
            table_name : name of the table to load on GBQ
            schema_bq : GBQ schema of the data
            write_disposition : the way of inserting data (WRITE_APPEND / WRITE_EMPTY / WRITE_TRUNCATE)
            format_input : format of the input file (CSV / NEWLINE_DELIMITED_JSON / PARQUET)
            field_delimiter : field delimiter
            quote_character : character used to quote
            leading_rows : number of leading rows (usually one for the header)
            max_bad_records : maximum number of errors to consider
        Output : tuple
            job_id : the job id
            errors : the errors during the job execution, None if no error
        """
        try:
            job_id = "%s_%s_loadFile-%s" % (
                dataset_name,
                table_name,
                str(uuid.uuid4()),
            )
            dataset_ref = self.dataset(dataset_name)
            table_ref = dataset_ref.table(table_name)
            job_config = bigquery.LoadJobConfig()
            job_config.write_disposition = write_disposition
            if format_input == "CSV":
                job_config.source_format = "text/csv"
                job_config.field_delimiter = field_delimiter
                job_config.quote_character = quote_character
                job_config.skip_leading_rows = leading_rows
                job_config.schema = schema_bq
            if format_input == "JSON":
                job_config.source_format = "NEWLINE_DELIMITED_JSON"
                job_config.schema = schema_bq
            if format_input == "PARQUET":
                job_config.source_format = "PARQUET"
            job_config.max_bad_records = max_bad_records
            if file_path[:3] == "gs:":
                job = self.load_table_from_uri(
                    file_path, table_ref, job_id=job_id, job_config=job_config
                )
            else:
                with open(file_path, "rb") as source_file:
                    job = self.load_table_from_file(
                        source_file, table_ref, job_id=job_id, job_config=job_config
                    )
            job_id = job.job_id
            try:
                job.result(timeout=self.timeout)
            except concurrent.futures.TimeoutError as e:
                self.logger.log_text(
                    text=f"Insert data to bq JobId {job_id} - Loaded rows, error waiting for complete : {file_path} : {e}",
                    severity="WARNING",
                    labels=self.labels,
                )
            self.logger.log_text(
                text=f"Insert data to bq JobId {job_id} - Loaded rows from {file_path} into {dataset_name}.{table_name}",
                severity="INFO",
                labels=self.labels,
            )
            return (job_id, job.errors)
        except Exception as e:
            if retry < 3:
                self.logger.log_text(
                    text=f"Error in send to BQ : {file_path} : {e}. Retry {retry + 1}",
                    severity="WARNING",
                    labels=self.labels,
                )
                time.sleep(5)
                return self.insert_file(
                    file_path,
                    dataset_name,
                    table_name,
                    schema_bq,
                    format_input,
                    write_disposition,
                    field_delimiter,
                    quote_character,
                    leading_rows,
                    max_bad_records,
                    retry=retry + 1,
                )
            else:
                self.logger.log_text(
                    text=f"Error in send to BQ : {file_path} : {e}. Max number of attempts exceeded",
                    severity="ERROR",
                    labels=self.labels,
                )
                return (None, None)

    def run_queryjob(
        self,
        dataset_name,
        table_name,
        query,
        use_legacy_sql=False,
        write_disposition="WRITE_EMPTY",
        allow_large_results=False,
        retry=0,
    ):
        """
        Function to run a query SQL in Google Big Query
        Input :
            dataset_name : name of the destination dataset
            table_name : name of the destination table to load on GBQ
            query : SQL query to execute
            use_legacy_sql : True if the query is written in legacy SQL, False otherwise
            write_disposition : the way of inserting data (WRITE_APPEND / WRITE_EMPTY / WRITE_TRUNCATE)
            allow_large_results : True if the query results can be large, False otherwise
        Output : tuple
            job_id : the job id
            errors : the errors during the job execution, None if no error
        """
        try:
            job_id = "%s_%s_fromQuery-%s" % (
                dataset_name,
                table_name,
                str(uuid.uuid4()),
            )
            dataset = self.dataset(dataset_name)
            table = dataset.table(table_name)
            job_config = bigquery.QueryJobConfig()
            job_config.destination = table
            job_config.use_legacy_sql = use_legacy_sql
            job_config.write_disposition = write_disposition
            job_config.allow_large_results = allow_large_results
            job = self.query(job_id=job_id, query=query, job_config=job_config)
            try:
                job.result(timeout=self.timeout)
            except concurrent.futures.TimeoutError as e:
                self.logger.log_text(
                    text=f"Run query bq JobId {job_id} - query, error waiting for complete : {query} : {e}",
                    severity="WARNING",
                    labels=self.labels,
                )
            self.logger.log_text(
                text=f"Run query bq JobId {job_id} - query : {query} into {dataset_name}.{table_name}",
                severity="INFO",
                labels=self.labels,
            )
            return (job_id, job.errors)
        except Exception as e:
            if retry < 3:
                self.logger.log_text(
                    text=f"Error in query bq : {query} : {e}. Retry {retry + 1}",
                    severity="WARNING",
                    labels=self.labels,
                )
                time.sleep(5)
                return self.run_queryjob(
                    dataset_name,
                    table_name,
                    query,
                    use_legacy_sql,
                    write_disposition,
                    allow_large_results,
                    retry + 1,
                )
            else:
                self.logger.log_text(
                    text=f"Error in query bq : {query} : {e}. Max number of attempts exceeded",
                    severity="ERROR",
                    labels=self.labels,
                )
                return (None, None)

    def run_extractjob(
        self,
        destination_uris,
        dataset_name,
        table_name,
        compression=None,
        destination_format="CSV",
        field_delimiter=",",
        print_header=True,
        retry=0,
    ):
        """
        Function to extract a table into google storage
        Input :
            destination_uris : gs destination path
            dataset_name : name of the source dataset
            table_name : name of the source table to extract
            compression : use 'GZIP' for compression
            destination_format : 'CSV', 'NEWLINE_DELIMITED_JSON' or 'AVRO'
            field_delimiter : separation caracter for csv format
            print_header : header in csv format
        Output : tuple
            job_id : the job id
            errors : the errors during the job execution, None if no error
        """
        try:
            job_id = "%s_%s_extractTable-%s" % (
                dataset_name,
                table_name,
                str(uuid.uuid4()),
            )
            dataset = self.dataset(dataset_name)
            table = dataset.table(table_name)
            job_config = bigquery.ExtractJobConfig()
            job_config.compression = compression
            job_config.destination_format = destination_format
            job_config.field_delimiter = field_delimiter
            job_config.print_header = print_header
            job = self.extract_table(
                table, destination_uris, job_id=job_id, job_config=job_config
            )
            try:
                job.result(timeout=self.timeout)
            except concurrent.futures.TimeoutError as e:
                self.logger.log_text(
                    text=f"Extract bq table JobId {job_id} - extraction error waiting for complete : {dataset_name}.{table_name} : {e}",
                    severity="WARNING",
                    labels=self.labels,
                )
            self.logger.log_text(
                text=f"Extract bq table JobId {job_id} - extraction {dataset_name}.{table_name} to {destination_uris}",
                severity="INFO",
                labels=self.labels,
            )
            return (job_id, job.errors)
        except Exception as e:
            if retry < 3:
                self.logger.log_text(
                    text=f"Error in extract bq table : {dataset_name}.{table_name} : {e}. Retry {retry + 1}",
                    severity="WARNING",
                    labels=self.labels,
                )
                time.sleep(5)
                return self.run_extractjob(
                    destination_uris,
                    dataset_name,
                    table_name,
                    compression,
                    destination_format,
                    field_delimiter,
                    print_header,
                    retry + 1,
                )
            else:
                self.logger.log_text(
                    text=f"Error in extract bq table : {dataset_name}.{table_name} : {e}. Max number of attempts exceeded",
                    severity="ERROR",
                    labels=self.labels,
                )
                return (None, None)


if __name__ == "__main__":

    ### vars to be define by reading a parameter file or env vars ...
    project_id = os.environ.get("PROJECT_ID", "")
    logger_name = os.environ.get("LOGGERNAME", "etlgcp")

    ### set the GOOGLE_APPLICATION_CREDENTIALS env var for service account authentication
    logging_client = logging.Client(project=project_id)
    logger = logging_client.logger(logger_name)
    etlbq = Etlbq(project_id, logger, {"test": "myvalue"}, timeout=120)

    ### example
    # schema = etlbq.build_schema_from_dict({"a": "STRING", "c": "INTEGER"})
    # etlbq.createwait_table("x_temp", "etlgcptable", schema)
    # etlbq.createwait_table("x_temp", "etlgcptable2", schema)
    # etlbq.labels = {"test2": "speciallabel"}
    # etlbq.run_queryjob(
    #     "x_temp",
    #     "etlgcptable2",
    #     "SELECT * from x_temp.etlgcptable2",
    #     write_disposition="WRITE_TRUNCATE",
    # )
    # etlbq.run_extractjob("gs://x_temp/testlcoextract.csv","x_temp","lcotestdup")
