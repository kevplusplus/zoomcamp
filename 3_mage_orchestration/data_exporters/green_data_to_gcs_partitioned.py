import pyarrow as pa
import pyarrow.parquet as pq 
import os 

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/fast-banner-412410-7e59d3224d17.json"

bucket_name = 'mage_bucket_424'
project_id =  'fast-banner-412410'

table_name = "green_taxi"

root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data_to_google_cloud_storage(data, **kwargs) -> None:

    table = pa.Table.from_pandas(data)
    
    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols = ['lpep_pickup_date'],
        filesystem=gcs
    )