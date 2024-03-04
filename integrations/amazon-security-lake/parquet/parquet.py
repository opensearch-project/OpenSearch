
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as pafs


class Parquet:

    @staticmethod
    def encode(data: dict):
        return pa.Table.from_pydict(data)

    @staticmethod
    def to_s3(data: pa.Table, s3: pafs.S3FileSystem):
        pass

    @staticmethod
    def to_file(data: pa.Table, path: str):
        # pq.write_to_dataset(table=data, root_path=path)
        pq.write_table(data, path)
