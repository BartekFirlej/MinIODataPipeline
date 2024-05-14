from minio import Minio
from config.minio_config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

def upload_to_minio(data, object_name):
    client.put_object(
        MINIO_BUCKET,
        object_name,
        data,
        length=-1,
        part_size=10*1024*1024  # 10MB part size
    )
