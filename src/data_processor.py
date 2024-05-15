import io
import uuid
import json
from src.minio_client import upload_to_minio

def process_and_store_message(message):
    try:
        # Try to parse the message as JSON
        message_dict = json.loads(message)

        # Remove the specified fields if they exist
        message_dict.pop('CountryId', None)
        message_dict.pop('VoivodeshipId', None)
        message_dict.pop('CityId', None)

        # Convert the modified message back to a JSON string
        modified_message = json.dumps(message_dict)
    except json.JSONDecodeError:
        # If message is not JSON, handle it accordingly
        print("Received non-JSON message, skipping:", message)
        return

    # Prepare the data for MinIO
    data = io.BytesIO(modified_message.encode('utf-8'))
    object_name = f'messages/{uuid.uuid4()}.txt'

    # Upload to MinIO
    upload_to_minio(data, object_name)

    print("Added object to MINio")