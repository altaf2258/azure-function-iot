import logging
import azure.functions as func
import os
import json
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError


app = func.FunctionApp()

BLOB_CONNECTION_STRING = os.getenv('AzureWebJobsStorage')
CONTAINER_NAME = "iot-data"

blob_service_client = BlobServiceClient.from_connection_string(
    BLOB_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

try:
    container_client.create_container()
except ResourceExistsError:
    pass


@app.function_name(name="iot_hub_trigger")
@app.event_hub_message_trigger(
    arg_name="event",
    event_hub_name="iothub-ehub",
    connection="IOTHUB_CONNECTION"
)
def iot_hub_trigger(event: func.EventHubEvent):

    try:
        message = event.get_body().decode('utf-8')
        logging.info(f"Received message: {message}")

        try:
            data = json.loads(message)
            device_id = data.get("deviceId", "unknown_device")
        except json.JSONDecodeError:
            device_id = "unknown_device"

        now = datetime.utcnow()
        folder_path = f"year={now.year}/month={now.month:02}/day={now.day:02}/hour={now.hour:02}"
        blob_name = f"{folder_path}/iot_{now.strftime('%Y%m%d_%H%M%S%f')}_{device_id}.json"

        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(message, overwrite=True)

        logging.info(f"Saved to blob: {blob_name}")

    except Exception as e:
        logging.error(f"Error: {e}")
