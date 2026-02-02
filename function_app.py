import azure.durable_functions as df
import azure.functions as func
from settings import (
    PRODUCT_SETTINGS, 
    PERF_REPORT_TABLE, 
    FINAL_INSP_REPORT_TABLE, 
    TABLE_STORAGE_URL, 
    OPS1_API_Q_NAME, 
    OPS1_API_Q_URL, 
    RECHECK_OPS1_Q_TIME, 
    PROD_TYPE_KEY, 
    TEST_TYPE_KEY, 
)
from datetime import datetime, timedelta
import json
import logging
from typing import Any
from utils import o365, ops1, storage
from utils.types import TestDataMessage, Ops1ResponseData, Ops1SaveData
from azure.data.tables import TableServiceClient
import os

# ops1_queue_client = storage.connect_to_storage_queue(OPS1_API_Q_URL, OPS1_API_Q_NAME)
# logging.info('Connected to Operations1 API queue.')
# perf_report_table_client = storage.connect_to_table_storage(TABLE_STORAGE_URL, PERF_REPORT_TABLE)
# logging.info('Connected to performance report table.')
# lc_table_client = storage.connect_to_table_storage(TABLE_STORAGE_URL, FINAL_INSP_REPORT_TABLE)
# logging.info('Connected to final inspection report table.')

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# @app.event_hub_message_trigger(arg_name="azmsg", 
#                                event_hub_name="messages/events", # Default for IoT Hub
#                                connection="IOTHUB_CONNECTION") 
# def iot_hub_receiver(azmsg: func.EventHubMessage):
#     try:
#         # Decode the incoming message body
#         body = azmsg.get_body().decode('utf-8')
#         logging.info(f'IoT Hub Message Received: {body}')
        
#         data = json.loads(body)
#         print(data)
        
#     except Exception as e:
#         logging.error(f"Error processing IoT Hub message: {e}")

@app.function_name(name="iot_hub_trigger")
@app.event_hub_message_trigger(
    arg_name="event",
    event_hub_name="iothub-ehub",
    connection="IOTHUB_CONNECTION"
)
@app.blob_output(
    arg_name="outputBlob",
    path="falcondata01/{rand-guid}.json",
    connection="AzureWebJobsStorage",
)
def iot_hub_trigger(event: func.EventHubEvent, outputBlob: func.Out[str]):

    msg = event.get_body().decode()
    # logging.info(f"IoT Hub message received: {msg}")
    
    full_event = {
        "body": event.get_body().decode(),

        # Event Hub metadata
        "enqueued_time": event.enqueued_time.isoformat(),

        # Application + system properties
        # "properties": event.properties,
    }
    logging.info(f"IoT Hub message received Next: {full_event}")
    # Save to Blob
    outputBlob.set(full_event)

# @app.function_name(name="iot_hub_trigger")
# @app.event_hub_message_trigger(
#     arg_name="event",
#     event_hub_name="iothub-ehub",
#     connection="IOTHUB_CONNECTION"
# )
# @app.blob_output(
#     arg_name="outputBlob",
#     path="falcondata01/{rand-guid}.json",
#     connection="AzureWebJobsStorage",
# )
# def iot_hub_trigger(event: func.EventHubEvent, outputBlob: func.Out[str]):

#     # ---------------- EXISTING CODE ----------------
#     msg = event.get_body().decode()

#     full_event = {
#         "body": msg,
#         "enqueued_time": event.enqueued_time.isoformat(),
#         # "properties": event.properties,
#     }

#     logging.info(f"IoT Hub message received Next: {full_event}")

#     # Save to Blob (UNCHANGED)
#     outputBlob.set(json.dumps(full_event))

#     try:
#         payload = json.loads(msg)

#         serial_number = payload.get("SN", "").strip()
#         data_values = payload.get("data", {})

#         table_service = TableServiceClient.from_connection_string("Endpoint=sb://iothub-ns-oprations0-69893441-8ac468442d.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=E0QnfQ5qR67tQ4OnkacaofEbhARaItTISAIoTPfKSEE=;EntityPath=oprations01")

#         table_client = table_service.get_table_client(
#             table_name="falcontestdata"
#         )

#         entity = {
#             "PartitionKey": serial_number if serial_number else "UNKNOWN",
#             "RowKey": str(int(datetime.datetime.utcnow().timestamp() * 1000)),

#             "AV1": data_values.get("AV1"),
#             "RV1": data_values.get("RV1"),

#             "enqueuedTime": payload.get("enqueuedTime"),
#             "version": payload.get("properties", {}).get("V"),

#             "createdAt": datetime.datetime.utcnow().isoformat()
#         }
#         print("Here!!!!!!!")
#         print(entity)
#         table_client.create_entity(entity)

#         logging.info("PLC data written to Table Storage")

#     except Exception as e:
#         logging.error(f"Table insert failed: {str(e)}")



# Webhook: SN entry in test report -> Cache report id and serial number for auto test data update
# @app.route(route='cache_open_report', methods=['POST', 'OPTIONS'])
# def cache_open_report(req: func.HttpRequest) -> func.HttpResponse:

#     if req.method == 'OPTIONS':
#         return func.HttpResponse(status_code=200, body=f'Connected suffcessfully')
    
#     # Check for connection to performance report table
#     if perf_report_table_client == None:
#         message = 'Could not connect to performance report table.'
#         return func.HttpResponse(status_code=500, body=message)
    
#     # Get product from request and check for support
#     req_json = req.get_json()
#     interaction_data = req_json['data']['progress']['interactionData']
#     product: str = interaction_data[0].get(PROD_TYPE_KEY, None) if interaction_data else ''

#     if not PRODUCT_SETTINGS.get(product, {}).get('performance', False):
#         logging.info(f'Product "{product}" not supported')
#         return func.HttpResponse(status_code=200, body=f'Request received. Product "{product}" not supported')
    
#     # Get report id and serial number (uppercased), then cache. If already cached, update serial number.
#     report_id = str(req_json['data']['progress']['id'])
#     serial_number: str = req_json['data']['interactionValue']['value']
#     serial_number = serial_number.upper()

#     # Find serial number used on old active (likely failed) reports before retest and deactivate instances.
#     sn_query_filter = f"PartitionKey eq '{product}' and SerialNumber eq '{serial_number}' and Status eq 'active'"
#     existing_serial_entities = [entity for entity in perf_report_table_client.query_entities(sn_query_filter)]
#     if len(existing_serial_entities) > 0:
#         logging.info(f'Serial number used in previous report(s). Purging uses of {serial_number}: {existing_serial_entities}')
#         for existing_entity in existing_serial_entities:
#             existing_entity.items()
#             del_entity = {
#                 'PartitionKey': product, 
#                 'RowKey': report_id, 
#             }
#             perf_report_table_client.delete_entity(entity=del_entity)

#     # Find if report id already used and only needs editing.
#     try:
#         existing_entity = perf_report_table_client.get_entity(partition_key=product, row_key=report_id)
#         logging.error(f'Report {report_id} already exists in cache. Updating existing report with new serial number: {serial_number}')
#         perf_report_table_client.update_entity(
#             entity=existing_entity | {
#                 'SerialNumber': serial_number, 
#                 'UpdatedAt': datetime.now()
#             }
#         )
#         return func.HttpResponse(status_code=200, body=f'Successfully updated existing serial number entry: {serial_number}')
#     except Exception as e:
#         logging.info(f'Report {report_id} not cached yet: {e}')

#     # Create new cache entity if report id not found.
#     entity = {
#         'PartitionKey': product,
#         'RowKey': report_id,
#         'CreatedAt': datetime.now(), 
#         'UpdatedAt': datetime.now(), 
#         'SerialNumber': serial_number,
#         'Status': 'open', 
#         'Passed': False, 
#     }
#     logging.info(f'Caching {entity} in {PERF_REPORT_TABLE}')
#     response = perf_report_table_client.create_entity(entity)
#     logging.info(response)

#     return func.HttpResponse(status_code=200, body=f'Successfully cached open report with new serial number {serial_number}')

# def check_data_validity(report_data, expected_test) -> bool:
#     test_type = report_data['test_type']
#     report_id = str(report_data['report_id'])
#     product = report_data['product']
#     serial_number: str = report_data['serial_number']
#     passed = report_data['passed']

#     # Test type not related to calling function
#     if test_type != expected_test:
#         message = f'Report {report_id} is not the correct type'
#         logging.info(message)
#         return False
    
#     # Product not supported for test type
#     elif not PRODUCT_SETTINGS.get(product, {}).get(expected_test, {}).get('supported', False):
#         message = f'Product "{product}" not supported. Serial number: {serial_number}'
#         logging.info(message)
#         return False

#     # No serial number
#     elif serial_number.rstrip() == '':
#         message = f'No serial number found in report {report_id}'
#         logging.info(message)
#         return False

#     # Test failed
#     elif passed == False:
#         message = f'Product failed test (SN: {serial_number})'
#         logging.info(message)
#         return False
    
#     return True

# Webhook: Report completion -> Mark report as "pass" if passed for later check before lean completion
# @app.route(route='complete_performance_report', methods=['POST', 'OPTIONS'])
# def complete_performance_report(req: func.HttpRequest) -> func.HttpResponse:

#     if req.method == 'OPTIONS':
#         return func.HttpResponse(status_code=200, body=f'Connected successfully')
    
#     # Check for connection to cloud resources
#     if perf_report_table_client == None:
#         message = 'Could not connect to performance report table.'
#         return func.HttpResponse(status_code=500, body=message)

#     # Extract report data and validate/check for support
#     try:
#         req_json = req.get_json()
#     except Exception as e:
#         message = f'Error converting payload body to JSON | Error: {e} | Body: {req.get_body()}'
#         logging.error(message)
#         return func.HttpResponse(status_code=400, body=message)
    
#     raw_report_data = req_json['data']
#     report_data = extract_report_data(raw_report_data)
#     data_is_valid = check_data_validity(report_data, expected_test='performance')

#     if not data_is_valid:
#         message = f'Data provided disallows performance report completion.'
#         return func.HttpResponse(status_code=400, body=message)
    
#     # Define update to existing entity
#     entity_update = {
#         'UpdatedAt': datetime.now(), 
#         'IsActive': True, 
#         'Passed': report_data['passed'], 
#     }
    
#     # Check if report entity is saved and active, then update or create new
#     try:
#         existing_entity = perf_report_table_client.get_entity(
#             partition_key=report_data['product'], 
#             row_key=report_data['report_id']
#         )
#     except Exception:
#         new_entity = entity_update | {
#             'PartitionKey': report_data['product'], 
#             'RowKey': report_data['report_id'], 
#             'SerialNumber': report_data['serial_number'], 
#         }
#         message = f'Could not find report {report_data['id']} in table {PERF_REPORT_TABLE}. \
#             Saving new entity: {new_entity}'
#         logging.info(message)
#         perf_report_table_client.create_entity(new_entity)
#         return func.HttpResponse(status_code=201, body=message)
    
#     updated_entity = existing_entity | entity_update
#     message = f'Updating pass/fail status of entity: {updated_entity}'
#     logging.info(message)
#     perf_report_table_client.update_entity(updated_entity)
#     return func.HttpResponse(status_code=200, body=message)

# @app.durable_client_input(client_name='client')
# @app.route(route='durable/{functionName}', methods=['POST', 'OPTIONS'])
# async def create_orchestrator(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:

#     if req.method == 'OPTIONS':
#         return func.HttpResponse(status_code=200, body=f'Connected successfully')
    
#     function_name = req.route_params.get('functionName', '')
#     try:
#         request_data = req.get_json()
#         logging.info(f'Request to {function_name}: {json.dumps(request_data)}')
#     except Exception as e:
#         logging.info(f'Invalid or no JSON payload: {req.get_body()}, {e}')
#         return func.HttpResponse(status_code=400, body='Invalid or no JSON payload.')
    
#     instance_id = await client.start_new(function_name, client_input=request_data)
#     response = client.create_check_status_response(req, instance_id)
#     return response

# @app.orchestration_trigger(context_name='context')
# def save_test_vals(context: df.DurableOrchestrationContext):

#     # Get data from orchestration context and check validity
#     test_data = context.get_input()

#     yield context.call_activity('register_in_queue', context.instance_id)
#     yield from wait_my_turn(context)

#     yield context.call_activity('activity_save_test_vals', json.dumps(test_data))

#     # Clear self from queue
#     yield context.call_activity('remove_queued', context.instance_id)

# @app.activity_trigger(input_name='messageString')
# def activity_save_test_vals(messageString: str) -> None:

#     message: TestDataMessage = json.loads(messageString)
#     if not is_valid_message(message):
#         return None

#     product = message['topic'].split('/')[1]
#     serial_number = message['SN'].rstrip()

#     # Find report id in cache using serial number
#     cache_data = {
#         'product': product,
#         'serial_number': serial_number, 
#     }
#     report_id = find_cached_report(cache_data)

#     # If report not found, generate report and enter serial number
#     if report_id == 0:

#         get_doc_id_result = get_document_id(product)
#         if get_doc_id_result['status'] == 'error':
#             return
        
#         doc_id = get_doc_id_result['data']
#         report_id_result = create_report(doc_id)
#         if report_id_result['status'] == 'error':
#             return
        
#         report_id: int = report_id_result['data']
#         serial_number_save_data: Ops1SaveData = {
#             'product': product,
#             'report_id': report_id,
#             'vpid': PRODUCT_SETTINGS[product]['performance']['vpids']['SN'],
#             'value': serial_number,
#             'step': 0
#         }
        
#         # update_report(serial_number_save_data)
        
#         # Cache new report info for future data packets
#         new_entity = {
#             'PartitionKey': product,
#             'RowKey': str(report_id),
#             'SerialNumber': serial_number, 
#             'IsActive': True, 
#             'Passed': False,
#         }

#         logging.info(f'Caching {new_entity} in {PERF_REPORT_TABLE}')
#         cache_report(new_entity)

#     # Save test values in matched/new test report
#     data = message['data']
#     for measure in data:
#         try:
#             vpid: int = PRODUCT_SETTINGS[product]['performance']['vpids'][measure]
#             if vpid == 0:
#                 logging.info(f'No version persistent id provided for measure "{measure}"')
#                 continue
#             save_data: Ops1SaveData = {
#                 'product': product, 
#                 'report_id': report_id,
#                 'vpid': vpid,
#                 'value': data[measure], 
#                 'step': 2
#             }
#             # update_report(save_data)
#         except Exception as e:
#             logging.error(f'Error updating report {report_id} for {measure}: {e}')
#             continue
        
#     logging.info(f'Successfully saved test values for {serial_number} on report {report_id}')

# @app.route(route='lean_complete', methods=['POST', 'OPTIONS'])
# def lean_complete(req: func.HttpRequest) -> func.HttpResponse:

#     if req.method == 'OPTIONS':
#         message = 'Connected successfully to lean completion webhook destination.'
#         logging.info(message)
#         return func.HttpResponse(status_code=200, body=message)

#     try:
#         req_json = req.get_json()
#     except Exception as e:
#         message = f'Erorr parsing request json: {req.get_body()}, Exception: {e}'
#         return func.HttpResponse(status_code=400, body=message)

#     # Get report data and verify product support
#     raw_report_data: dict[Any, Any] = req_json['data']
#     report_data = extract_report_data(raw_report_data)
#     data_is_valid = check_data_validity(report_data, expected_test='final_insp')

#     if not data_is_valid:
#         message = f'Data provided disallows lean completion.'
#         return func.HttpResponse(status_code=400, body=message)

#     report_id = report_data['report_id']
#     product = report_data['product']
#     serial_number = report_data['serial_number']
#     passed = report_data['passed']
    
#     # Check O365 connection before saving SN in Azure Table Storage
#     # o365_context = o365.connect_to_o365()
#     # if o365_context == None:
#     #     message = 'Failed to connect to O365.'
#     #     logging.error(message)
#     #     return func.HttpResponse(status_code=500, body=message)

#     # logging.info('Successfully connected to O365.')

#     # Connect to Azure Table Storage & check for repeat sn
#     if lc_table_client == None:
#         message = 'Failed to connect to azure storage resource.'
#         logging.error(message)
#         return func.HttpResponse(status_code=500, body=message)
#     if storage.find_repeat_sn(lc_table_client, product, serial_number):
#         message = f'Repeat serial_number found in lean completion history: {product}, {serial_number}'
#         logging.error(message)
#         return func.HttpResponse(status_code=400, body=message)

#     # Save serial number in table storage with relevant data
#     new_entity = {
#         'PartitionKey': product, 
#         'RowKey': report_id,
#         'CreatedAt': datetime.now(), 
#         'SerialNumber': serial_number, 
#         'Passed': passed, 
#         'Prefix': PRODUCT_SETTINGS[product]['prefix'], 
#         'PartNumber': PRODUCT_SETTINGS[product]['pn'], 
#     }
#     lc_table_client.create_entity(entity=new_entity)
#     logging.info(f'{serial_number} saved successfully.')

#     # Drop lean completion file in dedicated Sharepoint location using O365 connection
#     # filename = 'test_file.txt'
#     # file_content = 'pipe|separated|values'
#     # file = o365.save_lc_info(o365_context, filename, file_content)
#     # if not file:
#     #     message = f'Failed to create drop file {filename}.'
#     #     logging.error(message)

#     message = f'Drop file created successfully.'
#     return func.HttpResponse(status_code=200, body=message)

def wait_my_turn(context: df.DurableOrchestrationContext):

    queue = yield context.call_activity('get_queue_msgs', '2')
    next_in_line = queue[0]

    tries = 1
    while next_in_line != context.instance_id:
        execute_datetime = context.current_utc_datetime + timedelta(seconds=RECHECK_OPS1_Q_TIME)
        tries += 1

        yield context.create_timer(execute_datetime)

        queue = yield context.call_activity('get_queue_msgs', '2')
        next_in_line = queue[0]

    return True

def is_valid_message(message: TestDataMessage) -> bool:
    if 'SN' not in message:
        logging.error('Key "SN" not found in data. Ending process.')
        return False
    elif 'topic' not in message:
        logging.error('Key "topic" not found in data. Ending process.')
        return False
    elif 'data' not in message:
        logging.error('Key "data" not found in data. Ending process.')
        return False

    product = message['topic'].split('/')[1]

    if not PRODUCT_SETTINGS.get(product, {}).get('performance', False):
        logging.error(f'Product {product} not supported for data transfer to Operations1')
        return False
    
    return True

def get_document_id(product: str) -> Ops1ResponseData:
    logging.info(f'Getting latest document id for product: {product}')
    result = ops1.get_document_id(product)
    return result

def create_report(doc_id: str) -> Ops1ResponseData:
    logging.info(f'Creating Operations1 report for doc_id: {doc_id}')
    result = ops1.create_report(doc_id)
    return result

def update_report(report_data: Ops1SaveData) -> Ops1ResponseData:
    logging.info(f'Updating report with test value: {report_data}')
    result = ops1.update_report(report_data)
    return result

def extract_report_data(data: dict[Any, Any]) -> dict[str, Any]:
    extracted_data = {}
    extracted_data['report_id'] = data['id']
    interaction_data = data['interactionData']
    extracted_data['test_type'] = interaction_data[0].get(TEST_TYPE_KEY, '') if interaction_data else ''
    extracted_data['product'] = interaction_data[0].get(PROD_TYPE_KEY, '') if interaction_data else ''
    extracted_data['serial_number'] = ''
    extracted_data['passed'] = True
    for interaction in interaction_data:
        if 'serial_number' in interaction.get('interactionTagNames', ''):
            extracted_data['serial_number'] = interaction['interactionValue']
        if interaction['progressPassed'] != 'pass':
            extracted_data['passed'] = False
    
    return extracted_data

# @app.activity_trigger(input_name='funcInstanceId')
# def register_in_queue(funcInstanceId: str ) -> None:
#     logging.info(f'Adding {funcInstanceId} to the queue')
#     ops1_queue_client.send_message(funcInstanceId)
#     return None

# @app.activity_trigger(input_name='maxCount')
# def get_queue_msgs(maxCount: str) -> list[str]:
#     logging.info('Getting current queue messages')
#     current_queue = ops1_queue_client.peek_messages(max_messages=int(maxCount))
#     queue_messages = [item.content for item in current_queue]
#     if len(queue_messages) > 0:
#         logging.info(f'Next in line: {queue_messages}')
#     return queue_messages

# @app.activity_trigger(input_name='instanceId')
# def remove_queued(instanceId: str) -> None:
#     logging.info(f'Removing {instanceId} from queue.')
#     message = ops1_queue_client.receive_message()
#     ops1_queue_client.delete_message(message)
#     return None

def cache_report(cache_post_data: dict[str, Any]) -> None:
    partition_key = cache_post_data['PartitionKey']
    row_key = cache_post_data['RowKey']
    try:
        perf_report_table_client.get_entity(partition_key=partition_key, row_key=row_key)
        logging.error(f'Report {row_key} already exists in cache.')
        return None
    except Exception as e:
        pass
    logging.info(f'Caching {cache_post_data} in {PERF_REPORT_TABLE}')
    perf_report_table_client.create_entity(cache_post_data)
    return None

def find_cached_report(data: dict[str, str]) -> int:
    product = data['product']
    serial_number = data['serial_number']
    query_filter = f"PartitionKey eq '{product}' and SerialNumber eq '{serial_number}' and Status eq 'actice'"
    try:
        found_entities_iterator = perf_report_table_client.query_entities(query_filter)
        report_id = [entity for entity in found_entities_iterator][0]['RowKey']
        logging.info(f'Serial number {serial_number} found for report {report_id}')
        return report_id
    except Exception as e:
        logging.info(f'No cached report found for serial number {serial_number}')
        return 0

# @app.activity_trigger(input_name='product')
# def get_cached_sns(product: str) -> str:
#     logging.info(f'Getting cached SNs for {product}')
#     query_filter = f"PartitionKey eq '{product}'"
#     sn_entity_iterator = perf_report_table_client.query_entities(query_filter)
    
#     sn_report_map = {}
#     for entity in sn_entity_iterator:
#         sn_report_map |= {
#             entity['RowKey']: entity.get('SerialNumber', '')
#         }
    
#     sn_report_map_string = json.dumps(sn_report_map)
#     logging.info(f'Currently cached data: {sn_report_map_string}')
#     return sn_report_map_string

# @app.activity_trigger(input_name='entityString')
# def remove_from_cache(entityString: str) -> None:
#     entity = json.loads(entityString)
#     product = entity['PartitionKey']
#     report_id = entity['RowKey']

#     logging.info(f'Deleting cached entity: {entity}')
#     try:
#         perf_report_table_client.delete_entity(partition_key=product, row_key=report_id)
#     except Exception as e:
#         logging.error(f'Error removing entity {entity} from cache: {e}')
#     return None

