from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions, SetupOptions
import hashlib
import apache_beam as beam
import requests
import logging
from apache_beam.io.gcp.gcsio import GcsIO
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from datetime import datetime
import pytz
import os
import pandas as pd

# Configure logging
logging.basicConfig(
    filename='QBP_Bruteforce_Inventory_Update.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

csv_filename = './lookup_table.csv'
    
# Check if the CSV file exists
if not os.path.exists(csv_filename):
    pd.DataFrame(columns=['vtexId', 'vtexName', 'qbpId', 'vtexParentId']).to_csv(csv_filename, index=False)
    
# Read the CSV file
Lookup_DF = pd.read_csv(csv_filename)

# BigQuery variables - if we are using bq
BIGQUERY_PROJECT = 'smart-integration-beam'
BIGQUERY_DATASET = 'smart_integration'
BIGQUERY_TABLE = 'products'

# Specify Cloud Storage locations
GCS_BUCKET = 'smart-integration-beam-bucket'

# Set the time zone to Asia/Colombo
sri_lanka_timezone = pytz.timezone('Asia/Colombo')

start_time = datetime.now(sri_lanka_timezone).strftime("%Y-%m-%d_%H-%M-%S")

def lookup(category_code):
    # Look up the csv and get the corresponding row for the given category code
    matching_rows = Lookup_DF[Lookup_DF['qbpId'] == category_code]

    if not matching_rows.empty and 'ignore' in matching_rows['vtexId'].values:
        return True
    else:
        return False

def is_SKU_available(refID):
    url = 'https://benscycle.vtexcommercestable.com.br/api/catalog_system/pvt/sku/stockkeepingunitidbyrefid/'
        
    VTEX_headers = { 
        'X-VTEX-API-AppKey': 'vtexappkey-benscycle-RRVGRV',
        'X-VTEX-API-AppToken': 'EJOTZIHNLJRNHGIZYSOKGOYRIOVIVYRNIHNTRTFGNLFLVSZCDZPOWJPUQPXQGAXPZHZMFGARITJTYRRBPQTKZUBGHXHCOBZYNTHTBGHSFQUFSEPNLNZQLGUGISAYZARZ',
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    try:
        response = requests.get(f"{url}{refID}", headers=VTEX_headers)
        response.raise_for_status()
        sku = response.json()
        return True, sku if not (sku == '"SKU not found"') else False
    except requests.exceptions.RequestException as e:
        return False, None

def get_bigquery_schema():
    # Define your BigQuery schema here - if we are using
    schema = {
        "fields": [
            {"name": "product_code", "type": "STRING", "mode": "NULLABLE"},
            {"name": "product_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "model_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "unit", "type": "STRING", "mode": "NULLABLE"},
            {"name": "base_price", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "map_price", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "msrp", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "brand_code", "type": "STRING", "mode": "NULLABLE"},
            {"name": "brand_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "discontinued", "type": "BOOLEAN", "mode": "NULLABLE"},
            {"name": "third_party_allowed", "type": "BOOLEAN", "mode": "NULLABLE"},
            {"name": "images", "type": "STRING", "mode": "REPEATED"},
            {"name": "category_codes", "type": "STRING", "mode": "REPEATED"},
            {"name": "weights_and_measures", "type": "RECORD", "mode": "NULLABLE", "fields": [
                {"name": "weight", "type": "STRUCT", "mode": "NULLABLE", "fields": [
                    {"name": "value", "type": "FLOAT", "mode": "NULLABLE"},
                    {"name": "unit", "type": "STRING", "mode": "NULLABLE"}
                ]},
                {"name": "length", "type": "STRUCT", "mode": "NULLABLE", "fields": [
                    {"name": "value", "type": "FLOAT", "mode": "NULLABLE"},
                    {"name": "unit", "type": "STRING", "mode": "NULLABLE"}
                ]},
                {"name": "width", "type": "STRUCT", "mode": "NULLABLE", "fields": [
                    {"name": "value", "type": "FLOAT", "mode": "NULLABLE"},
                    {"name": "unit", "type": "STRING", "mode": "NULLABLE"}
                ]},
                {"name": "height", "type": "STRUCT", "mode": "NULLABLE", "fields": [
                    {"name": "value", "type": "FLOAT", "mode": "NULLABLE"},
                    {"name": "unit", "type": "STRING", "mode": "NULLABLE"}
                ]}
            ]},
            {"name": "product_attributes", "type": "RECORD", "mode": "REPEATED", "fields": [
                {"name": "value", "type": "STRING", "mode": "NULLABLE"},
                {"name": "name", "type": "STRING", "mode": "NULLABLE"},
                {"name": "unit", "type": "STRING", "mode": "NULLABLE"}
            ]},
            {"name": "last_updated", "type": "TIMESTAMP", "mode": "NULLABLE"}
        ]
    }

    return schema

# Define a ParDo function for partitioning
class PartitionDoFn(beam.DoFn):
    def process(self, element):
        code = element

        # Use a hash function to calculate the hash of the code
        hash_value = hashlib.sha1(code.encode()).hexdigest()

        # Convert the hash value to an integer and assign it to a partition
        partition_key = int(hash_value, 16) % 3

        yield (partition_key, element)

# Processing function for each partition
class ProcessCodes(beam.DoFn):
    def process(self, element):
        chunk_size = 100
        total_codes = len(element)

        logging.info(f"New codes found: {total_codes}")

        chunk_count = (len(element) // chunk_size) + 1
        remaining_codes = total_codes % chunk_size        

        if remaining_codes == 0:
            for i in range(chunk_count - 1):
                chunk = element[100*i:100*(i + 1)]
                yield chunk

        if remaining_codes > 0 and chunk_count > 1:
            for i in range(chunk_count - 1):
                chunk = element[100*i:100*(i + 1)]
                yield chunk
            yield element[-remaining_codes:]

        if remaining_codes > 0 and chunk_count == 1:
            yield element[:remaining_codes]

class IngestJson(beam.DoFn):
    def __init__(self):
        self.api_url = 'https://cls.qbp.com/api3/1/product/'
        self.i = 1
    
    def should_skip_processing(self, element):
        # Condition 1: Brand name (in lowercase) contains “jar of” or “display box”
        if 'brand' in element and 'name' in element['brand']:
            brand_name = element['brand']['name'].lower()
            if 'jar of' in brand_name or 'display box' in brand_name:
                return True, "Brand name contains 'jar of' or 'display box'"

        # Condition 2: Discontinued set to true
        if 'discontinued' in element and element['discontinued']:
            return True, "Discontinued set to true"

        # Condition 3: Category code from QBP cross-reference lookup value set to “ignore” or Category code array is empty
        try:
            # Check if 'categoryCode' key is present
            if 'categoryCodes' in element:
                category_code_list = element['categoryCodes']
                
                # Check if 'categoryCode' list is not empty
                if category_code_list:
                    category_code = category_code_list[0]
                    
                    # Condition 3: Category code from QBP cross-reference lookup value set to “ignore”
                    if lookup(category_code):
                        return True, "Category code from QBP cross-reference lookup value set to 'ignore'"
                else:
                    return True, "Category code array is empty"
            else:
                return True, "Category code key not found in the element"
        except KeyError:
            # Handle the case where 'categoryCode' key is not present
            return True, "Category code key not found in the element (KeyError)"
        except IndexError:
            # Handle the case where 'categoryCode' list is empty
            return True, "Category code array is empty (IndexError)"

        # Condition 4: Product name contains “Bulk Box”
        if 'name' in element and 'Bulk Box' in element['name']:
            return True, "Product name contains 'Bulk Box'"

        # If none of the conditions are met, continue processing
        return False, None

    def process(self, element):
        api_headers = {
            'X-QBPAPI-KEY': 'c5bd4c2141447969e2494b7892cf1757',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }

        valid_products = []  # Initialize outside the try block

        just_codes = [i[1] for i in element]

        # Fetch json data from the API
        payload = {'codes': just_codes} 

        try:
            response = requests.post(self.api_url, headers=api_headers, json=payload)
            response.raise_for_status() 

            fetched_products = response.json().get('products', [])

            for product in fetched_products:
                skip, reason = self.should_skip_processing(product)
                if skip and (reason == "Discontinued set to true"):
                    continue
                elif skip and (reason != "Discontinued set to true"):
                    logging.warning(f"Skipping product {product['code']}: {reason}")
                    continue
                else:
                    valid_products.append(product)

            fetch_error = len(just_codes) - len(response.json().get('products', [])) 
            FailedCodes = list(set(just_codes) - set([product['code'] for product in response.json().get('products', [])]))
            logging.info(f"From {len(just_codes)} products from batch-{self.i} and {len(response.json().get('products', []))} products were fetched successfully, {fetch_error} products failed to fetch the codes: {FailedCodes} and the valid product are {len(valid_products)}")

        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching data: {e}")
        finally:
            yield valid_products
            self.i += 1

class CleanData(beam.DoFn):
    def process(self, element):
        cleaned_products = []

        for product in element:
            cleaned_data = {
                "product_code": str(product.get("code", "")),
                "product_name": str(product.get("name", "")),
                "model_name": str(product.get("model", {}).get("name", "")),
                "unit": str(product.get("unit", "")),
                "base_price": float(product.get("basePrice", 0)),
                "map_price": float(product.get("mapPrice", 0)),
                "msrp": float(product.get("msrp", 0)),
                "brand_code": str(product.get("brand", {}).get("code", "")),
                "brand_name": str(product.get("brand", {}).get("name", "")),
                "discontinued": bool(product.get("discontinued", False)),
                "third_party_allowed": bool(product.get("thirdPartyAllowed", False)),
                "images": str(product.get("images", [])),
                "category_codes": str(product.get("categoryCodes", [])),
                "weights_and_measures": str(product.get("weightsAndMeasures", {})),
                "product_attributes": str(product.get("productAttributes", [])),
                "last_updated": datetime.now().isoformat()
            }

            cleaned_products.append(cleaned_data)
        
        logging.info(f"Data cleaning completed for all the data provided")
        yield cleaned_products

import csv

class CollectAndConvertToDataFrame(beam.DoFn):
    def __init__(self, output_file):
        self.output_file = output_file

class GetSKU(beam.DoFn):
    def process(self, element):
        
        VTEX_Products = []

        for product in element:
            is_available , sku = is_SKU_available(product.get('product_code', ''))
            if is_available:
                VTEX_Products.append(product)
        
        if (len(element) - len(VTEX_Products)) != 0:
            logging.warning(f"out of {len(element)} products SKU is not available for {len(element) - len(VTEX_Products)} products")
        else:
            logging.info(f"SKU is available for all products given: {len(element)}")


        yield VTEX_Products
    
class getInven(beam.DoFn):
    def process(self, element):
        Inventory_data = []

        url = 'https://cls.qbp.com/api3/1/inventory'

        api_headers = {
        'X-QBPAPI-KEY': 'c5bd4c2141447969e2494b7892cf1757',
        'Accept': 'application/json',
        'Content-Type': 'application/json'
        }

        
        codes = [product.get('product_code') for product in element]

        Inventory_payload = {
        "productCodes": codes,
        "warehouseCodes": ["1000", "1100", "1200", "1300"]
        }

        # Process each element in the batch
        response = requests.post(url, headers=api_headers, json=Inventory_payload)

        if response.status_code == 200:
            inventory_data = response.json().get('inventories', [])
            for data in inventory_data:
                Inventory_data.append(data)

        else:
            logging.warning(f'Failed to fetch inventory data: {response.status_code}')
            Inventory_data = []

        logging.info(f'Inventory data found for {len(codes)} products: {len(Inventory_data)}')

        yield Inventory_data
    
import concurrent.futures

class VTEX_Ingest(beam.DoFn):
    def put_vtex_concurrently(self, data):
        temp = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(self.put_vtex, product) for product in data]
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        temp.append(result)
                except Exception as e:
                    logging.error(f"An error occurred during VTEX Ingestion: {str(e)}", exc_info=True)
        return temp

    def process(self, element):

        # Make concurrent API calls to VTEX
        temp = self.put_vtex_concurrently(element)

        if len(temp) == len(element):
            logging.info(f"VTEX Ingestion completed for {len(temp)} products")
        else:
            logging.warning(f"VTEX Ingestion failed for {len(element) - len(temp)} products")

    def put_vtex(self, data):
        try:
            flag, product_sku = is_SKU_available(data.get('product'))
            if flag:
                # Cross-reference the warehouse with the VTEX warehouse
                reference_dict = {
                    1000: "QBP_MN",
                    1100: "QBP_CO",
                    1200: "QBP_PA",
                    1300: "QBP_NV"
                }

                VTEX_Warehouse = reference_dict.get(int(data.get('warehouse')))

                VTEX_url = f"https://benscycle.vtexcommercestable.com.br/api/logistics/pvt/inventory/skus/{product_sku}/warehouses/{VTEX_Warehouse}"

                VTEX_headers = {
                    'X-VTEX-API-AppKey': 'vtexappkey-benscycle-RRVGRV',
                    'X-VTEX-API-AppToken': 'EJOTZIHNLJRNHGIZYSOKGOYRIOVIVYRNIHNTRTFGNLFLVSZCDZPOWJPUQPXQGAXPZHZMFGARITJTYRRBPQTKZUBGHXHCOBZYNTHTBGHSFQUFSEPNLNZQLGUGISAYZARZ',
                    'Content-Type': 'application/json'
                }

                payload = {
                    "unlimitedQuantity": False,
                    "quantity": int(data.get('quantity')),
                }

                response = requests.put(VTEX_url, headers=VTEX_headers, json=payload)

                if response.status_code == 200:
                    return data
                else:
                    logging.warning(f"Failed to update VTEX for product {data.get('product_code')}: {response.status_code}")
        except Exception as e:
            # Log the full exception traceback
            logging.error(f"An error occurred while ingesting into VTEX: {str(e)}", exc_info=True)
        return None

def log_total_processed_data(element):
    # Calculate the total amount of data processed in all branches
    total_processed_data = sum(len(branch) for branch in element)

    # Log the total amount of data processed
    logging.info(f"Total amount of data processed in all branches: {total_processed_data}")

    # Return the original element to pass it through
    return element

def run(project):
    # Specify Cloud Storage locations
    GCS_BUCKET = 'smart-integration-beam-bucket'

    options = PipelineOptions()
    options.view_as(SetupOptions).save_main_session = True
    options.view_as(StandardOptions).runner = 'DataflowRunner'
    options.view_as(GoogleCloudOptions).project = project
    options.view_as(GoogleCloudOptions).job_name = 'qbp-brute-force-inventory-update'  
    options.view_as(GoogleCloudOptions).temp_location = f'gs://{GCS_BUCKET}/tmp/'
    options.view_as(GoogleCloudOptions).staging_location = f'gs://{GCS_BUCKET}/staging/'
    options.view_as(GoogleCloudOptions).region = 'us-west1'
    options.view_as(StandardOptions).streaming

    # Create a pipeline
    with beam.Pipeline(options=options) as pipeline:
        api_list_url = 'https://cls.qbp.com/api3/1/productcode/list?includeDiscontinued=false'

        headers = {
            'X-QBPAPI-KEY': 'c5bd4c2141447969e2494b7892cf1757',
            'Accept': 'application/json'
        }

        # Fetch latest codes from the API
        response = requests.get(api_list_url, headers=headers)
        if response.status_code == 200:
            latest_codes = response.json().get('codes', [])

            # Find new codes since the last run
            current_codes = [code for code in latest_codes]

        # Input data
        input_data = pipeline | "CreateInput" >> beam.Create(current_codes)

        # Branch the pipeline into four partitions
        partitions = (
            input_data
            | "PartitionData" >> beam.ParDo(PartitionDoFn())
            | "WithKeys" >> beam.Map(lambda x: (x[0], x[1]))
        )

        # Define and process four branches in parallel
        processed_branch_0 = (
            partitions
            | "FilterBranch0" >> beam.Filter(lambda x: x[0] == 0)
            | "CollectElements0" >> beam.combiners.ToList()
            | "Process Codes0" >> beam.ParDo(ProcessCodes())
            | "JSON Extract0" >> beam.ParDo(IngestJson())
            | "Clean Data0" >> beam.ParDo(CleanData())
            | "Get SKU0" >> beam.ParDo(GetSKU())
            | "Get Inventory0" >> beam.ParDo(getInven())
            | "Put VTEX0" >> beam.ParDo(VTEX_Ingest())
        )
        processed_branch_1 = (
            partitions
            | "FilterBranch1" >> beam.Filter(lambda x: x[0] == 1)
            | "CollectElements1" >> beam.combiners.ToList()
            | "Process Codes1" >> beam.ParDo(ProcessCodes())
            | "JSON Extract1" >> beam.ParDo(IngestJson())
            | "Clean Data1" >> beam.ParDo(CleanData())
            | "Get SKU1" >> beam.ParDo(GetSKU())
            | "Get Inventory1" >> beam.ParDo(getInven())
            | "Put VTEX1" >> beam.ParDo(VTEX_Ingest())
        )
        processed_branch_2 = (
            partitions
            | "FilterBranch2" >> beam.Filter(lambda x: x[0] == 2)
            | "CollectElements2" >> beam.combiners.ToList()
            | "Process Codes2" >> beam.ParDo(ProcessCodes())
            | "JSON Extract2" >> beam.ParDo(IngestJson())
            | "Clean Data2" >> beam.ParDo(CleanData())
            | "Get SKU2" >> beam.ParDo(GetSKU())
            | "Get Inventory2" >> beam.ParDo(getInven())
            | "Put VTEX2" >> beam.ParDo(VTEX_Ingest())
        )


        # Merge the results from all branches into a single PCollection
        final_result = (
            # processed_branch_0, processed_branch_1, processed_branch_2
            processed_branch_0, processed_branch_1, processed_branch_2
        ) | "MergeResults" >> beam.Flatten()

        # Log the total amount of data processed before further processing or writing
        final_result | "LogTotalProcessedData" >> beam.Map(log_total_processed_data)
    
    return pipeline

