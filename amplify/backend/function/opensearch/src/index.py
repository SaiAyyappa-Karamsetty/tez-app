import json
import boto3
from decimal import Decimal
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import os

# Initialize AWS services
region = 'us-east-1'  # Change to your region
service = 'es'  # For OpenSearch
credentials = boto3.Session().get_credentials()

# Create AWS4Auth for OpenSearch
awsauth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    region,
    service,
    session_token=credentials.token
)

# Set the OpenSearch endpoint (without https://)
# opensearch_endpoint = 'search-sampledomain-gfj7ad7tpnvohoupl7aibsmusu.us-east-1.es.amazonaws.com'  # Change to your endpoint

opensearch_endpoint = 'search-sampledomainioc-cp7bpm3rwz37zy6xamttwvcs2q.us-east-1.es.amazonaws.com'
# Create an OpenSearch client
client = OpenSearch(
    hosts=[{'host': opensearch_endpoint, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=False,  # Change to True for production
    connection_class=RequestsHttpConnection
)

# Create a DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name=region)

# Specify your DynamoDB table name
dynamodb_table_name =  os.environ['STORAGE_TEZBUILDDATA_NAME']
table = dynamodb.Table(dynamodb_table_name)

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def send_response(statusCode, body):
    return {
        'statusCode': statusCode,
        'headers': {
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': json.dumps(body, default=decimal_default)
    }

def handler(event, context):
    """Fetch data from DynamoDB and index it in OpenSearch."""
    try:
        # Scan the DynamoDB table to retrieve items
        response = table.scan()
        items = response.get('Items', [])
        indexed_count = 0


        for item in items:
            document_id = item.get('ItemType')  # Unique document ID
            if document_id:
                unique_id = f"{document_id}#{item.get('UniqueId')}"  # Modify as needed for uniqueness
                client.index(
                    index='page-cards-index',  # Replace with your OpenSearch index name
                    id=unique_id,
                    body={
                        "Category": item.get('Category'),
                        "Species": item.get('Species'),
                        "Image": item.get('Image'),
                        "Heading": item.get('Heading'),
                        "id": item.get('UniqueId'),
                        "Sku": item.get('SKU'),
                        "Subheading": item.get('Subheading'),
                        "ItemType": item.get('ItemType'),
                        "Length": item.get('Length'),
                        "Profile": item.get('Profile'),
                        "Grade": item.get('Grade'),
                        "FingerJoint": item.get('FingerJoint'),
                        "Precision": item.get('Precision'),
                        "Treatment": item.get('Treatment'),
                        "Brand": item.get('Brand'),
                        "Width": item.get('Width'),
                        "Thickness": item.get('Thickness'),
                        "PanelType": item.get('PanelType'),
                        "Edge": item.get('Edge'),
                        "Finish": item.get('Finish'),
                        "Origin": item.get('Origin'),
                        "Metric": item.get('Metric'),
                        "SearchableField": ((item.get('Category') or '') + " " + (item.get('Heading') or '')).strip(),

                    }
                )
                indexed_count += 1

        return send_response(200, {'message': f'Indexed {indexed_count} items from DynamoDB to OpenSearch.'})

    except Exception as e:
        return send_response(500, {'error': f"Error indexing data: {str(e)}"})
