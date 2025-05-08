import os
from azure.cosmos.aio import CosmosClient
from azure.cosmos import exceptions
from dotenv import load_dotenv
import logging
load_dotenv()

COSMOS_URI = os.getenv("COSMOS_DB_URI")
COSMOS_KEY = os.getenv("COSMOS_DB_KEY")
COSMOS_DB_ID = os.getenv("AZURE_DB_ID")
COSMOS_CONTAINER_ID = os.getenv("AZURE_CONTAINER_NAME")

async def get_user_history_from_cosmos(user_id):
    try:
        async with CosmosClient(COSMOS_URI, credential=COSMOS_KEY) as client:
            db = client.get_database_client(COSMOS_DB_ID)
            container = db.get_container_client(COSMOS_CONTAINER_ID)
            query = "SELECT * FROM c WHERE c.client_principal_id = @user_id ORDER BY c._ts DESC"
            params = [{"name": "@user_id", "value": user_id}]
            # items = container.query_items(query=query, parameters=params, enable_cross_partition_query=True)
            items = container.query_items(query="SELECT * FROM c WHERE c.userId = @userId", parameters=[{"name": "@userId", "value": user_id}])
            return [item async for item in items]
    except exceptions.CosmosHttpResponseError as e:
        logging.error(f"Cosmos DB query failed: {e}")
        return []