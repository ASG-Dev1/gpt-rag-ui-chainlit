# import os
# from azure.cosmos.aio import CosmosClient
# from azure.cosmos import exceptions
# from dotenv import load_dotenv
# import logging
# import json

# load_dotenv()

# COSMOS_URI = os.getenv("COSMOS_DB_URI")
# COSMOS_KEY = os.getenv("COSMOS_DB_KEY")
# COSMOS_DB_ID = os.getenv("AZURE_DB_ID")
# COSMOS_CONTAINER_ID = os.getenv("AZURE_CONTAINER_NAME")


# async def get_user_history_from_cosmos(user_id: str):
#     try:
#         async with CosmosClient(COSMOS_URI, credential=COSMOS_KEY) as client:
#             container = client.get_database_client(COSMOS_DB_ID).get_container_client(
#                 COSMOS_CONTAINER_ID
#             )

#             query = """
#               SELECT *
#                 FROM c
#                WHERE c.user_id = @user_id
#             ORDER BY c._ts DESC
#             """
#             params = [{"name": "@user_id", "value": user_id}]

#             # ← no partition_key, so this is a true cross-partition query
#             items = container.query_items(query=query, parameters=params)

#             results = [item async for item in items]
#             # logging.info(f"[cosmos] Fetched {len(results)} convos for {user_id}")
#             # logging.info(
#             #     f"[cosmos] First history item: {json.dumps(results[0], indent=2)}"
#             # )
#             return results

#     except exceptions.CosmosHttpResponseError as e:
#         logging.error(f"Cosmos DB query failed: {e}")
#         return []


# async def get_user_messages(thread_id: str):
#     try:
#         async with CosmosClient(COSMOS_URI, credential=COSMOS_KEY) as client:
#             container = client.get_database_client(COSMOS_DB_ID).get_container_client(
#                 COSMOS_CONTAINER_ID
#             )

#             query = """
#               SELECT VALUE c.history
#                 FROM c
#                WHERE c.id = @thread_id
#             """
#             params = [{"name": "@thread_id", "value": thread_id}]
#             items = container.query_items(query=query, parameters=params)

#             # Get the first result only
#             async for item in items:
#                 return item  # this is a list of {"speaker", "content"} dicts

#             return []

#     except exceptions.CosmosHttpResponseError as e:
#         logging.error(f"[cosmos] Failed to fetch history for {thread_id}: {e}")
#         return []
