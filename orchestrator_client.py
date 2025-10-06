import os
import json
import httpx
import logging
from azure.identity import (
    ManagedIdentityCredential,
    AzureCliCredential,
    ChainedTokenCredential,
)
import requests


# Obtain the token using Managed Identity
def get_managed_identity_token():
    credential = ChainedTokenCredential(
        ManagedIdentityCredential(), AzureCliCredential()
    )
    token = credential.get_token("https://management.azure.com/.default").token
    return token


def get_function_key():
    subscription_id = os.getenv("AZURE_SUBSCRIPTION_ID")
    resource_group = os.getenv("AZURE_RESOURCE_GROUP_NAME")
    function_app_name = os.getenv("AZURE_ORCHESTRATOR_FUNC_NAME")
    token = get_managed_identity_token()
    logging.info("[orchestrator_client] Obtaining function key.")

    # URL to get all function keys, including the default one
    requestUrl = f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Web/sites/{function_app_name}/functions/orchestrator_streaming/listKeys?api-version=2022-03-01"

    requestHeaders = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    response = requests.post(requestUrl, headers=requestHeaders)

    # Check for HTTP errors
    if response.status_code >= 400:
        logging.error(
            f"[orchestrator_client] Failed to obtain function key. HTTP status code: {response.status_code}. Error details: {response.text}"
        )
        function_key = None
    else:
        try:
            response_json = response.json()
            function_key = response_json["default"]
        except KeyError as e:
            function_key = None
            logging.error(
                f"[orchestrator_client] Error when getting function key. Details: {str(e)}."
            )

    return function_key


async def call_orchestrator_stream(thread_id: str, question: str, auth_info: dict):
    print("🟦 auth_info for orchestrator:", auth_info)
    url = os.getenv("ORCHESTRATOR_STREAM_ENDPOINT")
    if not url:
        raise Exception("ORCHESTRATOR_STREAM_ENDPOINT not set in environment variables")

    is_local = False
    if url:
        u = url.lower()
        is_local = ("localhost" in u) or ("127.0.0.1" in u)

    if is_local:
        function_key = None  # no key needed locally
    else:
        function_key = get_function_key()
        if not function_key:
            raise Exception(
                f"Error getting function key. Conversation ID: {thread_id if thread_id else 'N/A'}"
            )

    headers = {"Content-Type": "application/json"}
    if not is_local:
        headers["x-functions-key"] = function_key

    payload = {
        "thread_id": thread_id,
        "question": question,
        "client_principal_id": auth_info.get("client_principal_id", "no-auth"),
        "client_principal_name": auth_info.get("client_principal_name", "anonymous"),
        "client_group_names": auth_info.get("client_group_names", []),
        "access_token": auth_info.get("access_token"),
        "email": auth_info["email"],  # "admin"
        "name": auth_info["name"],  # "admin"
    }

    async with httpx.AsyncClient(timeout=None) as client:
        async with client.stream(
            "POST", url, json=payload, headers=headers
        ) as response:
            if response.status_code >= 400:
                raise Exception(
                    f"Error calling orchestrator. HTTP status code: {response.status_code}. Details: {response.reason_phrase}"
                )
            async for chunk in response.aiter_text():
                if not chunk:
                    continue
                yield chunk
                # logging.info("[orchestrator_client] Yielding text chunk: %s", chunk)
