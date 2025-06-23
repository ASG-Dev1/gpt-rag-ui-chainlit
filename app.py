import os
import re
import uuid
import json
import logging


logging.basicConfig(level=logging.INFO)

# Quiet down SDKs
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
    logging.WARNING
)
logging.getLogger("azure.cosmos").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
import urllib.parse
from typing import Optional, Tuple

import chainlit as cl
from chainlit import Action

from datetime import datetime, timezone

# from chainlit.types import ThreadDict
from chainlit import Text, ElementSidebar
from chainlit.data import BaseDataLayer
from orchestrator_client import call_orchestrator_stream
from cosmos_layer import CosmosDataLayer
from dotenv import load_dotenv

load_dotenv()


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


print("💥 Chainlit version =", cl.__version__)
print("ENABLE_AUTH =", os.getenv("ENABLE_AUTH"))
print("CHAINLIT_USERNAME =", os.getenv("CHAINLIT_USERNAME"))


@cl.data_layer
def get_data_layer():
    return CosmosDataLayer(
        endpoint=os.getenv("COSMOS_DB_URI") or cl.config.cosmosdb.uri,
        key=os.getenv("COSMOS_DB_KEY") or cl.config.cosmosdb.key,
        database_name=os.getenv("AZURE_DB_ID", "db0-wvvannyqg5e74"),
        container_threads=os.getenv("AZURE_CONTAINER_NAME", "conversations"),
        container_users=os.getenv("AZURE_USER_CONTAINER", "users"),
    )


# Constants
UUID_REGEX = re.compile(
    r"^\s*([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\s+",
    re.IGNORECASE,
)

SUPPORTED_EXTENSIONS = [
    "pdf",
    "bmp",
    "jpeg",
    "png",
    "tiff",
    "xlsx",
    "docx",
    "pptx",
    "md",
    "txt",
    "html",
    "shtml",
    "htm",
    "py",
    "csv",
    "xml",
    "json",
    "vtt",
]

REFERENCE_REGEX = re.compile(
    r"\[([^\]]+\.(?:" + "|".join(SUPPORTED_EXTENSIONS) + r"))\]", re.IGNORECASE
)

TERMINATE_TOKEN = "TERMINATE"


# Helpers
def read_env_boolean(var_name: str, default: bool = False) -> bool:
    value = os.getenv(var_name, str(default)).strip().lower()
    return value in {"true", "1", "yes"}


def extract_conversation_id_from_chunk(chunk: str) -> Tuple[Optional[str], str]:
    match = UUID_REGEX.match(chunk)
    if match:
        conv_id = match.group(1)
        logging.info("[app] Extracted Conversation ID: %s", conv_id)
        return conv_id, chunk[match.end() :]
    return None, chunk


def replace_source_reference_links(text: str) -> str:
    def replacer(match):
        source_file = match.group(1)
        decoded = urllib.parse.unquote(source_file)
        encoded = urllib.parse.quote(decoded)
        return f"[{decoded}](/source/{encoded})"

    return re.sub(REFERENCE_REGEX, replacer, text)


def check_authorization() -> dict:
    app_user = cl.user_session.get("user")
    result = {
        "authorized": True,
        "client_principal_id": "no-auth",
        "client_principal_name": "anonymous",
        "email": "unknown",
        "name": "anonymous",
        "client_group_names": [],
        "access_token": None,
    }
    if app_user:
        # Prefer metadata, fallback to user.identifier if metadata is empty
        metadata = getattr(app_user, "metadata", {}) or {}
        if metadata:
            result["authorized"] = metadata.get("authorized", True)
            result["client_principal_id"] = metadata.get(
                "client_principal_id", metadata.get("email", "no-auth")
            )
            result["client_principal_name"] = metadata.get(
                "client_principal_name", metadata.get("email", "anonymous")
            )
            result["email"] = metadata.get(
                "email", metadata.get("client_principal_name", "unknown")
            )
            result["name"] = metadata.get(
                "name", metadata.get("client_principal_name", "anonymous")
            )
            result["client_group_names"] = metadata.get("client_group_names", [])
            result["access_token"] = metadata.get("access_token")
        else:
            # No metadata: use identifier for everything
            identifier = getattr(app_user, "identifier", None)
            if identifier:
                result["client_principal_id"] = identifier
                result["client_principal_name"] = identifier
                result["email"] = identifier
                result["name"] = identifier
    return result


# Defines a list of available chat profiles (e.g, different assistant personas)
@cl.set_chat_profiles
async def chat_profiles():
    return [
        cl.ChatProfile(
            name="ASGPT 2.0",
            icon="🧠",
            id="rag",
            markdown_description="Main assistant profile for ASGPT 2.0 answers",
        ),
        cl.ChatProfile(
            name="ASGPT",
            icon="favicon",
            id="rag",
            markdown_description="Main assistant profile for ASGPT answers",
        ),
        cl.ChatProfile(
            name="GPT-RAG",
            icon="🧠",
            id="rag",
            markdown_description="Main assistant profile for GPT-RAG answers",
        ),
        cl.ChatProfile(
            name="Legal GPT",
            icon="🧠",
            id="rag",
            markdown_description="Main assistant profile for Legal GPT answers",
        ),
    ]


USERS = {"admin": "1234", "james": "0000", "csaez": "0404"}


@cl.password_auth_callback
async def login(username: str, password: str):
    from cosmos_layer import CosmosDataLayer

    class SimpleUser:
        def __init__(self, identifier):
            self.identifier = identifier
            self.id = identifier
            self.name = identifier
            self.email = f"{identifier}@example.com"
            self.metadata = {
                "email": self.email,
                "client_principal_id": identifier,
                "client_principal_name": identifier,
                "name": identifier,
                "authorized": True,
                "chat_profile": "rag",
            }

        def to_dict(self):
            return {
                "identifier": self.identifier,
                "name": self.name,
                "email": self.email,
                "metadata": self.metadata,
            }

    if USERS.get(username) == password:
        user = SimpleUser(username)
        dl = CosmosDataLayer(
            endpoint=os.getenv("COSMOS_DB_URI"),
            key=os.getenv("COSMOS_DB_KEY"),
            database_name=os.getenv("AZURE_DB_ID", "db0-wvvannyqg5e74"),
            container_threads=os.getenv("AZURE_CONTAINER_NAME", "conversations"),
            container_users=os.getenv("AZURE_USER_CONTAINER", "users"),
        )
        try:
            await dl.ensure_user_exists(user.to_dict())
        finally:
            await dl.aclose()
        return user

    return None


# @cl.on_chat_start
async def on_chat_start():
    data_layer = get_data_layer()
    user = cl.user_session.get("user")

    # Explicitly create a new thread early - Helps conversation thread appear on sidebar without refreshing the page.
    if user:
        thread_id = await data_layer.create_thread(user.identifier)
        cl.user_session.set("conversation_id", thread_id)

    await cl.Message(content="Welcome!").send()


@cl.on_message
async def handle_message(message: cl.Message):
    data_layer = get_data_layer()
    user = cl.user_session.get("user")
    print(f"🟠 on_message: user = {user}")

    message.id = message.id or str(uuid.uuid4())

    conversation_id = cl.user_session.get("conversation_id")
    if not conversation_id:
        conversation_id = message.thread_id
        cl.user_session.set("conversation_id", conversation_id)

    response_msg = cl.Message(content="")
    await response_msg.send()
    if user and not user.metadata.get("authorized", True):
        await response_msg.stream_token(
            "Oops! It looks like you don’t have access to this service."
        )
        return

    buffer = ""
    full_text = ""
    references = set()
    auth_info = check_authorization()
    generator = call_orchestrator_stream(conversation_id, message.content, auth_info)

    try:
        async for chunk in generator:
            chunk = chunk.strip()
            parts = chunk.split("data:")
            for part in parts:
                part = part.strip()
                if not part:
                    continue
                try:
                    parsed = json.loads(part)
                    cleaned_chunk = parsed.get("content", "")
                except Exception as e:
                    logging.warning(f"[parser] Failed to parse chunk: {e}")
                    continue

                extracted_id, cleaned_chunk = extract_conversation_id_from_chunk(
                    cleaned_chunk
                )
                if extracted_id:
                    conversation_id = extracted_id

                cleaned_chunk = cleaned_chunk.replace("\\n", "\n")
                found_refs = set(REFERENCE_REGEX.findall(cleaned_chunk))
                references.update(found_refs)
                cleaned_chunk = REFERENCE_REGEX.sub("", cleaned_chunk)

                buffer += cleaned_chunk
                full_text += cleaned_chunk

                token_index = buffer.find(TERMINATE_TOKEN)
                if token_index != -1:
                    if token_index > 0:
                        await response_msg.stream_token(buffer[:token_index])
                    logging.info(
                        "[app] TERMINATE detected. Draining remaining chunks..."
                    )
                    async for _ in generator:
                        pass
                    break

                safe_flush_length = len(buffer) - len(TERMINATE_TOKEN)
                if safe_flush_length > 0:
                    await response_msg.stream_token(buffer[:safe_flush_length])
                    buffer = buffer[safe_flush_length:]

    except Exception as e:
        logging.exception("[app] Error during message handling.")
        await response_msg.stream_token(f"⚠️ Error: {e}")
    finally:
        try:

            await generator.aclose()
        except Exception:
            pass
    full_text = full_text.replace(TERMINATE_TOKEN, "").replace("\\n", "\n")
    full_text = re.sub(r"(?<=[a-zA-Z])(?=[A-Z])", " ", full_text)

    message_list = cl.user_session.get("message_list") or []
    message_list.append({"question": message.content, "answer": full_text})
    cl.user_session.set("message_list", message_list)

    logging.info(f"[response message is]: {response_msg}")
    # await response_msg.update()
    await response_msg.update(full_text)
