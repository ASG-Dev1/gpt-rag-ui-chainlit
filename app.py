import os
import re
import uuid
import json
import logging
import urllib.parse
from typing import Optional, Tuple

import chainlit as cl
from chainlit import Action

from chainlit.types import ThreadDict
from chainlit import Text, ElementSidebar
from chainlit.data import BaseDataLayer
from orchestrator_client import call_orchestrator_stream
from cosmos_layer import CosmosDataLayer
from dotenv import load_dotenv

load_dotenv()


# my_secret = os.getenv("CHAINLIT_AUTH_SECRET")
# print("CHAINLIT_AUTH_SECRET:", my_secret)
# my_auth = os.getenv("CHAINLIT_AUTH")
# print("CHAINLIT_AUTH:", my_auth)
print("💥 Chainlit version =", cl.__version__)
print("ENABLE_AUTH =", os.getenv("ENABLE_AUTH"))
print("CHAINLIT_USERNAME =", os.getenv("CHAINLIT_USERNAME"))
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


@cl.data_layer
def get_data_layer():
    return CosmosDataLayer(
        endpoint=os.getenv("COSMOS_DB_URI") or cl.config.cosmosdb.uri,
        key=os.getenv("COSMOS_DB_KEY") or cl.config.cosmosdb.key,
        database_name=os.getenv("AZURE_DB_ID", "db0-wvvannyqg5e74"),
        container_threads=os.getenv("AZURE_CONTAINER_NAME", "conversations"),
        # opcional: contenedor de usuarios (si lo usas)
        container_users=os.getenv("AZURE_USER_CONTAINER", "users"),
    )


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
            name="ASGPT",
            icon="favicon",
            id="rag",
            markdown_description="Main assistant profile for ASGPT answers",
        ),
        cl.ChatProfile(
            name="ASGPT 2.0",
            icon="🧠",
            id="rag",
            markdown_description="Main assistant profile for ASGPT 2.0 answers",
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
def login(username: str, password: str):
    import asyncio
    from cosmos_layer import CosmosDataLayer

    class SimpleUser:
        def __init__(self, identifier):
            self.identifier = identifier
            self.id = identifier
            self.metadata = {
                "email": identifier,
                "client_principal_id": identifier,
                "client_principal_name": identifier,
                "name": identifier,
                "authorized": True,
                "chat_profile": "rag",
            }

        def to_dict(self):
            return {"identifier": self.identifier, "metadata": self.metadata}

    if USERS.get(username) == password:
        user = SimpleUser(username)
        # ---- NEW: Ensure user exists in CosmosDB ----
        dl = CosmosDataLayer(
            endpoint=os.getenv("COSMOS_DB_URI"),
            key=os.getenv("COSMOS_DB_KEY"),
            database_name=os.getenv("AZURE_DB_ID", "db0-wvvannyqg5e74"),
            container_threads=os.getenv("AZURE_CONTAINER_NAME", "conversations"),
            container_users=os.getenv("AZURE_USER_CONTAINER", "users"),
        )
        # This should be an async method in your CosmosDataLayer:
        asyncio.run(dl.ensure_user_exists(user.to_dict()))
        # ---------------------------------------------
        return user
    return None


@cl.on_chat_start
async def on_chat_start():
    cl.user_session.set("conversation_id", str(uuid.uuid4()))

    # Show welcome message and action buttons in chat
    await cl.Message(
        content="👋 Welcome to ASGPT 2.0! Select a past conversation to resume:",
    ).send()


@cl.on_chat_resume
async def resume(thread):
    cl.user_session.set("conversation_id", thread["id"])
    cl.Message(content="👍 Back where we left off!").send()


@cl.on_message
async def handle_message(message: cl.Message):
    user = cl.user_session.get("user")
    print(f"🟠 on_message: user = {user}")

    message.id = message.id or str(uuid.uuid4())
    conversation_id = cl.user_session.get("conversation_id") or ""
    response_msg = cl.Message(content="")

    app_user = cl.user_session.get("user")
    print("🔐 Local user session:", cl.user_session.get("user"))
    if app_user and not app_user.metadata.get("authorized", True):
        await response_msg.stream_token(
            "Oops! It looks like you don’t have access to this service."
        )
        return

    await response_msg.stream_token(" ")  # keep to initialize stream

    buffer = ""
    full_text = ""
    references = set()
    auth_info = check_authorization()
    generator = call_orchestrator_stream(conversation_id, message.content, auth_info)

    try:
        async for chunk in generator:
            chunk = chunk.strip()

            # Handle multi-line "data:" entries from orchestrator
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

                # flush safe portion
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

    cl.user_session.set("conversation_id", conversation_id)

    full_text = full_text.replace(TERMINATE_TOKEN, "").strip()

    message_list = cl.user_session.get("message_list") or []
    message_list.append({"question": message.content, "answer": full_text})
    cl.user_session.set("message_list", message_list)
    logging.info(f"[response message is]: {response_msg}")
    await response_msg.update()
