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

from util.cosmos_history import get_user_history_from_cosmos, get_user_messages
from orchestrator_client import call_orchestrator_stream

from dotenv import load_dotenv

load_dotenv()


my_secret = os.getenv("CHAINLIT_AUTH_SECRET")
print("CHAINLIT_AUTH_SECRET:", my_secret)
my_auth = os.getenv("CHAINLIT_AUTH")
print("CHAINLIT_AUTH:", my_auth)

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
    if app_user:
        metadata = app_user.metadata or {}
        return {
            "authorized": metadata.get("authorized", True),
            "client_principal_id": metadata.get("client_principal_id", "no-auth"),
            "client_principal_name": metadata.get("client_principal_name", "anonymous"),
            "client_group_names": metadata.get("client_group_names", []),
            "access_token": metadata.get("access_token"),
        }

    return {
        "authorized": True,
        "client_principal_id": "no-auth",
        "client_principal_name": "anonymous",
        "client_group_names": [],
        "access_token": None,
    }


# Defines a list of available chat profiles (e.g, different assistant personas)
@cl.set_chat_profiles
async def chat_profiles():
    return [
        cl.ChatProfile(
            name="GPT-RAG",
            icon="🧠",
            id="rag",
            markdown_description="Main assistant profile for RAG answers",
        )
    ]


# Defines a callback for password-based authentication
@cl.password_auth_callback
def login(username: str, password: str):
    class SimpleUser:
        def __init__(self, identifier):
            self.identifier = identifier
            self.metadata = {
                "email": identifier,
                "client_principal_id": identifier,
                "client_principal_name": identifier,
                "chat_profile": "rag",  # 🔥 this is the key
            }

        def to_dict(self):
            return {"identifier": self.identifier, "metadata": self.metadata}

    if username == "admin" and password == "1234":
        return SimpleUser("admin")
    return None


# 🔁 Sidebar update helper
# async def update_sidebar():
#     user = cl.user_session.get("user")
#     user_id = user.metadata.get("client_principal_id") if user else "no-auth"
#     history = await get_user_history_from_cosmos(user_id)
#     elements = []

#     for convo in history:
#         convo_id = convo["id"]
#         summary = convo.get("summary") or convo.get("messages", [{}])[0].get(
#             "content", convo_id[:40]
#         )

#         elements.append(
#             cl.Text(
#                 content=summary,
#                 name=f"convo_{convo_id}",
#                 display="side",
#                 actions=[
#                     cl.Action(
#                         name="resume_convo",
#                         label="▶ Resume Conversation",
#                         payload={"value": convo_id},
#                     )
#                 ],
#             )
#         )


#     await ElementSidebar.set_title("Conversation Histories")
#     await ElementSidebar.set_elements(elements)
async def update_sidebar():
    user = cl.user_session.get("user")
    user_id = user.metadata.get("client_principal_id") if user else "no-auth"
    history = await get_user_history_from_cosmos(user_id)

    elements = []

    for convo in history:
        convo_id = convo["id"]
        summary = convo.get("summary") or convo.get("messages", [{}])[0].get(
            "content", convo_id[:40]
        )

        elements.append(
            cl.Text(
                content=summary,
                name=f"convo_{convo_id}",
                display="side",
                actions=[
                    cl.Action(
                        name="resume_convo",
                        label="▶ Resume",
                        payload={"value": convo_id},
                    )
                ],
            )
        )

    await ElementSidebar.set_title("💬 Conversation History")
    await ElementSidebar.set_elements(elements)


#  Start of chat - calls the sidebar updater
@cl.on_chat_start
async def on_chat_start():
    cl.user_session.set("conversation_id", str(uuid.uuid4()))
    await update_sidebar()
    await cl.Message(
        content="👋 Welcome to ASGPT 2.0! Choose a conversation or start a new one."
    ).send()


# Starters appear below chat message box
# @cl.set_starters
# async def set_starters():
#     return [
#         cl.Starter(
#             label="Fix Grammer",
#             message="Review and fix grammatical errors",
#             icon="/public/favicon.ico",
#         )
#     ]


@cl.action_callback("resume_convo")
async def on_resume_convo(action: cl.Action):
    print(f"💥 CLICKED: {action.payload}")  # Add this to verify click
    convo_id = action.payload["value"]
    cl.user_session.set("conversation_id", convo_id)

    await cl.Message(content=f"🔄 Resuming conversation {convo_id}").send()

    messages = await get_user_messages(convo_id)
    for msg in messages:
        content = msg["content"].replace("\\n", "\n").replace("TERMINATE", "").strip()
        await cl.Message(content=content, author=msg["speaker"]).send()


# Resume a previous chat session, restore conversation context
@cl.on_chat_resume
async def on_chat_resume(thread: ThreadDict):
    # 1) Update session’s conversation_id
    cl.user_session.set("conversation_id", thread["id"])

    # 2) Optionally rehydrate any in-memory state or agents,

    await cl.Message(content=f"🔄 Resuming conversation {thread['id']}").send()


# Message sends and generates and sends a response
@cl.on_message
async def handle_message(message: cl.Message):
    # 🔁 TEMP DEBUG: manual resume command
    if message.content.startswith("/go "):
        convo_id = message.content.split("/go ")[1].strip()
        print(f"💥 Manually resuming: {convo_id}")
        await on_resume_convo(
            cl.Action(name="resume_convo", payload={"value": convo_id})
        )
        return

    if message.content.strip().lower() == "/history":
        message_list = cl.user_session.get("message_list", [])
        if not message_list:
            await cl.Message(content="No chat history found.").send()
            return
        for entry in message_list[-5:]:
            question = entry.get("question", "No question recorded.")
            answer = entry.get("answer", "No response recorded.")
            await cl.Message(content=f"**Q:** {question}\n**A:** {answer}").send()
        return

    message.id = message.id or str(uuid.uuid4())
    conversation_id = cl.user_session.get("conversation_id") or ""
    response_msg = cl.Message(content="")

    app_user = cl.user_session.get("user")
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
    await update_sidebar()
    # Strip TERMINATE before saving
    full_text = full_text.replace(TERMINATE_TOKEN, "").strip()

    message_list = cl.user_session.get("message_list") or []
    message_list.append({"question": message.content, "answer": full_text})
    cl.user_session.set("message_list", message_list)
    logging.info(f"[response message is]:", response_msg)
    await response_msg.update()
