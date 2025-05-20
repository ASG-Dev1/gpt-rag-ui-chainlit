import os
import re
import uuid
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


# @cl.on_chat_start
# async def on_chat_start():
#     await cl.Message(
#         content="Choose an option:",
#         actions=[
#             Action(name="option_a", label="Option A", payload={"choice": "A"}),
#             Action(name="option_b", label="Option B", payload={"choice": "B"}),
#         ],
#     ).send()


# @cl.action_callback("option_a")
# async def handle_option_a(action: cl.Action):
#     await cl.Message(content="✅ You selected Option A").send()


# @cl.action_callback("option_b")
# async def handle_option_b(action: cl.Action):
#     await cl.Message(content="✅ You selected Option B").send()


# 🔁 Sidebar update helper
async def update_sidebar():
    user = cl.user_session.get("user")
    user_id = user.metadata.get("client_principal_id") if user else "no-auth"

    history = await get_user_history_from_cosmos(user_id)
    elements = []

    for convo in history:
        convo_id = convo["id"]
        summary = convo.get("summary") or convo.get("messages", [{}])[0].get(
            "content", convo_id[:8]
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

    await ElementSidebar.set_title("🧾 Past Conversations")
    await ElementSidebar.set_elements(elements)


# 🧠 Start of chat - calls the sidebar updater
@cl.on_chat_start
async def on_chat_start():
    cl.user_session.set("conversation_id", str(uuid.uuid4()))
    await update_sidebar()
    await cl.Message(
        content="👋 Welcome! Choose a conversation or start a new one."
    ).send()


@cl.action_callback("resume_convo")
async def on_resume_convo(action: cl.Action):
    # convo_id = action.value
    convo_id = action.payload["value"]
    cl.user_session.set("conversation_id", convo_id)
    await cl.Message(content=f"🔄 Resuming conversation {convo_id}").send()

    messages = await get_user_messages(convo_id)
    for msg in messages:
        await cl.Message(content=msg["content"], author=msg["speaker"]).send()


@cl.on_chat_resume
async def on_chat_resume(thread: ThreadDict):
    # 1) Update your session’s conversation_id
    cl.user_session.set("conversation_id", thread["id"])

    # 2) Optionally rehydrate any in-memory state or agents,

    await cl.Message(content=f"🔄 Resuming conversation {thread['id']}").send()


@cl.on_message
async def handle_message(message: cl.Message):
    # ✅ Handle Action button click
    if message.metadata and message.metadata.get("action"):
        action = message.metadata["action"]
        payload = action.get("payload", {})
        choice = payload.get("choice", "unknown")
        await cl.Message(
            content=f"You clicked: {action['label']} (choice={choice})"
        ).send()
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
    response_msg = cl.Message(author="assistant", content="")

    app_user = cl.user_session.get("user")
    if app_user and not app_user.metadata.get("authorized", True):
        await response_msg.stream_token(
            "Oops! It looks like you don’t have access to this service. "
            "If you think you should, please reach out to your administrator for help."
        )
        return

    await response_msg.stream_token(" ")

    buffer = ""
    full_text = ""
    references = set()
    auth_info = check_authorization()
    generator = call_orchestrator_stream(conversation_id, message.content, auth_info)

    try:
        async for chunk in generator:
            # logging.info("[app] Chunk received: %s", chunk)

            # Extract and update conversation ID
            extracted_id, cleaned_chunk = extract_conversation_id_from_chunk(chunk)
            if extracted_id:
                conversation_id = extracted_id

            cleaned_chunk = cleaned_chunk.replace("\\n", "\n")

            # Track and clean references
            found_refs = set(REFERENCE_REGEX.findall(cleaned_chunk))
            if found_refs:
                logging.info("[app] Found file references: %s", found_refs)
            references.update(found_refs)
            cleaned_chunk = REFERENCE_REGEX.sub("", cleaned_chunk)

            buffer += cleaned_chunk
            full_text += cleaned_chunk

            # Handle TERMINATE token
            token_index = buffer.find(TERMINATE_TOKEN)
            if token_index != -1:
                if token_index > 0:
                    await response_msg.stream_token(buffer[:token_index])
                logging.info(
                    "[app] TERMINATE token detected. Draining remaining chunks..."
                )
                async for _ in generator:
                    pass  # drain
                break

            # Stream safe part of buffer
            safe_flush_length = len(buffer) - (len(TERMINATE_TOKEN) - 1)
            if safe_flush_length > 0:
                await response_msg.stream_token(buffer[:safe_flush_length])
                buffer = buffer[safe_flush_length:]

    except Exception as e:
        error_message = (
            "I'm sorry, I had a problem with the request. Please report the error. "
            f"Details: {e}"
        )
        logging.exception("[app] Error during message handling.")
        await response_msg.stream_token(error_message)

    finally:
        try:
            await generator.aclose()
        except RuntimeError as exc:
            if "async generator ignored GeneratorExit" not in str(exc):
                raise

    cl.user_session.set("conversation_id", conversation_id)
    await response_msg.update()
    # Save question-answer to message list

    # if not cl.user_session.get("message_list"):
    #     cl.user_session.set("message_list", [])

    # message_list = cl.user_session.get("message_list")
    # message_list.append({"question": message.content, "answer": full_text})
    # cl.user_session.set("message_list", message_list)

    # Final reference handling and update
    # references.update(REFERENCE_REGEX.findall(full_text))
    # final_text = replace_source_reference_links(full_text.replace(TERMINATE_TOKEN, ""))
    # response_msg.content = final_text
    await response_msg.update()
