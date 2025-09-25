import os
import re
import uuid
import json
import logging
import requests

logging.basicConfig(level=logging.INFO, force=True)

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

# from cosmos_layer import CosmosDataLayer

from postgres_layer import PostgresDataLayer

from dotenv import load_dotenv

load_dotenv()


from openai import AzureOpenAI

# Bing Agent client
bing_client = AzureOpenAI(
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),  # same key or a separate one
    azure_endpoint=os.getenv(
        "AZURE_OPENAI_ENDPOINT"
    ),  # e.g. https://<your-resource>.openai.azure.com/
    api_version="2024-08-01-preview",
)

BING_AGENT_DEPLOYMENT = os.getenv(
    "AZURE_OPENAI_DEPLOYMENT_NAME"
)  # e.g. "my-bing-agent"
# ==================== 🔹 Bing REST fallback helpers 🔹 ====================
BING_SUBSCRIPTION_KEY = os.getenv("BING_SEARCH_KEY")
BING_ENDPOINT = "https://api.bing.microsoft.com/v7.0/search"


def bing_rest_search(query: str, count: int = 5):
    headers = {"Ocp-Apim-Subscription-Key": BING_SUBSCRIPTION_KEY}
    params = {"q": query, "mkt": "es-ES", "count": count, "textDecorations": True}
    r = requests.get(BING_ENDPOINT, headers=headers, params=params, timeout=15)
    r.raise_for_status()
    items = r.json().get("webPages", {}).get("value", [])
    return [
        {"title": i["name"], "url": i["url"], "snippet": i.get("snippet", "")}
        for i in items
    ]


# ==================== 🔹 End Bing helpers 🔹 ====================
MIN_SIM_SCORE = 0.2  # tune this


def looks_like_no_answer(text: str) -> bool:
    if not text or len(text.strip()) < 40:
        return True
    patterns = [
        "no encontr",  # ES: no encontré / no encontrado
        "no pude encontrar",  # ES
        "no se encontr",  # ES
        "i couldn't find",  # EN
        "i don't have information",
        "no information found",
        "not found in the available resources",
    ]
    tl = text.lower()
    return any(p in tl for p in patterns)


async def quick_similarity_score(data_layer, query: str) -> float:
    logging.info(f"🔎 quick_similarity_score CALLED with query: {query}")

    """
    Implement a *fast* top-1 similarity peek using whatever your data layer exposes.
    Return 0.0 if you can’t compute it; we’ll then prefer web.
    """
    try:
        # Example API — adjust to your own data layer
        hits = await data_layer.semantic_search(query, k=1)
        if not hits:
            return 0.0
        logging.info(f"🔎 similarity score: {hits[0].get('score')} for query: {query}")
        return float(hits[0].get("score", 0.0))
    except Exception as e:
        logging.exception("semantic_search failed")
        return 0.0


async def ask_bing(markdown_instructions: str, user_query: str) -> str:
    """
    Call your Azure OpenAI deployment that has Bing/Web grounding enabled.
    We instruct it to ALWAYS include clickable Markdown links.
    """
    completion = bing_client.chat.completions.create(
        model=BING_AGENT_DEPLOYMENT,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a web search agent. "
                    "Always answer concisely and include a final section titled 'Fuentes' "
                    "with a bulleted list of clickable Markdown links to your sources."
                ),
            },
            {"role": "user", "content": f"{markdown_instructions}\n\n{user_query}"},
        ],
        temperature=0.2,
    )
    return completion.choices[0].message.content or "No response."


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


print("💥 Chainlit version =", cl.__version__)
print("ENABLE_AUTH =", os.getenv("ENABLE_AUTH"))
print("CHAINLIT_USERNAME =", os.getenv("CHAINLIT_USERNAME"))


# print("ORCHESTRATOR_STREAM_ENDPOINT =", os.getenv("ORCHESTRATOR_STREAM_ENDPOINT"))


# @cl.data_layer
# def get_data_layer():
#     return CosmosDataLayer(
#         endpoint=os.getenv("COSMOS_DB_URI") or cl.config.cosmosdb.uri,
#         key=os.getenv("COSMOS_DB_KEY") or cl.config.cosmosdb.key,
#         database_name=os.getenv("AZURE_DB_ID", "db0-wvvannyqg5e74"),
#         container_threads=os.getenv("AZURE_CONTAINER_NAME", "conversations"),
#         container_users=os.getenv("AZURE_USER_CONTAINER", "users"),
#     )


@cl.data_layer
def get_data_layer():
    print(f"🔗 Connecting to DB: {os.getenv('POSTGRES_URI')}")
    return PostgresDataLayer(os.getenv("POSTGRES_URI"))


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
            id="rag2",
            markdown_description="Main assistant profile for ASGPT answers",
        ),
        cl.ChatProfile(
            name="GPT-RAG",
            icon="🧠",
            id="rag3",
            markdown_description="Main assistant profile for GPT-RAG answers",
        ),
        cl.ChatProfile(
            name="Legal GPT",
            icon="🧠",
            id="rag4",
            markdown_description="Main assistant profile for Legal GPT answers",
        ),
    ]


USERS = {"admin": "1234", "james": "0000", "csaez": "0404"}


@cl.password_auth_callback
async def login(username: str, password: str):
    print(f"🔐 login() called for {username}")

    class SimpleUser:
        def __init__(self, identifier):
            self.identifier = identifier  # Login name
            self.id = str(
                uuid.uuid5(uuid.NAMESPACE_DNS, identifier)
            )  # Deterministic UUID based on username
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
                "id": self.id,
                "identifier": self.identifier,
                "name": self.name,
                "email": self.email,
                "metadata": self.metadata,
            }

    if USERS.get(username) == password:
        user = SimpleUser(username)
        # dl = PostgresDataLayer(
        #     endpoint=os.getenv("COSMOS_DB_URI"),
        #     key=os.getenv("COSMOS_DB_KEY"),
        #     database_name=os.getenv("AZURE_DB_ID", "db0-wvvannyqg5e74"),
        #     container_threads=os.getenv("AZURE_CONTAINER_NAME", "conversations"),
        #     container_users=os.getenv("AZURE_USER_CONTAINER", "users"),
        # )
        dl = PostgresDataLayer(os.getenv("POSTGRES_URI"))
        try:
            await dl.ensure_user_exists(user.to_dict())
        finally:
            await dl.aclose()
        return user

    return None


@cl.on_chat_start
async def on_chat_start():
    user = cl.user_session.get("user")
    if user and not getattr(user, "metadata", None):
        # Reconstruct and restore metadata if missing
        # (only necessary if it was returned from login() but not persisted)
        restored_metadata = {
            "email": f"{user.identifier}@example.com",
            "client_principal_id": user.identifier,
            "client_principal_name": user.identifier,
            "name": user.identifier,
            "authorized": True,
            "chat_profile": "rag",
        }
        user.metadata = restored_metadata
        cl.user_session.set("user", user)  # ✅ Safe to call here

    print("🟢 on_chat_start called")
    print("🧾 user from session:", user)
    print("🧾 user.metadata:", getattr(user, "metadata", {}))


@cl.on_chat_resume
async def on_chat_resume(thread):
    cl.user_session.set("conversation_id", thread["id"])

    # 👇 Show status
    await cl.Message(content="🔁 Restoring session...").send()

    data_layer = get_data_layer()
    thread_data = await data_layer.get_thread(thread["id"])

    if not thread_data or thread_data.get("id") != thread["id"]:
        logging.warning(
            f"[on_chat_resume] Thread not found or mismatch: {thread['id']}"
        )
        await cl.Message(content="⚠️ This thread no longer exists.").send()
        return

    steps = await data_layer.list_steps(thread["id"])
    logging.info(f"[on_chat_resume] Loaded {len(steps)} steps")

    non_messages = [s for s in steps if s.get("type") != "message"]
    logging.info(f"[on_chat_resume] Skipping {len(non_messages)} non-message steps")

    # 👇 Send messages one-by-one with debug log
    rendered = 0
    for step in steps:
        if step.get("type") != "message":
            continue

        role = step.get("role", "assistant")
        content = step.get("output" if role == "assistant" else "input", "")
        if not content:
            continue

        msg = cl.Message(
            content=content,
            author=step["author"]["identifier"],
            thread_id=thread["id"],
        )
        await msg.send()
        logging.info(
            f"[on_chat_resume] Sent message from {msg.author}: {msg.content[:60]}"
        )
        rendered += 1

    logging.info(f"[on_chat_resume] Rendered total: {rendered}")


def is_bing_question(text: str) -> bool:
    """
    Simple routing logic to decide when to call the Bing Agent.
    Replace with something smarter later.
    """
    return any(
        kw in text.lower()
        for kw in [
            "bing",
            "latest news",
            "search the web",
            "current president",
            "what's happening",
            "world news",
        ]
    )


@cl.on_message
async def handle_message(message: cl.Message):
    data_layer = get_data_layer()
    user = cl.user_session.get("user")
    print(f"🟠 on_message: user = {user}")

    message.id = message.id or str(uuid.uuid4())

    conversation_id = message.thread_id or cl.user_session.get("conversation_id")

    print(f"📨 Handling message for thread: {conversation_id}")

    if not conversation_id:

        conversation_id = await data_layer.create_thread(user.identifier)
        cl.user_session.set("conversation_id", conversation_id)

    message.thread_id = conversation_id
    cl.user_session.set("conversation_id", conversation_id)

    # --- Persist the user's message as a Chainlit step (role=user) ---
    user_identifier = getattr(user, "identifier", None) or "anonymous"
    await data_layer.create_step(
        {
            "threadId": conversation_id,
            "id": message.id,  # reuse Chainlit message id if available
            "role": "user",  # ✅ user
            "type": "message",
            "author": {"identifier": user_identifier},  # ✅ NOT 'anonymous'
            "input": message.content,  # user typed text
            "output": message.content,  # ensures it renders correctly
        }
    )

    response_msg = cl.Message(content="")
    await response_msg.send()

    if user and not user.metadata.get("authorized", True):
        await response_msg.stream_token(
            "Oops! It looks like you don’t have access to this service."
        )
        return

    # 🔎 1) Fast routing decision (index vs web) BEFORE heavy orchestration
    sim = await quick_similarity_score(data_layer, message.content)
    route_to_web = sim < MIN_SIM_SCORE

    if route_to_web:
        # 🌐 Go straight to web search (Bing-grounded Azure OpenAI)
        try:
            web_answer = await ask_bing(
                markdown_instructions="Responde en el idioma del usuario. Incluye 'Fuentes' con enlaces Markdown.",
                user_query=message.content,
            )
            await response_msg.stream_token(web_answer)
            response_msg.content = web_answer
            await response_msg.update()
        except Exception as e:
            await response_msg.stream_token(f"⚠️ Bing Agent error: {e}")
        # Persist assistant step & close as you already do
        # (… keep your existing persistence code below …)
        # IMPORTANT: return here so we don't run the orchestrator too.
        return

    # 👉 Bing Agent path
    if is_bing_question(message.content):
        response_msg = cl.Message(content="")
        await response_msg.send()
        try:
            completion = await bing_client.chat.completions.create(
                model=BING_AGENT_DEPLOYMENT,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a helpful Bing search agent.",
                    },
                    {"role": "user", "content": message.content},
                ],
            )
            answer = completion.choices[0].message.content
            await response_msg.stream_token(answer)
            response_msg.content = answer
            await response_msg.update()
        except Exception as e:
            await response_msg.stream_token(f"⚠️ Bing Agent error: {e}")
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
                if not part or part.lower() in {"heartbeat", "ping"}:
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
                    # Keep Chainlit session aligned with the server-provided conversation id
                    if extracted_id != cl.user_session.get("conversation_id"):
                        cl.user_session.set("conversation_id", extracted_id)

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

    if looks_like_no_answer(full_text):
        try:
            web_answer = await ask_bing(
                markdown_instructions="Responde en el idioma del usuario. Incluye 'Fuentes' con enlaces Markdown.",
                user_query=message.content,
            )
            # Nicely separate the two sections
            await response_msg.stream_token("\n\n---\n\n🌐 Búsqueda en la web:\n")
            await response_msg.stream_token(web_answer)
            full_text = (full_text or "") + "\n\n---\n\n" + web_answer
        except Exception as e:
            await response_msg.stream_token(f"\n\n⚠️ Bing Agent error: {e}")

    message_list = cl.user_session.get("message_list") or []
    message_list.append({"question": message.content, "answer": full_text})
    cl.user_session.set("message_list", message_list)

    logging.info(f"[response message is]: {response_msg}")

    response_msg.content = full_text
    await response_msg.update()

    # --- Persist the assistant's reply as a Chainlit step (role=assistant) ---
    await data_layer.create_step(
        {
            "threadId": conversation_id,
            "role": "assistant",  # ✅ assistant
            "type": "message",
            "author": {"identifier": "assistant"},
            "input": "",
            "output": full_text,  # streamed final answer
        }
    )

    if full_text and message.content:
        await data_layer.update_thread(
            conversation_id,
            name=message.content[:60],
            summary=message.content[:60],
        )

        # ✅ No refresh_threads in current Chainlit version
        logging.info("🧵 Thread title updated. Will appear after sidebar refresh.")
        # Close the pool after all DB work is done
    await data_layer.aclose()
