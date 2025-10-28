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

from chainlit import Text, ElementSidebar
from chainlit.data import BaseDataLayer
from orchestrator_client import call_orchestrator_stream


from postgres_layer import PostgresDataLayer

from dotenv import load_dotenv

load_dotenv()


print("💥 Chainlit version =", cl.__version__)
print("ENABLE_AUTH =", os.getenv("ENABLE_AUTH"))
print("CHAINLIT_USERNAME =", os.getenv("CHAINLIT_USERNAME"))

# === Blob URL helper ===
BLOB_BASE_URL = os.getenv("BLOB_BASE_URL", "").rstrip("/")
BLOB_CONTAINER_SAS = (os.getenv("BLOB_CONTAINER_SAS") or "").lstrip("?")


def build_blob_url(filename: str) -> str:
    """
    Build a proper Azure Blob Storage URL for a given filename.
    """
    storage = os.getenv("BLOB_ACCOUNT_NAME", "asgprjedi1")
    container = os.getenv("BLOB_CONTAINER_NAME", "attachments")
    sas_token = (os.getenv("BLOB_CONTAINER_SAS") or "").lstrip("?")

    safe_name = urllib.parse.quote(filename)
    base = f"https://{storage}.blob.core.windows.net/{container}/{safe_name}"
    return f"{base}?{sas_token}" if sas_token else base


# ==================== 🔹 Bing Agent helpers 🔹 ====================
from openai import AzureOpenAI

# ==================== 🔹 Conversation Summary Helpers 🔹 ====================
summary_client = AzureOpenAI(
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_version="2024-02-15-preview",
)


def is_summary_request(text: str) -> bool:
    keywords = [
        "resume nuestra conversación",
        "resumen de la conversación",
        "que te pregunté",
        "de que temas hablamos",
        "de qué temas hablamos",
        "de qué hablamos",
        "qué te pregunté",
        "summarize",
        "what did i ask",
        "summary of our chat",
    ]
    return any(k in text.lower() for k in keywords)


async def summarize_conversation(conversation_text: str) -> str:
    completion = summary_client.chat.completions.create(
        model=os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME"),
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a helpful summarizer. Summarize the conversation in a few sentences "
                    "mentioning the main topics discussed. Respond in the same language the user used."
                ),
            },
            {"role": "user", "content": conversation_text},
        ],
        temperature=0.3,
        max_tokens=200,
    )
    return completion.choices[0].message.content.strip()


# ==================== 🔹 Bing Values 🔹 ====================
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
# # hitting Microsoft’s standalone Bing Search API (a REST endpoint you can use for raw search results).
# BING_SUBSCRIPTION_KEY = os.getenv("BING_SEARCH_KEY")
# BING_ENDPOINT = "https://api.bing.microsoft.com/v7.0/search"


# def bing_rest_search(query: str, count: int = 5):
#     headers = {"Ocp-Apim-Subscription-Key": BING_SUBSCRIPTION_KEY}
#     params = {"q": query, "mkt": "es-ES", "count": count, "textDecorations": True}
#     r = requests.get(BING_ENDPOINT, headers=headers, params=params, timeout=15)
#     r.raise_for_status()
#     items = r.json().get("webPages", {}).get("value", [])
#     return [
#         {"title": i["name"], "url": i["url"], "snippet": i.get("snippet", "")}
#         for i in items
#     ]


# ==================== 🔹 End Bing helpers 🔹 ====================
MIN_SIM_SCORE = 0.0  # tune this


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


# async def ask_bing(markdown_instructions: str, user_query: str) -> str:
#     """
#     Call Azure AI Foundry Agent endpoint directly for real-time Bing-grounded search.
#     Includes safety checks for missing or malformed endpoint URLs.
#     """
#     try:
#         agent_id = os.getenv("AZURE_OPENAI_WS_AGENT_ID")
#         endpoint = os.getenv("AZURE_OPENAI_WS_ENDPOINT", "").strip()
#         api_key = os.getenv("AZURE_OPENAI_API_KEY")

#         # ✅ Safety: ensure endpoint starts with https://
#         if not endpoint:
#             raise ValueError(
#                 "AZURE_OPENAI_WS_ENDPOINT environment variable is not set."
#             )
#         if not endpoint.startswith("http"):
#             endpoint = f"https://{endpoint.lstrip('/')}"

#         endpoint = endpoint.rstrip("/")
#         url = f"{endpoint}/openai/agents/{agent_id}/chat/completions?api-version=2024-10-01-preview"

#         # 🪵 Log for debugging
#         logging.info(f"[ask_bing] Using endpoint: {endpoint}")
#         logging.info(f"[ask_bing] Final URL: {url}")

#         headers = {"Content-Type": "application/json", "api-key": api_key}
#         payload = {
#             "input": f"{markdown_instructions}\n\n{user_query}",
#             "stream": False,
#         }

#         response = requests.post(url, headers=headers, json=payload, timeout=30)
#         response.raise_for_status()
#         data = response.json()

#         if data.get("output"):
#             return data["output"][0]["content"][0]["text"]
#         else:
#             return "⚠️ No response from Bing Agent."

#     except Exception as e:
#         logging.exception("❌ Bing Agent call failed")
#         return f"⚠️ Error en la búsqueda web: {e}"


# def _iso_now() -> str:
#     return datetime.now(timezone.utc).isoformat()


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


# Postgres data layer
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


def extract_thread_id_from_chunk(chunk: str) -> Tuple[Optional[str], str]:
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


@cl.set_starters
async def set_starters():
    return [
        cl.Starter(
            label="ASG",
            message="Cuál es el portal oficial de la Administración de Servicios Generales de Puerto Rico?",
            icon="/public/asg_concept-05.png",
        ),
        cl.Starter(
            label="Mercadito",
            message="Cuál es el portal oficial para acceder a J.E.D.I de la Administración de Servicios Generales de Puerto Rico?",
            icon="/public/cart.jpg",
        ),
        cl.Starter(
            label="FAQ",
            message="Qué preguntas frecuentes hay sobre la Administración de Servicios Generales de Puerto Rico?",
            icon="/public/faq.jpg",
            # command="ASGPT-FAQ",
        ),
        cl.Starter(
            label="Clima",
            message="Cuál es el pronóstico del tiempo para hoy en San Juan, Puerto Rico?",
            icon="/public/forecast.png",
        ),
    ]


# Defines a list of available chat profiles (e.g, different assistant personas)
@cl.set_chat_profiles
async def chat_profiles():
    transparent_pixel = (
        "data:image/png;base64,"
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGNgYGBgAAAABAABJzQnCgAAAABJRU5ErkJggg=="
    )

    return [
        cl.ChatProfile(
            name="ASGPT 2.0",
            id="rag",
            # icon=transparent_pixel,
            icon="/public/asgpt 2.0.png",
            markdown_description=(
                # "![ASGPT Logo](/public/bgg.png)\n\n"
                "**Bienvenido a ASGPT 2.0**"
            ),
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


# @cl.on_chat_start
# async def demo_pdf():
#     import httpx

#     storage = os.getenv("BLOB_ACCOUNT_NAME", "asgprjedi1")
#     container = os.getenv("BLOB_CONTAINER_NAME", "chainlit-attachments-pdf")
#     sas_token = os.getenv("BLOB_CONTAINER_SAS", "")

#     pdf_url = (
#         f"https://{storage}.blob.core.windows.net/{container}/1431815.pdf?{sas_token}"
#     )

#     async with httpx.AsyncClient(timeout=30) as client:
#         r = await client.get(pdf_url)
#         r.raise_for_status()
#         pdf_bytes = r.content  # esto sí es el PDF

#     await cl.Message(
#         content="",
#         elements=[
#             cl.Pdf(name="Inline test", display="inline", content=pdf_bytes, page=1),
#             # cl.Pdf(name="Sidebar test", display="side", content=pdf_bytes, page=1),
#         ],
#     ).send()


# async def demo_pdf():
#     storage = os.getenv("BLOB_ACCOUNT_NAME", "asgprjedi1")
#     container = os.getenv("BLOB_CONTAINER_NAME", "chainlit-attachments-pdf")
#     sas_token = os.getenv("BLOB_CONTAINER_SAS", "")

#     pdf_url = (
#         f"https://{storage}.blob.core.windows.net/{container}/1431815.pdf?{sas_token}"
#     )

#     await cl.Message(
#         content="Test of PDF in both inline and side:",
#         elements=[
#             cl.Pdf(name="Inline test", display="inline", url=pdf_url, page=1),
#             cl.Pdf(name="Sidebar test", display="side", url=pdf_url, page=1),
#         ],

#     ).send()


# ==================== 🔹 Chainlit Resume & History Handlers 🔹 ====================


# @cl.on_chat_resume
# async def on_chat_resume(thread_id: str):
#     cl.user_session.set("thread_id", thread_id)
#     print(f"🟢 on_chat_resume called with thread_id={thread_id}")
@cl.on_chat_resume
async def on_chat_resume(thread):  # ⛔ no `cl.Thread` type here
    print(f"🔁 Resuming thread: {thread.id}")
    cl.user_session.set("thread", thread)

    messages = await cl.get_messages(thread.id)
    for msg in messages:
        print(f"{msg.author}: {msg.content}")


# @cl.on_chat_load_history
async def load_history(thread_id: str):
    print(f"📜 load_history called with thread_id={thread_id}")
    from chainlit.data import get_data_layer

    data_layer = await get_data_layer()
    steps = await data_layer.list_steps(thread_id)

    for step in steps:
        if step["type"] != "message":
            continue
        await cl.Message(
            content=step["output"] or step["input"] or "",
            author=step["author"]["identifier"],
        ).send()


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
            "últimas noticias",
            "buscar en la web",
            "pronóstico del tiempo",
            "noticias actuales",
            "el clima",
            "internet",
            "búsqueda web",
            "red",
            "navegador",
            "weather",
            "tiempo",
            "search engine",
            "motor de búsqueda",
            "google",
            "wikipedia",
            "tell me about",
            "dime sobre",
            "look up",
            "búscame",
            "find me",
            "encuéntrame",
            "research",
            "investiga",
            "busca en la red",
        ]
    )


@cl.on_message
async def handle_message(message: cl.Message):
    data_layer = get_data_layer()
    user = cl.user_session.get("user")
    print(f"🟠 on_message: user = {user}")

    message.id = message.id or str(uuid.uuid4())

    thread_id = message.thread_id or cl.user_session.get("thread_id")

    print(f"📨 Handling message for thread: {thread_id}")

    if not thread_id:

        thread_id = await data_layer.create_thread(user.identifier)
        cl.user_session.set("thread_id", thread_id)

    message.thread_id = thread_id
    cl.user_session.set("thread_id", thread_id)

    # --- Persist the user's message as a Chainlit step (role=user) ---
    user_identifier = getattr(user, "identifier", None) or "anonymous"
    await data_layer.create_step(
        {
            "threadId": thread_id,
            "id": message.id,  # reuse Chainlit message id if available
            "role": "user",  # ✅ user
            "type": "message",
            "author": {"identifier": user_identifier},  # ✅ NOT 'anonymous'
            "input": message.content,  # user typed text
            "output": message.content,  # ensures it renders correctly
        }
    )

    # 🧩 Check if the user asked for a conversation summary
    if is_summary_request(message.content):
        steps = await data_layer.list_steps(thread_id)
        if not steps:
            await cl.Message(
                content="No hay mensajes previos en esta conversación."
            ).send()
            return
        conversation_text = "\n".join(
            f"{s['role']}: {s.get('output') or s.get('input', '')}" for s in steps
        )
        summary = await summarize_conversation(conversation_text)
        await cl.Message(content=summary).send()
        return

    response_msg = cl.Message(content="")
    await response_msg.send()

    if user and not user.metadata.get("authorized", True):
        await response_msg.stream_token(
            "Oops! It looks like you don’t have access to this service."
        )
        return

        # ✅ SMALL-TALK GUARD — put this BEFORE similarity routing

    def is_smalltalk(text: str) -> bool:
        t = text.lower().strip()
        return (
            any(t.startswith(x) for x in ["hi", "hello", "hey", "hola"])
            or re.match(r"(que|qué)\s*d[ií]a", t)
            or re.match(r"what( is|'s)? the day", t)
        )

    smalltalk = is_smalltalk(message.content)

    # if smalltalk:
    #     sim = 1.0
    #     route_to_web = False
    # else:
    #     sim = await quick_similarity_score(data_layer, message.content)
    #     route_to_web = sim < MIN_SIM_SCORE

    # if route_to_web:
    #     # 🌐 Go straight to web search (Bing-grounded Azure OpenAI)
    #     try:
    #         web_answer = await ask_bing(
    #             markdown_instructions="Responde en el idioma del usuario. Incluye 'Fuentes' con enlaces Markdown.",
    #             user_query=message.content,
    #         )
    #         await response_msg.stream_token(web_answer)
    #         response_msg.content = web_answer
    #         await response_msg.update()
    #     except Exception as e:
    #         await response_msg.stream_token(f"⚠️ Bing Agent error: {e}")
    #     return

    # # 👉 Bing Agent path
    # if is_bing_question(message.content):
    #     response_msg = cl.Message(content="")
    #     await response_msg.send()
    #     try:
    #         completion = await bing_client.chat.completions.create(
    #             model=BING_AGENT_DEPLOYMENT,
    #             messages=[
    #                 {
    #                     "role": "system",
    #                     "content": "You are a helpful Bing search agent.",
    #                 },
    #                 {"role": "user", "content": message.content},
    #             ],
    #         )
    #         answer = completion.choices[0].message.content
    #         await response_msg.stream_token(answer)
    #         response_msg.content = answer
    #         await response_msg.update()
    #     except Exception as e:
    #         await response_msg.stream_token(f"⚠️ Bing Agent error: {e}")
    #     return
    if smalltalk:
        sim = 1.0
        route_to_web = False
    else:
        sim = await quick_similarity_score(data_layer, message.content)
        route_to_web = sim < MIN_SIM_SCORE

    # 🧭 Always handle explicit "live" or time-sensitive questions first
    if is_bing_question(message.content):
        try:
            web_answer = await ask_bing(
                markdown_instructions="Responde con información actualizada del web, incluyendo fuentes.",
                user_query=message.content,
            )
            await response_msg.stream_token(web_answer)
            response_msg.content = web_answer
            await response_msg.update()
        except Exception as e:
            await response_msg.stream_token(f"⚠️ Bing Agent error: {e}")
        return  # ✅ stop here; don't go to orchestrator

    # 🌍 Fallback: route to web if similarity score is low
    if route_to_web:
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
        return

    buffer = ""
    full_text = ""
    references = set()
    auth_info = check_authorization()
    generator = call_orchestrator_stream(thread_id, message.content, auth_info)

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

                extracted_id, cleaned_chunk = extract_thread_id_from_chunk(
                    cleaned_chunk
                )
                if extracted_id:
                    thread_id = extracted_id
                    # Keep Chainlit session aligned with the server-provided conversation id
                    if extracted_id != cl.user_session.get("thread_id"):
                        cl.user_session.set("thread_id", extracted_id)

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

    # full_text = full_text.replace(TERMINATE_TOKEN, "").replace("\\n", "\n")
    # full_text = re.sub(r"(?<=[a-zA-Z])(?=[A-Z])", " ", full_text)

    # 🧼 Clean up special tokens and weird artifacts
    full_text = (
        full_text.replace(TERMINATE_TOKEN, "")
        .replace("QUESTION_ANSWERED", "")
        .replace("Q U E S T I O N_A N S W E R E D", "")
        .replace("\\n", "\n")
    )

    # Fix weird camel-case spacing artifacts
    full_text = re.sub(r"(?<=[a-zA-Z])(?=[A-Z])", " ", full_text)
    full_text = full_text.strip()

    # ⬇️ ONLY run Bing fallback if NOT smalltalk
    if not smalltalk and looks_like_no_answer(full_text):
        try:
            web_answer = await ask_bing(
                markdown_instructions=(
                    "Responde en el idioma del usuario. "
                    "Incluye 'Fuentes' con enlaces Markdown."
                ),
                user_query=message.content,
            )
            await response_msg.stream_token("\n\n---\n\n🌐 Búsqueda en la web:\n")
            await response_msg.stream_token(web_answer)
            full_text = (full_text or "") + "\n\n---\n\n" + web_answer
        except Exception as e:
            await response_msg.stream_token(f"\n\n⚠️ Bing Agent error: {e}")

    message_list = cl.user_session.get("message_list") or []
    message_list.append({"question": message.content, "answer": full_text})
    cl.user_session.set("message_list", message_list)

    logging.info(f"[response message is]: {response_msg}")

    import httpx

    # Transform [filename](url) links to [INLINE_PDF:filename|url] SO THAT CHAINLIT PDF VIEWER CAN PICK THEM UP AND SHOW THEM
    def convert_pdf_links_to_inline_tags(text: str) -> str:
        return re.sub(
            r"\[([^\]]+)\]\((https?://[^\)]+\.pdf)\)",
            r"[INLINE_PDF:\1|\2]",
            text,
            flags=re.IGNORECASE,
        )

    full_text = convert_pdf_links_to_inline_tags(full_text)

    # Handle inline PDF viewer
    inline_pdfs = re.findall(r"\[INLINE_PDF:(.*?)\|(.*?)\]", full_text)

    # Clean up placeholder tags before updating response
    clean_text = re.sub(r"\[INLINE_PDF:.*?\|.*?\]", "", full_text).strip()

    # Update final user message
    response_msg.content = clean_text
    await response_msg.update()

    # Send PDF viewers separately (fetch content first)
    async with httpx.AsyncClient(timeout=30) as client:
        for pdf_name, pdf_url in inline_pdfs:
            logging.info(
                f"📄 Fetching PDF for inline display: {pdf_name} from {pdf_url}"
            )
            try:
                r = await client.get(pdf_url.strip())
                r.raise_for_status()
                pdf_bytes = r.content
                pdf_element = cl.Pdf(
                    name=pdf_name.strip(),
                    display="inline",
                    content=pdf_bytes,
                    page=1,
                )
                await cl.Message(
                    content=f"📄 Documento adjunto: {pdf_name.strip()}",
                    elements=[pdf_element],
                ).send()
            except Exception as e:
                logging.warning(f"⚠️ Failed to fetch or render PDF {pdf_name}: {e}")

    # --- Persist the assistant's reply as a Chainlit step (role=assistant) ---
    await data_layer.create_step(
        {
            "threadId": thread_id,
            "role": "assistant",  # ✅ assistant
            "type": "message",
            "author": {"identifier": "assistant"},
            "input": "",
            "output": full_text,  # streamed final answer
        }
    )

    if full_text and message.content:
        await data_layer.update_thread(
            thread_id,
            name=message.content[:60],
            summary=message.content[:60],
        )

        logging.info("🧵 Thread title updated. Will appear after sidebar refresh.")
        # Close the pool after all DB work is done
    await data_layer.aclose()
