# cosmos_layer.py  – Chainlit ≥ 2.5  (async Cosmos SDK)

from __future__ import annotations
import uuid, logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List

from azure.cosmos.aio import CosmosClient
from azure.cosmos import PartitionKey
from azure.cosmos.exceptions import CosmosResourceNotFoundError
from chainlit.data.base import BaseDataLayer
from chainlit import User
from chainlit.types import Pagination, PaginatedResponse, PageInfo, ThreadDict, Feedback
from chainlit.element import Element
import pprint
import json

logging.getLogger().setLevel(logging.DEBUG)
logging.getLogger("azure.identity").setLevel(logging.ERROR)
_DB_NAME = "db0-wvvannyqg5e74"
_CONTAINER_THREADS = "conversations"
_CONTAINER_USERS = "users"


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ───────────────────────────────  Data layer  ────────────────────────────────
class CosmosDataLayer(BaseDataLayer):
    def __init__(
        self,
        *,
        endpoint: str,
        key: str,
        database_name: str = _DB_NAME,
        container_threads: str = _CONTAINER_THREADS,
        container_users: str = _CONTAINER_USERS,
    ):
        super().__init__()
        self._client = CosmosClient(endpoint, credential=key)
        self._db_name = database_name
        self._threads_id = container_threads
        self._users_id = container_users
        self._threads = self._users = None  # lazy

    # ──────────────────────────────  USERS  ──────────────────────────────────
    # async def _get_users(self):
    #     if not self._users:
    #         db = await self._client.create_database_if_not_exists(self._db_name)
    #         self._users = await db.create_container_if_not_exists(
    #             id=self._users_id, partition_key=PartitionKey(path="/id")
    #         )
    #     return self._users

    # async def ensure_user_exists(self, user_dict):
    #     cont = await self._get_users()
    #     try:
    #         await cont.read_item(
    #             item=user_dict["identifier"], partition_key=user_dict["identifier"]
    #         )
    #     except Exception:
    #         await cont.create_item(
    #             {
    #                 "id": user_dict["identifier"],
    #                 "identifier": user_dict["identifier"],
    #                 "email": user_dict["metadata"]["email"],
    #                 "name": user_dict["metadata"]["name"],
    #                 "authorized": user_dict["metadata"].get("authorized", True),
    #                 "chat_profile": user_dict["metadata"].get("chat_profile", "rag"),
    #                 "type": "user",
    #             }
    #         )

    # async def get_user(self, identifier: str) -> Optional[User]:
    #     cont = await self._get_users()
    #     try:
    #         doc = await cont.read_item(item=identifier, partition_key=identifier)
    #     except CosmosResourceNotFoundError:
    #         return None
    #     u = User(
    #         identifier=doc.get("identifier", doc["id"]),
    #         name=doc.get("name", doc["id"]),
    #         email=doc.get("email", doc["id"]),
    #     )
    #     u.id = u.identifier
    #     return u

    # async def create_user(self, user: User) -> None:
    #     cont = await self._get_users()
    #     await cont.upsert_item(
    #         {
    #             "id": user.identifier,
    #             "identifier": user.identifier,
    #             "name": user.name,
    #             "email": user.email,
    #         }
    #     )

    # # ────────────────────────────  THREADS  ──────────────────────────────────
    # async def _get_threads(self):
    #     if not self._threads:
    #         db = await self._client.create_database_if_not_exists(self._db_name)
    #         self._threads = await db.create_container_if_not_exists(
    #             id=self._threads_id, partition_key=PartitionKey(path="/id")
    #         )
    #     return self._threads

    # async def create_thread(self, user_id: str) -> str:
    #     cont = await self._get_threads()
    #     thread_id = str(uuid.uuid4())
    #     await cont.create_item(
    #         {
    #             "id": thread_id,
    #             "user_id": user_id,
    #             "name": "New conversation",
    #             "summary": "New conversation",
    #             "createdAt": _iso_now(),
    #             "updatedAt": _iso_now(),
    #             "messages": [],
    #         }
    #     )
    #     return thread_id

    # async def list_threads(self, pagination: Pagination, filters):
    #     cont = await self._get_threads()
    #     query_str = """
    #     SELECT
    #       c.id,
    #       c.name,
    #       c.summary,
    #       c.createdAt,
    #       c.updatedAt
    #     FROM c
    #     WHERE c.user_id = @uid
    #     ORDER BY c.updatedAt DESC
    #     """
    #     params = [{"name": "@uid", "value": filters.userId}]

    #     items = cont.query_items(
    #         query=query_str,
    #         parameters=params,
    #     )

    #     rows: list[Dict[str, Any]] = []
    #     async for it in items:
    #         rows.append(
    #             {
    #                 "id": it["id"],
    #                 "name": it.get("name", "Untitled Conversation"),
    #                 "summary": it.get("summary", "No summary"),
    #                 "updatedAt": it.get("updatedAt", it["createdAt"]),
    #                 "createdAt": it["createdAt"],
    #             }
    #         )

    #     # ✅ Only log once
    #     if not hasattr(self, "_logged_threads_once") or not self._logged_threads_once:
    #         logging.info(
    #             "📋 list_threads → %d threads for user '%s'", len(rows), filters.userId
    #         )

    #         # ✅ Log all thread names cleanly
    #         thread_names = [row["name"] for row in rows]
    #         logging.info("🧵 Thread names: %s", ", ".join(thread_names))

    #         self._logged_threads_once = True

    #     return PaginatedResponse(
    #         data=rows,
    #         pageInfo=PageInfo(hasNextPage=False, startCursor=None, endCursor=None),
    #     )

    # def sanitize_content(content: str) -> str:
    #     return content.replace("TERMINATE", "").strip(" \n!.")

    # async def list_steps(self, thread_id: str) -> list[dict]:
    #     cont = await self._get_threads()
    #     try:
    #         doc = await cont.read_item(item=thread_id, partition_key=thread_id)
    #     except CosmosResourceNotFoundError:
    #         logging.warning("❌ Thread %s not found", thread_id)
    #         return []

    #     raw_steps = doc.get("steps", [])
    #     logging.info(pprint.pformat(raw_steps, compact=True, width=120))
    #     safe_steps = []
    #     for step in raw_steps:
    #         # This logic ensures Chainlit sees content for both user and assistant
    #         safe_steps.append(
    #             {
    #                 "id": step.get("id", str(uuid.uuid4())),
    #                 "author": step.get("author", {"identifier": "unknown"}),
    #                 "role": step.get("role", "assistant"),
    #                 "type": step.get("type", "message"),
    #                 "input": step.get("input", ""),
    #                 "output": step.get("output", ""),
    #                 "content": (
    #                     step.get("input", "")
    #                     if step.get("role", "assistant") == "user"
    #                     else step.get("output", "")
    #                 ),
    #                 "createdAt": step.get("createdAt", _iso_now()),
    #                 "updatedAt": step.get(
    #                     "updatedAt", step.get("createdAt", _iso_now())
    #                 ),
    #                 "thread_id": thread_id,  # Ensure every step has a thread_id
    #             }
    #         )

    #     logging.info("📨 Loaded %d steps for thread %s", len(safe_steps), thread_id)
    #     return safe_steps

    # async def get_thread(self, thread_id: str) -> ThreadDict | None:
    #     logging.info(f"📥 get_thread() called with id: {thread_id}")
    #     cont = await self._get_threads()
    #     try:
    #         doc = await cont.read_item(item=thread_id, partition_key=thread_id)
    #     except CosmosResourceNotFoundError:
    #         return None
    #     # **ALSO** fetch the steps that belong to this thread
    #     steps = await self.list_steps(thread_id)
    #     # doc["steps"] = steps

    #     logging.info(f"🧵🟡 get_thread({thread_id}) loaded:")
    #     logging.info(pprint.pformat(doc, compact=True, width=120))
    #     # ✅ Ensure every step has a thread_id
    #     for step in steps:
    #         step["thread_id"] = thread_id
    #     # return doc
    #     # ✅ Construct sanitized dictionary
    #     thread_data = {
    #         "id": doc["id"],
    #         "name": doc.get("name", "Untitled"),
    #         "summary": doc.get("summary", ""),
    #         "createdAt": doc.get("createdAt"),
    #         "updatedAt": doc.get("updatedAt"),
    #         "steps": steps,
    #         "user_id": doc.get("user_id"),  # required for sidebar edit/delete
    #     }

    #     logging.info(f"🔁 get_thread returning thread_id: {thread_data['id']}")
    #     logging.info(
    #         f"🔁 get_thread sanitized return: {json.dumps(thread_data, indent=2)}"
    #     )
    #     return thread_data

    # async def append_message(self, thread_id: str, message: Dict[str, Any]):
    #     cont = await self._get_threads()
    #     doc = await cont.read_item(item=thread_id, partition_key=thread_id)

    #     author = message.get("author")
    #     if isinstance(author, dict):
    #         safe_author = author  # assume already in right shape
    #     else:
    #         safe_author = {"identifier": str(author)}

    #     now = _iso_now()
    #     role = message.get("role", "user")
    #     text = message.get("content", "")

    #     step = {
    #         "id": message.get("id", str(uuid.uuid4())),
    #         "role": role,
    #         "author": safe_author,
    #         "type": "message",
    #         # 👇  Chainlit expects these two keys
    #         "input": text if role == "user" else "",
    #         # Always copy the visible content to `output` so the UI can render
    #         "output": text,
    #         "createdAt": now,
    #         "updatedAt": now,
    #     }

    #     doc["messages"] = doc.get("messages", []) + [step]
    #     doc["steps"] = doc.get("steps", []) + [step]

    #     doc["updatedAt"] = now

    #     await cont.replace_item(
    #         item=doc["id"], body=doc, request_options={"partitionKey": doc["id"]}
    #     )
    #     logging.info(f"💾 Appending message to thread {thread_id}:")
    #     pprint(step)

    # async def update_step(self, thread_id=None, step_dict=None):
    #     # Handle Chainlit calling update_step(step_dict)
    #     if isinstance(thread_id, dict) and step_dict is None:
    #         step_dict = thread_id
    #         thread_id = step_dict.get("threadId", "unknown")

    #     if step_dict is None:
    #         logging.warning(
    #             f"update_step called without step_dict (thread_id={thread_id})"
    #         )
    #         return

    #     try:
    #         cont = await self._get_threads()
    #         doc = await cont.read_item(item=thread_id, partition_key=thread_id)
    #     except CosmosResourceNotFoundError:
    #         logging.warning(
    #             f"[update_step] Skipping: Thread {thread_id} does not exist yet."
    #         )
    #         return
    #     except Exception as e:
    #         logging.exception(
    #             f"[update_step] Unexpected error for thread {thread_id}: {e}"
    #         )
    #         return

    #     # Proceed with update
    #     for i, st in enumerate(doc.get("steps", [])):
    #         if st["id"] == step_dict["id"]:
    #             doc["steps"][i].update(step_dict)
    #             break
    #     else:
    #         doc["steps"].append(step_dict)

    #     doc["updatedAt"] = _iso_now()
    #     await cont.replace_item(
    #         item=doc["id"], body=doc, request_options={"partitionKey": doc["id"]}
    #     )

    # async def update_thread(self, thread_id: str, **kwargs):
    #     cont = await self._get_threads()
    #     try:
    #         doc = await cont.read_item(item=thread_id, partition_key=thread_id)
    #     except CosmosResourceNotFoundError:
    #         return

    #     for k in ("name", "summary"):
    #         if k in kwargs:
    #             doc[k] = kwargs[k]

    #     doc["updatedAt"] = _iso_now()

    #     await cont.replace_item(
    #         item=doc["id"], body=doc, request_options={"partitionKey": doc["id"]}
    #     )

    # async def delete_thread(self, thread_id: str):
    #     cont = await self._get_threads()
    #     await cont.delete_item(item=thread_id, partition_key=thread_id)

    # async def get_thread_author(self, thread_id: str) -> Optional[str]:
    #     doc = await self.get_thread(thread_id)
    #     return doc.get("user_id") if doc else None

    # # ────────────────────────  FEEDBACK  ─────────────────────────────────
    # async def upsert_feedback(self, feedback: Feedback) -> str:
    #     """Create or update feedback attached to a step."""
    #     if not feedback.threadId:
    #         return ""

    #     cont = await self._get_threads()
    #     doc = await cont.read_item(item=feedback.threadId, partition_key=feedback.threadId)

    #     fb_id = feedback.id or str(uuid.uuid4())
    #     fb_dict = {
    #         "id": fb_id,
    #         "forId": feedback.forId,
    #         "value": feedback.value,
    #         "comment": feedback.comment,
    #         "createdAt": _iso_now(),
    #         "updatedAt": _iso_now(),
    #     }

    #     feedbacks = doc.get("feedbacks", [])
    #     for i, existing in enumerate(feedbacks):
    #         if existing["id"] == fb_id:
    #             feedbacks[i] = fb_dict
    #             break
    #     else:
    #         feedbacks.append(fb_dict)
    #     doc["feedbacks"] = feedbacks
    #     doc["updatedAt"] = _iso_now()

    #     await cont.replace_item(item=doc["id"], body=doc, request_options={"partitionKey": doc["id"]})
    #     return fb_id

    # async def delete_feedback(self, feedback_id: str) -> bool:
    #     cont = await self._get_threads()
    #     query = (
    #         "SELECT c.id FROM c JOIN f IN c.feedbacks WHERE f.id = @fid"
    #     )
    #     params = [{"name": "@fid", "value": feedback_id}]

    #     thread_id = None
    #     items = cont.query_items(query=query, parameters=params)
    #     async for it in items:
    #         thread_id = it["id"]
    #         break

    #     if not thread_id:
    #         return False

    #     doc = await cont.read_item(item=thread_id, partition_key=thread_id)
    #     before = len(doc.get("feedbacks", []))
    #     doc["feedbacks"] = [f for f in doc.get("feedbacks", []) if f.get("id") != feedback_id]
    #     doc["updatedAt"] = _iso_now()
    #     await cont.replace_item(item=doc["id"], body=doc, request_options={"partitionKey": doc["id"]})
    #     return before != len(doc.get("feedbacks", []))

    # # ────────────────────────  ELEMENTS  ─────────────────────────────────
    # async def create_element(self, element: Element):
    #     cont = await self._get_threads()
    #     thread_id = element.thread_id
    #     doc = await cont.read_item(item=thread_id, partition_key=thread_id)
    #     el_dict = element.to_dict()
    #     elements = doc.get("elements", [])
    #     elements.append(el_dict)
    #     doc["elements"] = elements
    #     doc["updatedAt"] = _iso_now()
    #     await cont.replace_item(item=doc["id"], body=doc, request_options={"partitionKey": doc["id"]})

    # async def get_element(self, thread_id: str, element_id: str) -> Optional[Dict[str, Any]]:
    #     cont = await self._get_threads()
    #     try:
    #         doc = await cont.read_item(item=thread_id, partition_key=thread_id)
    #     except CosmosResourceNotFoundError:
    #         return None
    #     for el in doc.get("elements", []):
    #         if el.get("id") == element_id:
    #             return el
    #     return None

    # async def delete_element(self, element_id: str, thread_id: Optional[str] = None):
    #     cont = await self._get_threads()
    #     if not thread_id:
    #         query = "SELECT c.id FROM c JOIN e IN c.elements WHERE e.id = @eid"
    #         params = [{"name": "@eid", "value": element_id}]
    #         items = cont.query_items(query=query, parameters=params)
    #         async for it in items:
    #             thread_id = it["id"]
    #             break
    #         if not thread_id:
    #             return

    #     doc = await cont.read_item(item=thread_id, partition_key=thread_id)
    #     doc["elements"] = [e for e in doc.get("elements", []) if e.get("id") != element_id]
    #     doc["updatedAt"] = _iso_now()
    #     await cont.replace_item(item=doc["id"], body=doc, request_options={"partitionKey": doc["id"]})

    # # ───────────────────────────  STEPS  ──────────────────────────────────
    # async def create_step(self, step_dict: Dict[str, Any]):
    #     thread_id = step_dict.get("threadId")
    #     if not thread_id:
    #         return
    #     cont = await self._get_threads()
    #     doc = await cont.read_item(item=thread_id, partition_key=thread_id)
    #     step_dict.setdefault("createdAt", _iso_now())
    #     step_dict.setdefault("updatedAt", step_dict["createdAt"])
    #     steps = doc.get("steps", [])
    #     steps.append(step_dict)
    #     doc["steps"] = steps
    #     if step_dict.get("type") == "message":
    #         doc.setdefault("messages", []).append(step_dict)
    #     doc["updatedAt"] = _iso_now()
    #     await cont.replace_item(item=doc["id"], body=doc, request_options={"partitionKey": doc["id"]})

    # async def delete_step(self, step_id: str):
    #     cont = await self._get_threads()
    #     query = "SELECT c.id FROM c JOIN s IN c.steps WHERE s.id = @sid"
    #     params = [{"name": "@sid", "value": step_id}]
    #     items = cont.query_items(query=query, parameters=params)
    #     thread_id = None
    #     async for it in items:
    #         thread_id = it["id"]
    #         break
    #     if not thread_id:
    #         return
    #     doc = await cont.read_item(item=thread_id, partition_key=thread_id)
    #     doc["steps"] = [s for s in doc.get("steps", []) if s.get("id") != step_id]
    #     doc["messages"] = [m for m in doc.get("messages", []) if m.get("id") != step_id]
    #     doc["updatedAt"] = _iso_now()
    #     await cont.replace_item(item=doc["id"], body=doc, request_options={"partitionKey": doc["id"]})

    # async def build_debug_url(self, thread_id: str | None = None) -> str | None:
    #     if not thread_id:
    #         return None
    #     return (
    #         f"https://portal.azure.com/#view/Microsoft_Azure_CosmosDB/"
    #         f"DatabaseId/{self._db_name}/containerId/{self._threads_id}/itemId/{thread_id}"
    #     )

    # async def aclose(self):
    #     """Close the underlying async Cosmos client (called manually or by Chainlit)."""
    #     try:
    #         await self._client.__aexit__(None, None, None)
    #     except AttributeError:

    #         self._client.close()

    # async def get_message_history(self, thread_id: str) -> list[dict]:
    #     cont = await self._get_threads()
    #     try:
    #         doc = await cont.read_item(item=thread_id, partition_key=thread_id)
    #     except CosmosResourceNotFoundError:
    #         return []

    #     messages = doc.get("messages", [])
    #     result = []

    #     for msg in messages:
    #         role = msg.get("role", "assistant")
    #         input_text = msg.get("input", "")
    #         output_text = msg.get("output", "")
    #         content = output_text if role == "assistant" else input_text

    #         result.append(
    #             {
    #                 "id": msg["id"],
    #                 "role": role,
    #                 "type": msg.get("type", "message"),
    #                 "author": msg.get("author", {}),
    #                 "input": input_text,
    #                 "output": output_text,
    #                 "content": content,
    #                 "createdAt": msg.get("createdAt"),
    #                 "updatedAt": msg.get("updatedAt", msg.get("createdAt")),
    #             }
    #         )

    #     return result
    # ---------- Users ----------
    async def ensure_user_exists(self, user_dict):
        p = await self._get_pool()
        async with p.acquire() as con:
            await con.execute(
                """
            CREATE TABLE IF NOT EXISTS users (
              id TEXT PRIMARY KEY,
              identifier TEXT NOT NULL,
              email TEXT,
              name TEXT,
              meta JSONB,
              created_at TIMESTAMPTZ DEFAULT now(),
              updated_at TIMESTAMPTZ DEFAULT now()
            )"""
            )
            await con.execute(
                """
            INSERT INTO users (id, identifier, email, name, meta)
            VALUES ($1,$2,$3,$4,$5)
            ON CONFLICT (id) DO UPDATE SET
              identifier=EXCLUDED.identifier,
              email=EXCLUDED.email,
              name=EXCLUDED.name,
              meta=EXCLUDED.meta,
              updated_at=now()
            """,
                user_dict["identifier"],
                user_dict["identifier"],
                (user_dict.get("metadata") or {}).get("email"),
                (user_dict.get("metadata") or {}).get("name")
                or user_dict["identifier"],
                json.dumps(user_dict.get("metadata") or {}),
            )

    async def get_user(self, identifier: str) -> Optional[User]:
        p = await self._get_pool()
        async with p.acquire() as con:
            row = await con.fetchrow(
                "SELECT identifier, name, email FROM users WHERE id=$1", identifier
            )
            if not row:
                return None
            u = User(identifier=row["identifier"], name=row["name"], email=row["email"])
            u.id = u.identifier
            return u

    async def create_user(self, user: User) -> None:
        p = await self._get_pool()
        async with p.acquire() as con:
            await con.execute(
                """
            INSERT INTO users (id, identifier, email, name)
            VALUES ($1,$2,$3,$4) ON CONFLICT (id) DO NOTHING
            """,
                user.identifier,
                user.identifier,
                user.email,
                user.name,
            )

    # ---------- Threads / Steps ----------
    async def create_thread(self, user_id: str) -> str:
        p = await self._get_pool()
        thread_id = str(uuid.uuid4())
        async with p.acquire() as con:
            await con.execute(
                """
            INSERT INTO conversations (id, user_id, name, summary, created_at, updated_at)
            VALUES ($1,$2,'New conversation','New conversation', now(), now())""",
                thread_id,
                user_id,
            )
        return thread_id

    async def get_thread(self, thread_id: str):
        p = await self._get_pool()
        async with p.acquire() as con:
            row = await con.fetchrow(
                """
            SELECT id, user_id, name, summary, created_at, updated_at
            FROM conversations WHERE id=$1""",
                thread_id,
            )
            if not row:
                return None
            rows = await con.fetch(
                """
            SELECT id, role, type, author_identifier, input, output, created_at, updated_at
            FROM steps WHERE thread_id=$1 ORDER BY created_at""",
                thread_id,
            )
            steps = [
                {
                    "id": str(r["id"]),
                    "role": r["role"],
                    "type": r["type"],
                    "author": {"identifier": r["author_identifier"]},
                    "input": r["input"],
                    "output": r["output"],
                    "createdAt": r["created_at"].isoformat(),
                    "updatedAt": r["updated_at"].isoformat(),
                    "threadId": str(row["id"]),
                }
                for r in rows
            ]
            return {
                "id": str(row["id"]),
                "user_id": row["user_id"],
                "name": row["name"],
                "summary": row["summary"],
                "createdAt": row["created_at"].isoformat(),
                "updatedAt": row["updated_at"].isoformat(),
                "steps": steps,
            }

    async def list_steps(self, thread_id: str) -> List[dict]:
        doc = await self.get_thread(thread_id)
        return doc.get("steps", []) if doc else []

    async def append_message(self, thread_id: str, message: Dict[str, Any]):
        p = await self._get_pool()
        role = message.get("role", "user")
        author = message.get("author", {})
        author_id = (
            author.get("identifier") if isinstance(author, dict) else str(author)
        )
        input_text = message.get("input", "")
        output_text = message.get("output", "")
        type_ = message.get("type", "message")
        async with p.acquire() as con:
            await con.execute(
                """
                INSERT INTO conversations (id, user_id, name, summary, created_at, updated_at)
                VALUES ($1, $2, 'New conversation', 'New conversation', now(), now())
                ON CONFLICT (id) DO NOTHING;
                INSERT INTO steps (id, thread_id, role, type, author_identifier, input, output, created_at, updated_at)
                VALUES (gen_random_uuid(), $1, $3, $4, $5, $6, $7, now(), now());
                UPDATE conversations SET updated_at=now() WHERE id=$1;
                """,
                thread_id,
                author_id if role == "user" else "anonymous",
                role,
                type_,
                author_id,
                input_text,
                output_text,
            )

    async def update_thread(self, thread_id: str, **kwargs):
        name = kwargs.get("name")
        summary = kwargs.get("summary")
        if name is None and summary is None:
            return
        p = await self._get_pool()
        async with p.acquire() as con:
            if name is not None and summary is not None:
                await con.execute(
                    "UPDATE conversations SET name=$1, summary=$2, updated_at=now() WHERE id=$3",
                    name,
                    summary,
                    thread_id,
                )
            elif name is not None:
                await con.execute(
                    "UPDATE conversations SET name=$1, updated_at=now() WHERE id=$2",
                    name,
                    thread_id,
                )
            elif summary is not None:
                await con.execute(
                    "UPDATE conversations SET summary=$1, updated_at=now() WHERE id=$2",
                    summary,
                    thread_id,
                )

    async def list_threads(self, pagination=None, filters=None):
        """
        Return minimal thread summaries for the sidebar.
        """
        user_id = getattr(filters, "userId", None) if filters else None
        p = await self._get_pool()
        async with p.acquire() as con:
            if user_id:
                rows = await con.fetch(
                    """
                    SELECT id, name, summary, created_at, updated_at
                    FROM conversations
                    WHERE user_id=$1
                    ORDER BY updated_at DESC
                    """,
                    user_id,
                )
            else:
                rows = await con.fetch(
                    """
                    SELECT id, name, summary, created_at, updated_at
                    FROM conversations
                    ORDER BY updated_at DESC
                    """
                )
        data = [
            {
                "id": str(r["id"]),
                "name": r["name"] or "Untitled Conversation",
                "summary": r["summary"] or "",
                "createdAt": r["created_at"].isoformat(),
                "updatedAt": r["updated_at"].isoformat(),
            }
            for r in rows
        ]
        return PaginatedResponse(
            data=data,
            pageInfo=PageInfo(hasNextPage=False, startCursor=None, endCursor=None),
        )

    async def create_step(self, step_dict):
        """
        Insert a single step row. If the conversation doesn't exist yet,
        create it so the FK is satisfied.
        """
        thread_id = step_dict.get("threadId") or step_dict.get("thread_id")
        if not thread_id:
            return

        role = step_dict.get("role", "user")
        type_ = step_dict.get("type", "message")
        author_id = (step_dict.get("author") or {}).get("identifier", "anonymous")
        input_text = step_dict.get("input", "")
        output_text = step_dict.get("output", "")
        step_id = step_dict.get("id") or str(uuid.uuid4())

        p = await self._get_pool()
        async with p.acquire() as con:
            # 🔸 unconditionally upsert conversation row to avoid FK races
            await con.execute(
                """
                INSERT INTO conversations (id, user_id, name, summary, created_at, updated_at)
                VALUES ($1, $2, 'New conversation', 'New conversation', now(), now())
                ON CONFLICT (id) DO NOTHING
                """,
                thread_id,
                author_id if role == "user" else "anonymous",
            )
            # insert the step
            await con.execute(
                """
                INSERT INTO steps (id, thread_id, role, type, author_identifier, input, output, created_at, updated_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7, now(), now())
                ON CONFLICT (id) DO NOTHING
                """,
                step_id,
                thread_id,
                role,
                type_,
                author_id,
                input_text,
                output_text,
            )
            await con.execute(
                "UPDATE conversations SET updated_at=now() WHERE id=$1", thread_id
            )

    async def update_step(self, thread_id=None, step_dict=None):
        """
        Update an existing step (by id). Accepts either (thread_id, step_dict) or just a single dict.
        """
        if isinstance(thread_id, dict) and step_dict is None:
            step_dict = thread_id
        if not step_dict:
            return
        step_id = step_dict.get("id")
        if not step_id:
            return
        fields = []
        values = []
        for col_key, db_col in [
            ("role", "role"),
            ("type", "type"),
            ("input", "input"),
            ("output", "output"),
        ]:
            if col_key in step_dict:
                fields.append(f"{db_col}=${len(values)+1}")
                values.append(step_dict[col_key])
        if "author" in step_dict:
            fields.append(f"author_identifier=${len(values)+1}")
            values.append((step_dict["author"] or {}).get("identifier", "anonymous"))
        if not fields:
            return
        p = await self._get_pool()
        async with p.acquire() as con:
            await con.execute(
                f"UPDATE steps SET {', '.join(fields)}, updated_at=now() WHERE id=${len(values)+1}",
                *values,
                step_id,
            )

    async def delete_step(self, step_id: str):
        p = await self._get_pool()
        async with p.acquire() as con:
            await con.execute("DELETE FROM steps WHERE id=$1", step_id)

    async def delete_thread(self, thread_id: str):
        p = await self._get_pool()
        async with p.acquire() as con:
            await con.execute("DELETE FROM conversations WHERE id=$1", thread_id)

    async def get_thread_author(self, thread_id: str) -> Optional[str]:
        p = await self._get_pool()
        async with p.acquire() as con:
            row = await con.fetchrow(
                "SELECT user_id FROM conversations WHERE id=$1", thread_id
            )
            return row["user_id"] if row else None

    # ----- Elements & Feedback (no-ops for now to satisfy BaseDataLayer) -----

    async def create_element(self, element: Element):
        # Not persisted in this app
        return

    async def get_element(self, thread_id: str, element_id: str):
        return None

    async def delete_element(self, element_id: str, thread_id: Optional[str] = None):
        return

    async def upsert_feedback(self, feedback: Feedback) -> str:
        # Return a stable id; not persisted.
        return feedback.id or str(uuid.uuid4())

    async def delete_feedback(self, feedback_id: str) -> bool:
        return False

    def build_debug_url(self, thread_id: Optional[str] = None) -> Optional[str]:
        return None

    async def aclose(self):
        if self._pool_obj:
            await self._pool_obj.close()
            self._pool_obj = None
