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
from chainlit.types import Pagination, PaginatedResponse, PageInfo, ThreadDict
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
    async def _get_users(self):
        if not self._users:
            db = await self._client.create_database_if_not_exists(self._db_name)
            self._users = await db.create_container_if_not_exists(
                id=self._users_id, partition_key=PartitionKey(path="/id")
            )
        return self._users

    async def ensure_user_exists(self, user_dict):
        cont = await self._get_users()
        try:
            await cont.read_item(
                item=user_dict["identifier"], partition_key=user_dict["identifier"]
            )
        except Exception:
            await cont.create_item(
                {
                    "id": user_dict["identifier"],
                    "identifier": user_dict["identifier"],
                    "email": user_dict["metadata"]["email"],
                    "name": user_dict["metadata"]["name"],
                    "authorized": user_dict["metadata"].get("authorized", True),
                    "chat_profile": user_dict["metadata"].get("chat_profile", "rag"),
                    "type": "user",
                }
            )

    async def get_user(self, identifier: str) -> Optional[User]:
        cont = await self._get_users()
        try:
            doc = await cont.read_item(item=identifier, partition_key=identifier)
        except CosmosResourceNotFoundError:
            return None
        u = User(
            identifier=doc.get("identifier", doc["id"]),
            name=doc.get("name", doc["id"]),
            email=doc.get("email", doc["id"]),
        )
        u.id = u.identifier
        return u

    async def create_user(self, user: User) -> None:
        cont = await self._get_users()
        await cont.upsert_item(
            {
                "id": user.identifier,
                "identifier": user.identifier,
                "name": user.name,
                "email": user.email,
            }
        )

    # ────────────────────────────  THREADS  ──────────────────────────────────
    async def _get_threads(self):
        if not self._threads:
            db = await self._client.create_database_if_not_exists(self._db_name)
            self._threads = await db.create_container_if_not_exists(
                id=self._threads_id, partition_key=PartitionKey(path="/id")
            )
        return self._threads

    async def create_thread(self, user_id: str) -> str:
        cont = await self._get_threads()
        thread_id = str(uuid.uuid4())
        await cont.create_item(
            {
                "id": thread_id,
                "user_id": user_id,
                "name": "New conversation",
                "summary": "New conversation",
                "createdAt": _iso_now(),
                "updatedAt": _iso_now(),
                "messages": [],
            }
        )
        return thread_id

    async def list_threads(self, pagination: Pagination, filters):
        cont = await self._get_threads()
        query_str = """
        SELECT
          c.id,
          c.name,
          c.summary,
          c.createdAt,
          c.updatedAt
        FROM c
        WHERE c.user_id = @uid
        ORDER BY c.updatedAt DESC
        """
        params = [{"name": "@uid", "value": filters.userId}]

        items = cont.query_items(
            query=query_str,
            parameters=params,
        )

        rows: list[Dict[str, Any]] = []
        async for it in items:
            rows.append(
                {
                    "id": it["id"],
                    "name": it.get("name", "Untitled Conversation"),
                    "summary": it.get("summary", "No summary"),
                    "updatedAt": it.get("updatedAt", it["createdAt"]),
                    "createdAt": it["createdAt"],
                }
            )

        # ✅ Only log once
        if not hasattr(self, "_logged_threads_once") or not self._logged_threads_once:
            logging.info(
                "📋 list_threads → %d threads for user '%s'", len(rows), filters.userId
            )

            # ✅ Log all thread names cleanly
            thread_names = [row["name"] for row in rows]
            logging.info("🧵 Thread names: %s", ", ".join(thread_names))

            self._logged_threads_once = True

        return PaginatedResponse(
            data=rows,
            pageInfo=PageInfo(hasNextPage=False, startCursor=None, endCursor=None),
        )

    def sanitize_content(content: str) -> str:
        return content.replace("TERMINATE", "").strip(" \n!.")

    async def list_steps(self, thread_id: str) -> list[dict]:
        cont = await self._get_threads()
        try:
            doc = await cont.read_item(item=thread_id, partition_key=thread_id)
        except CosmosResourceNotFoundError:
            logging.warning("❌ Thread %s not found", thread_id)
            return []

        raw_steps = doc.get("steps", [])
        logging.info(pprint.pformat(raw_steps, compact=True, width=120))
        safe_steps = []
        for step in raw_steps:
            # This logic ensures Chainlit sees content for both user and assistant
            safe_steps.append(
                {
                    "id": step.get("id", str(uuid.uuid4())),
                    "author": step.get("author", {"identifier": "unknown"}),
                    "role": step.get("role", "assistant"),
                    "type": step.get("type", "message"),
                    "input": step.get("input", ""),
                    "output": step.get("output", ""),
                    "content": (
                        step.get("input", "")
                        if step.get("role", "assistant") == "user"
                        else step.get("output", "")
                    ),
                    "createdAt": step.get("createdAt", _iso_now()),
                    "updatedAt": step.get(
                        "updatedAt", step.get("createdAt", _iso_now())
                    ),
                    "thread_id": thread_id,  # Ensure every step has a thread_id
                }
            )

        logging.info("📨 Loaded %d steps for thread %s", len(safe_steps), thread_id)
        return safe_steps

    async def get_thread(self, thread_id: str) -> ThreadDict | None:
        logging.info(f"📥 get_thread() called with id: {thread_id}")
        cont = await self._get_threads()
        try:
            doc = await cont.read_item(item=thread_id, partition_key=thread_id)
        except CosmosResourceNotFoundError:
            return None
        # **ALSO** fetch the steps that belong to this thread
        steps = await self.list_steps(thread_id)
        # doc["steps"] = steps

        logging.info(f"🧵🟡 get_thread({thread_id}) loaded:")
        logging.info(pprint.pformat(doc, compact=True, width=120))
        # ✅ Ensure every step has a thread_id
        for step in steps:
            step["thread_id"] = thread_id
        # return doc
        # ✅ Construct sanitized dictionary
        thread_data = {
            "id": doc["id"],
            "name": doc.get("name", "Untitled"),
            "summary": doc.get("summary", ""),
            "createdAt": doc.get("createdAt"),
            "updatedAt": doc.get("updatedAt"),
            "steps": steps,
            "user_id": doc.get("user_id"),  # required for sidebar edit/delete
        }

        logging.info(f"🔁 get_thread returning thread_id: {thread_data['id']}")
        logging.info(
            f"🔁 get_thread sanitized return: {json.dumps(thread_data, indent=2)}"
        )
        return thread_data

    async def append_message(self, thread_id: str, message: Dict[str, Any]):
        cont = await self._get_threads()
        doc = await cont.read_item(item=thread_id, partition_key=thread_id)

        author = message.get("author")
        if isinstance(author, dict):
            safe_author = author  # assume already in right shape
        else:
            safe_author = {"identifier": str(author)}

        now = _iso_now()
        role = message.get("role", "user")
        text = message.get("content", "")

        step = {
            "id": message.get("id", str(uuid.uuid4())),
            "role": role,
            "author": safe_author,
            "type": "message",
            # 👇  Chainlit expects these two keys
            "input": text if role == "user" else "",
            # Always copy the visible content to `output` so the UI can render
            "output": text,
            "createdAt": now,
            "updatedAt": now,
        }

        doc["messages"] = doc.get("messages", []) + [step]
        doc["steps"] = doc.get("steps", []) + [step]

        doc["updatedAt"] = now

        await cont.replace_item(
            item=doc["id"], body=doc, request_options={"partitionKey": doc["id"]}
        )
        logging.info(f"💾 Appending message to thread {thread_id}:")
        pprint(step)

    async def update_step(self, thread_id=None, step_dict=None):
        # Handle Chainlit calling update_step(step_dict)
        if isinstance(thread_id, dict) and step_dict is None:
            step_dict = thread_id
            thread_id = step_dict.get("threadId", "unknown")

        if step_dict is None:
            logging.warning(
                f"update_step called without step_dict (thread_id={thread_id})"
            )
            return

        try:
            cont = await self._get_threads()
            doc = await cont.read_item(item=thread_id, partition_key=thread_id)
        except CosmosResourceNotFoundError:
            logging.warning(
                f"[update_step] Skipping: Thread {thread_id} does not exist yet."
            )
            return
        except Exception as e:
            logging.exception(
                f"[update_step] Unexpected error for thread {thread_id}: {e}"
            )
            return

        # Proceed with update
        for i, st in enumerate(doc.get("steps", [])):
            if st["id"] == step_dict["id"]:
                doc["steps"][i].update(step_dict)
                break
        else:
            doc["steps"].append(step_dict)

        doc["updatedAt"] = _iso_now()
        await cont.replace_item(
            item=doc["id"], body=doc, request_options={"partitionKey": doc["id"]}
        )

    async def update_thread(self, thread_id: str, **kwargs):
        cont = await self._get_threads()
        try:
            doc = await cont.read_item(item=thread_id, partition_key=thread_id)
        except CosmosResourceNotFoundError:
            return

        for k in ("name", "summary"):
            if k in kwargs:
                doc[k] = kwargs[k]

        doc["updatedAt"] = _iso_now()

        await cont.replace_item(
            item=doc["id"], body=doc, request_options={"partitionKey": doc["id"]}
        )

    async def delete_thread(self, thread_id: str):
        cont = await self._get_threads()
        await cont.delete_item(item=thread_id, partition_key=thread_id)

    async def get_thread_author(self, thread_id: str) -> Optional[str]:
        doc = await self.get_thread(thread_id)
        return doc.get("user_id") if doc else None

    # stubs for feedback / steps / elements …
    async def upsert_feedback(self, *a, **kw):
        pass

    async def delete_feedback(self, *a, **kw):
        pass

    async def create_element(self, *a, **kw):
        pass

    async def get_element(self, *a, **kw):
        return None

    async def delete_element(self, *a, **kw):
        pass

    async def create_step(self, *a, **kw):
        pass

    async def delete_step(self, *a, **kw):
        pass

    async def build_debug_url(self, thread_id: str | None = None) -> str | None:
        if not thread_id:
            return None
        return (
            f"https://portal.azure.com/#view/Microsoft_Azure_CosmosDB/"
            f"DatabaseId/{self._db_name}/containerId/{self._threads_id}/itemId/{thread_id}"
        )

    async def aclose(self):
        """Close the underlying async Cosmos client (called manually or by Chainlit)."""
        try:
            await self._client.__aexit__(None, None, None)
        except AttributeError:

            self._client.close()

    async def get_message_history(self, thread_id: str) -> list[dict]:
        cont = await self._get_threads()
        try:
            doc = await cont.read_item(item=thread_id, partition_key=thread_id)
        except CosmosResourceNotFoundError:
            return []

        messages = doc.get("messages", [])
        result = []

        for msg in messages:
            role = msg.get("role", "assistant")
            input_text = msg.get("input", "")
            output_text = msg.get("output", "")
            content = output_text if role == "assistant" else input_text

            result.append(
                {
                    "id": msg["id"],
                    "role": role,
                    "type": msg.get("type", "message"),
                    "author": msg.get("author", {}),
                    "input": input_text,
                    "output": output_text,
                    "content": content,
                    "createdAt": msg.get("createdAt"),
                    "updatedAt": msg.get("updatedAt", msg.get("createdAt")),
                }
            )

        return result
