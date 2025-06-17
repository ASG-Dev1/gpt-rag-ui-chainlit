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

    async def list_steps(self, thread_id: str) -> list[Dict[str, Any]]:
        cont = await self._get_threads()
        try:
            doc = await cont.read_item(item=thread_id, partition_key=thread_id)
        except CosmosResourceNotFoundError:
            return []

        steps = doc.get("messages", [])
        logging.info(f"📨 Loaded {len(steps)} steps for thread {thread_id}")

        return [
            {
                "id": step["id"],
                "author": (
                    step["author"].get("identifier", "unknown")
                    if isinstance(step["author"], dict)
                    else step["author"]
                ),
                "content": step["content"],
                "createdAt": step.get("createdAt"),
                "updatedAt": step.get("updatedAt", step.get("createdAt")),
                "type": "message",
                "role": step["role"],
            }
            for step in steps
        ]

    async def get_thread(self, thread_id: str) -> ThreadDict | None:
        cont = await self._get_threads()
        try:
            doc = await cont.read_item(item=thread_id, partition_key=thread_id)
        except CosmosResourceNotFoundError:
            return None  # ← CHANGE IS HERE: Do NOT create a new thread!
        # **ALSO** fetch the steps that belong to this thread
        steps = await self.list_steps(thread_id)  # implement this!
        doc["steps"] = steps

        return doc

    async def append_message(self, thread_id: str, message: Dict[str, Any]):
        cont = await self._get_threads()
        doc = await cont.read_item(item=thread_id, partition_key=thread_id)

        doc.setdefault("messages", []).append(
            {
                "id": message.get("id", str(uuid.uuid4())),
                "role": message.get("role", "user"),
                "author": {"identifier": message.get("author", "user")},
                "content": message.get("content", ""),
                "type": "message",  # ✅ Required by Chainlit
                "createdAt": _iso_now(),
            }
        )
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

    async def update_step(self, *a, **kw):
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
