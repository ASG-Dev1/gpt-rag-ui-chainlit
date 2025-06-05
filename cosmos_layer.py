# ───────────────────────────────────────────────────────────────
# data_layer/cosmos_data_layer.py  (FULL VERSION)
# ───────────────────────────────────────────────────────────────
"""
Minimal but complete Cosmos DB NoSQL data layer for Chainlit ≥ 2.5.

Implements every abstract hook with either real logic (threads/messages)
or a stub that does nothing but satisfy the ABC.  Enough to unlock:
  • conversation sidebar
  • thread resume
  • basic auth (create/get user)
"""

from __future__ import annotations  # Python <3.12 compatibility

import uuid
from datetime import datetime, timezone
import json
import logging
from typing import Any, Dict, List, Optional

from azure.cosmos.aio import CosmosClient
from azure.cosmos import PartitionKey, exceptions
from azure.cosmos.exceptions import CosmosResourceNotFoundError, CosmosHttpResponseError
from chainlit.types import PageInfo
from chainlit import User

from chainlit.data.base import BaseDataLayer
from chainlit.types import Pagination, PaginatedResponse

# ---------------------------------------------------------------------------
# Helper constants
# ---------------------------------------------------------------------------

_DB_NAME = "db0-wvvannyqg5e74"
_CONTAINER_THREADS = "conversations"
_CONTAINER_USERS = "users"


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------


class CosmosDataLayer(BaseDataLayer):
    def __init__(
        self,
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

        # Lazily created containers
        self._threads = None
        self._users = None

    async def ensure_user_exists(self, user_dict):
        users_container = await self._get_users()
        try:
            await users_container.read_item(
                item=user_dict["identifier"], partition_key=user_dict["identifier"]
            )
        except Exception:
            await users_container.create_item(
                {
                    "id": user_dict["identifier"],
                    "identifier": user_dict["identifier"],  # always add this!
                    "email": user_dict["metadata"]["email"],
                    "client_principal_id": user_dict["metadata"]["client_principal_id"],
                    "client_principal_name": user_dict["metadata"][
                        "client_principal_name"
                    ],
                    "name": user_dict["metadata"]["name"],
                    "authorized": user_dict["metadata"].get("authorized", True),
                    "chat_profile": user_dict["metadata"].get("chat_profile", "rag"),
                    "type": "user",
                }
            )

    async def _get_users(self):
        if not self._users:
            db = await self._client.create_database_if_not_exists(self._db_name)
            self._users = await db.create_container_if_not_exists(
                id=self._users_id,
                partition_key=PartitionKey(path="/id"),
            )
        return self._users

    # ── internal helpers ──────────────────────────────────────────────

    async def _get_threads(self):
        if not self._threads:
            db = await self._client.create_database_if_not_exists(self._db_name)
            self._threads = await db.create_container_if_not_exists(
                id=self._threads_id,
                partition_key=PartitionKey(path="/id"),
            )
        return self._threads

    async def _get_users(self):
        if not self._users:
            db = await self._client.create_database_if_not_exists(self._db_name)
            self._users = await db.create_container_if_not_exists(
                id=self._users_id,
                partition_key=PartitionKey(path="/id"),
            )
        return self._users

    # ── threads & messages (the pieces the sidebar needs) ─────────────

    async def create_thread(self, user_id: str) -> str:
        print(f"🚨 Creating thread for user_id = {user_id}")
        cont = await self._get_threads()
        thread_id = str(uuid.uuid4())
        await cont.create_item(
            {
                "id": thread_id,
                "user_id": user_id,
                "name": "New conversation",
                "messages": [],
                "summary": "New conversation",
                "createdAt": datetime.now(timezone.utc).isoformat(),
            }
        )
        return thread_id

    async def list_threads(self, pagination, filters):
        try:
            container = await self._get_threads()
            query = """
            SELECT c.id, c.name, c.summary, c.createdAt, c.updatedAt
            FROM c
            WHERE c.user_id = @user_id
            ORDER BY c._ts DESC
            """
            params = [{"name": "@user_id", "value": filters.userId}]
            print("🔎 filters.userId =", filters.userId)

            items = container.query_items(query=query, parameters=params)
            results = []

            async for item in items:
                results.append(
                    {
                        "id": item["id"],
                        "name": item.get("name", "Untitled Conversation"),
                        "summary": item.get("summary", "No summary"),
                        "updatedAt": item.get(
                            "updatedAt",
                            datetime.now(timezone.utc).isoformat(),
                        ),
                    }
                )

            return PaginatedResponse(
                data=results,
                pageInfo=PageInfo(hasNextPage=False, startCursor=None, endCursor=None),
            )

        except exceptions.CosmosHttpResponseError as e:
            logging.error(f"[cosmos_layer] Failed to fetch threads: {e}")
            return PaginatedResponse(data=[], pageInfo=PageInfo(False, None, None))

    # async def get_thread(self, thread_id: str) -> Optional[Dict[str, Any]]:
    #     cont = await self._get_threads()
    #     print(f"🔎 get_thread called with id={thread_id}")
    #     try:
    #         item = await cont.read_item(item=thread_id, partition_key=thread_id)
    #         print(f"✅ found: {item}")
    #         return item
    #     except Exception as e:
    #         print(f"❌ NOT found: {thread_id}, error={e}")
    #         return None

    async def get_thread(self, thread_id: str) -> Optional[Dict[str, Any]]:
        cont = await self._get_threads()
        print(f"🔎 get_thread called with id={thread_id}")
        try:
            item = await cont.read_item(item=thread_id, partition_key=thread_id)
            print(f"✅ found: {item}")
            # Strip Cosmos system fields before returning to Chainlit!
            allowed_keys = {
                "id",
                "user_id",
                "user_name",
                "name",
                "summary",
                "createdAt",
                "updatedAt",
                "messages",
            }
            clean_item = {k: v for k, v in item.items() if k in allowed_keys}
            return clean_item
        except Exception as e:
            print(f"❌ NOT found: {thread_id}, error={e}")
            return None

    async def append_message(self, thread_id: str, message: Dict[str, Any]):
        cont = await self._get_threads()
        item = await cont.read_item(thread_id, partition_key=thread_id)
        item.setdefault("messages", [])
        # item["messages"].append(
        #     {
        #         "author": message.get("author", "user"),
        #         "content": message.get("content", ""),
        #         "createdAt": datetime.now(timezone.utc).isoformat(),
        #     }
        # )
        # await cont.replace_item(item=item, body=item)
        item["messages"].append(
            {
                "id": message.get("id", str(uuid.uuid4())),
                "role": message.get("role", "user"),
                "author": {"identifier": message.get("author", "user")},
                "content": message.get("content", ""),
                "createdAt": datetime.now(timezone.utc).isoformat(),
            }
        )
        item["updatedAt"] = datetime.now(timezone.utc).isoformat()
        await cont.replace_item(item=item, body=item)

    async def update_thread(self, thread_id: str, **kwargs):
        print("✅ update_thread CALLED")
        # Called, e.g., when Chainlit stores a summary.
        cont = await self._get_threads()
        item = await cont.read_item(thread_id, partition_key=thread_id)
        allowed_keys = {"name", "summary"}
        for k, v in kwargs.items():
            if k in allowed_keys:
                item[k] = v
            if "updatedAt" not in item or k == "summary":
                item["updatedAt"] = datetime.now(timezone.utc).isoformat()
        print("🔧 kwargs received:", kwargs)
        await cont.replace_item(item=item, body=item)

    async def delete_thread(self, thread_id: str):
        cont = await self._get_threads()
        await cont.delete_item(thread_id, partition_key=thread_id)

    async def get_thread_author(self, thread_id: str) -> Optional[str]:
        thread = await self.get_thread(thread_id)
        return thread.get("user_id") if thread else None

    # ── user management (basic password-auth) ─────────────────────────

    async def get_user(self, identifier: str) -> Optional[User]:
        """
        Chainlit calls this to load a user by its identifier.
        Since our container is partitioned on /id, we can just read_item by id.
        """
        users_container = await self._get_users()
        try:
            user_doc = await users_container.read_item(
                item=identifier, partition_key=identifier
            )
        except CosmosResourceNotFoundError:
            return None

        # # Build a chainlit.User and then set `user.id = identifier`
        # u = User(
        #     identifier=user_doc.get("identifier"),
        #     name=user_doc.get("username", user_doc.get("identifier")),
        #     email=user_doc.get("email"),
        # )
        # Defensive: fallback to id if identifier is missing
        identifier_val = user_doc.get("identifier", user_doc.get("id"))
        name_val = user_doc.get("name", identifier_val)
        email_val = user_doc.get("email", identifier_val)

        u = User(
            identifier=identifier_val,
            name=name_val,
            email=email_val,
        )
        u.id = identifier_val
        # Attach the `id` attribute that Chainlit’s sidebar/resume logic needs:
        u.id = identifier
        return u

    async def create_user(self, user: User) -> None:
        """
        Chainlit calls this when a new user signs up (or first appears).
        We should write the User data into Cosmos so that future get_user(...) can find it.
        """
        # users_container = await (await self._get_users())
        users_container = await self._get_users()
        user_doc = {
            "id": user.identifier,  # Cosmos’ “id” field can match Chainlit’s identifier
            "identifier": user.identifier,
            "name": user.name,
            "email": user.email,
            # store any extra fields you want (e.g. picture, metadata…)
        }
        await users_container.upsert_item(body=user_doc)

    # ── feedback API (stubs) ──────────────────────────────────────────

    async def upsert_feedback(self, *args, **kwargs):
        pass

    async def delete_feedback(self, *args, **kwargs):
        pass

    # ── elements / steps (stubs for now) ──────────────────────────────

    async def create_element(self, *args, **kwargs):
        pass

    async def get_element(self, *args, **kwargs):
        return None

    async def delete_element(self, *args, **kwargs):
        pass

    async def create_step(self, *args, **kwargs):
        pass

    async def update_step(self, *args, **kwargs):
        pass

    async def delete_step(self, *args, **kwargs):
        pass

    # ── misc utilities (stub) ─────────────────────────────────────────

    async def build_debug_url(self, thread_id: str | None = None) -> str | None:
        """
        Return a URL where someone can inspect the 'thread_id' document
        in Azure Cosmos DB Explorer (or similar). If no thread_id is given,
        return a general link to the Cosmos container.
        """
        # For example, if you want a per-thread URL, you could do:
        if thread_id:
            return (
                f"https://portal.azure.com/#view/"
                f"Microsoft_Azure_CosmosDB/DatabaseId/{self._db_name}"
                f"/containerId/{self._threads_id}"
                f"/itemId/{thread_id}"
            )
        else:
            # Fallback to the container’s “browse” page (or just return None)
            return None
