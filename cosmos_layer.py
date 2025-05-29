# cosmos_layer.py
import os
import uuid
import datetime as dt
from typing import List, Dict, Any, Optional

from azure.cosmos.aio import CosmosClient
from azure.cosmos import PartitionKey, exceptions as cos_ex

from chainlit.data import BaseDataLayer, Pagination, PaginatedResponse, ThreadDict


class CosmosLayer(BaseDataLayer):
    """
    Data-layer para Chainlit que persiste hilos y mensajes en
    Azure Cosmos DB (SQL API). Compatible con el sidebar de historial.
    """

    # ---------- INIT ------------------------------------------------------
    def __init__(self):
        # 1️⃣ Conexión
        conn_str = os.getenv("COSMOS_CONNECTION_STRING")
        uri = os.getenv("COSMOS_DB_URI")
        key = os.getenv("COSMOS_DB_KEY")

        if conn_str:
            self.client = CosmosClient.from_connection_string(conn_str)
        elif uri and key:
            self.client = CosmosClient(uri, credential=key)
        else:
            raise ValueError(
                "❌ Cosmos creds missing. "
                "Set COSMOS_CONNECTION_STRING or COSMOS_DB_URI + COSMOS_DB_KEY"
            )

        # 2️⃣ DB y contenedor
        db_name = os.getenv("COSMOS_DB_NAME", "ragdb")
        ctr_name = os.getenv("COSMOS_CONTAINER", "conversations")

        self.database = self.client.get_database_client(db_name)
        self.container = self.database.get_container_client(ctr_name)

    # ---------- HELPERS ---------------------------------------------------
    async def _upsert(self, doc: Dict[str, Any]) -> None:
        """
        Up-sert genérico, maneja partition key = conversation_id
        """
        await self.container.upsert_item(doc)

    # ---------- THREADS ---------------------------------------------------
    async def create_thread(self, thread: Dict[str, Any]) -> str:
        """
        Crea un nuevo hilo y devuelve su id.
        Chainlit pasa algo así:
        {
            "id": "",  # puede venir vacío
            "title": "Untitled",
            "user_id": "sub|xyz",
            ...
        }
        """
        thread_id = thread.get("id") or str(uuid.uuid4())
        doc = {
            **thread,
            "id": thread_id,
            "conversation_id": thread_id,  # partition key
            "type": "thread",
            "created_at": dt.datetime.utcnow().isoformat(),
        }
        await self._upsert(doc)
        return thread_id

    async def update_thread(self, thread_id: str, metadata: Dict[str, Any]) -> None:
        """
        Permite cambiar título o marcar como archivado.
        """
        try:
            doc = await self.container.read_item(thread_id, partition_key=thread_id)
            doc.update(metadata)
            await self._upsert(doc)
        except cos_ex.CosmosResourceNotFoundError:
            pass  # silencioso; Chainlit lo ignora

    async def delete_thread(self, thread_id: str) -> None:
        try:
            await self.container.delete_item(thread_id, partition_key=thread_id)
        except cos_ex.CosmosResourceNotFoundError:
            pass

    async def list_threads(self, user_id: str) -> List[Dict[str, Any]]:
        """
        Devuelve todos los hilos del usuario (solo metadatos).
        """
        query = """
        SELECT c.id, c.title, c.created_at
        FROM c
        WHERE c.type = 'thread' AND c.user_id = @uid
        ORDER BY c.created_at DESC
        """
        items = self.container.query_items(
            query=query,
            parameters=[{"name": "@uid", "value": user_id}],
            enable_cross_partition_query=True,
        )
        return [i async for i in items]

    async def read_thread(self, thread_id: str) -> List[Dict[str, Any]]:
        """
        Devuelve todos los documentos (thread + messages) del hilo.
        """
        query = """
        SELECT * FROM c
        WHERE c.conversation_id = @cid
        ORDER BY c.timestamp ASC
        """
        items = self.container.query_items(
            query=query,
            parameters=[{"name": "@cid", "value": thread_id}],
            partition_key=thread_id,
        )
        return [i async for i in items]

    # ---------- MESSAGES --------------------------------------------------
    async def append_message(self, message: Dict[str, Any]) -> None:
        """
        Recibe un dict:
        {
            "id": "uuid-msg",
            "thread_id": "conversation_id",
            "author": "user" | "assistant" | "system",
            "content": "…",
            "timestamp": "2025-05-28T19:25:00Z",
            "tokens": 123
        }
        """
        doc = {
            **message,
            "conversation_id": message["thread_id"],  # partition key
            "type": "message",
        }
        await self._upsert(doc)

    # ---------- NOT USED (pero requeridos por interfaz) -------------------
    async def delete_message(self, message_id: str, thread_id: str) -> None:
        try:
            await self.container.delete_item(message_id, partition_key=thread_id)
        except cos_ex.CosmosResourceNotFoundError:
            pass

    async def update_message(
        self, message_id: str, thread_id: str, metadata: Dict[str, Any]
    ) -> None:
        try:
            doc = await self.container.read_item(message_id, partition_key=thread_id)
            doc.update(metadata)
            await self._upsert(doc)
        except cos_ex.CosmosResourceNotFoundError:
            pass
