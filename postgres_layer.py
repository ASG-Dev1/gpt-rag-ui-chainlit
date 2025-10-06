from __future__ import annotations
from azure.search.documents.aio import SearchClient
from azure.core.credentials import AzureKeyCredential
from openai import AsyncAzureOpenAI
import asyncpg
import os, uuid, json
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List
from chainlit.data.base import BaseDataLayer
from chainlit import User
from chainlit.types import PaginatedResponse, PageInfo, Feedback
from chainlit.element import Element


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class PostgresDataLayer(BaseDataLayer):
    """
    Data layer backed by my normalized tables:
      threads(id uuid pk, user_id text, name text, summary text, created_at, updated_at)
      steps(id uuid pk, thread_id uuid fk, role text, type text, author_identifier text,
            input text, output text, created_at, updated_at)
    """

    def __init__(self, dsn: str):
        super().__init__()
        if not dsn:
            raise RuntimeError("POSTGRES_URI or DATABASE_URL is required")
        self._dsn = dsn.replace("postgresql+asyncpg://", "postgresql://")
        self._pool_obj: Optional[asyncpg.Pool] = None
        # 🔹 add Azure Search client
        self._search_client = SearchClient(
            endpoint=os.getenv("AZURE_SEARCH_SERVICE_ENDPOINT"),
            index_name=os.getenv("AZURE_SEARCH_INDEX"),
            credential=AzureKeyCredential(os.getenv("AZURE_SEARCH_API_KEY")),
        )

    async def semantic_search(self, query: str, k: int = 1):
        """
        Perform true vector similarity using Azure OpenAI embeddings + Azure Search.
        """
        # 1) Generate embedding for the query
        aoai = AsyncAzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
            api_version="2024-08-01-preview",
        )
        emb = await aoai.embeddings.create(
            model=os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT"), input=query
        )
        vector = emb.data[0].embedding

        # Field label mapping
        field_labels = {
            "Id_de_Requisicion": "Requisition ID",
            "Numero_de_Caso": "Case Number",
            "Costo_Unitario_Estimado_de_Articulo": "Estimated Unit Cost",
            "Descripcion_de_Articulo": "Description",
            "Marca_de_Articulo": "Brand",
            "Modelo_de_Articulo": "Model",
            "Garantia_de_Articulo": "Warranty",
            "Unidad_de_Medida": "Unit",
            "Cantidad": "Quantity",
            "Fecha_Recibo_de_Requisicion": "Received Date",
            "Numero_de_Requisicion": "Requisition Number",
            "Titulo_de_Requisicion": "Requisition Title",
            "Categoria_de_Requisicion": "Category",
            "SubCategoria_de_Requisicion": "Subcategory",
            "Agencia": "Agency",
            "Nombre_de_Agencia_de_Entrega": "Delivery Agency",
            "Metodo_de_Adquisicion": "Acquisition Method",
            "Costo_Estimado_Total_de_Orden_de_Articulo": "Estimated Total Cost",
            "Numero_de_Contrato": "Contract Number",
            "Costo_Unitario_Final_de_Articulo": "Final Unit Cost",
            "Costo_Final_de_Orden_de_Articulo": "Final Order Cost",
            "Numero_de_Orden_de_Compra": "Purchase Order Number",
            "Nombre_de_Archivo_de_Orden_de_Compra": "Order File Name",
            "Url_de_Archivo_de_Orden_de_Compra": "Order File URL",
            "Nombre_de_Suplidor": "Supplier",
            "Telefono_de_Contacto_de_Suplidor": "Supplier Phone",
            "Email_de_Suplidor": "Supplier Email",
        }
        all_fields = list(field_labels.keys())

        # 2) Query Azure Search with both semantic and index search
        semantic_results = await self._search_client.search(
            search_text=query,
            query_type="semantic",
            semantic_configuration_name="my-semantic-config",
            select=",".join(all_fields),
            top=k,
        )
        index_results = await self._search_client.search(
            search_text=query,
            select=list(field_labels.keys()),
            top=k,
        )

        combined_hits = []

        async for doc in semantic_results:
            combined_hits.append(
                {
                    "source": "semantic",
                    "score": doc["@search.score"],
                    "fields": {
                        label: doc.get(field)
                        for field, label in field_labels.items()
                        if doc.get(field)
                    },
                }
            )

        async for doc in index_results:
            combined_hits.append(
                {
                    "source": "index",
                    "score": doc["@search.score"],
                    "fields": {
                        label: doc.get(field)
                        for field, label in field_labels.items()
                        if doc.get(field)
                    },
                }
            )

        # sort or filter if you like
        combined_hits.sort(key=lambda x: x["score"], reverse=True)
        return combined_hits

    async def _get_pool(self) -> asyncpg.Pool:
        if self._pool_obj is None:
            self._pool_obj = await asyncpg.create_pool(
                self._dsn, min_size=1, max_size=5
            )
        return self._pool_obj

    # ---------- Users ----------
    async def create_user(self, user: User) -> None:
        p = await self._get_pool()
        async with p.acquire() as con:
            await con.execute(
                """
                INSERT INTO users (id, identifier, email, name)
                VALUES ($1,$2,$3,$4)
                ON CONFLICT (identifier) DO UPDATE SET
                email=EXCLUDED.email,
                name=EXCLUDED.name,
                updated_at=now()
                """,
                str(user.id),
                user.identifier,
                user.email,
                user.name,
            )

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
                )
                """
            )
            # make sure identifier is unique (your DB already has this, but keeping it idempotent)
            await con.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS users_identifier_key ON users(identifier)"
            )

            await con.execute(
                """
                INSERT INTO users (id, identifier, email, name, meta)
                VALUES ($1,$2,$3,$4,$5)
                ON CONFLICT (identifier) DO UPDATE SET
                id=EXCLUDED.id,
                email=EXCLUDED.email,
                name=EXCLUDED.name,
                meta=EXCLUDED.meta,
                updated_at=now()
                """,
                user_dict["id"],
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
                "SELECT id, identifier, name, email, meta FROM users WHERE identifier=$1",
                identifier,
            )
            print("Fetched user:", row)
            if not row:
                return None
            u = User(identifier=row["identifier"], name=row["name"], email=row["email"])
            u.id = row["id"]
            return u

    # ---------- Threads / Steps ----------
    async def create_thread(self, user_id: str) -> str:
        p = await self._get_pool()
        thread_id = str(uuid.uuid4())
        print("Creating thread for user_id:", user_id, "with thread_id:", thread_id)
        async with p.acquire() as con:
            await con.execute(
                """
            INSERT INTO threads (id, user_id, name, summary, created_at, updated_at)
            VALUES ($1,$2,'New conversation','New conversation', now(), now())""",
                thread_id,
                user_id,
            )
        return thread_id

    # This function is relied on by the Chainlit sidebar to resume conversations.
    async def get_thread(self, thread_id: str):
        print("Fetching thread:", thread_id)
        p = await self._get_pool()
        async with p.acquire() as con:
            row = await con.fetchrow(
                """
            SELECT id, user_id, name, summary, created_at, updated_at
            FROM threads WHERE id=$1""",
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

    # This function is relied on by the Chainlit sidebar to resume conversations.
    async def append_message(self, thread_id: str, message: Dict[str, Any]):
        print("Appending message to thread:", thread_id)
        role = message.get("role", "user")
        author = message.get("author", {})
        author_id = (
            author.get("identifier") if isinstance(author, dict) else str(author)
        )
        print("Role:", role, "Author ID:", author_id)
        p = await self._get_pool()
        input_text = message.get("input", "")
        output_text = message.get("output", "")
        type_ = message.get("type", "message")
        async with p.acquire() as con:
            await con.execute(
                """
                INSERT INTO threads (id, user_id, name, summary, created_at, updated_at)
                VALUES ($1, $2, 'New conversation', 'New conversation', now(), now())
                ON CONFLICT (id) DO NOTHING;
                -- Only claim authorship for user role, not assistant; ensures only 'user' can claim threads
                UPDATE threads
                SET user_id=$2, updated_at=now()
                WHERE id=$1
                AND role='user'
                AND $2 <> 'anonymous'
                AND (user_id IS NULL OR user_id='anonymous');
                INSERT INTO steps (id, thread_id, role, type, author_identifier, input, output, created_at, updated_at)
                VALUES (gen_random_uuid(), $1, $3, $4, $5, $6, $7, now(), now());
                UPDATE threads SET updated_at=now() WHERE id=$1;
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
                    "UPDATE threads SET name=$1, summary=$2, updated_at=now() WHERE id=$3",
                    name,
                    summary,
                    thread_id,
                )
            elif name is not None:
                await con.execute(
                    "UPDATE threads SET name=$1, updated_at=now() WHERE id=$2",
                    name,
                    thread_id,
                )
            elif summary is not None:
                await con.execute(
                    "UPDATE threads SET summary=$1, updated_at=now() WHERE id=$2",
                    summary,
                    thread_id,
                )

    # This function is relied on by the Chainlit sidebar to resume conversations.
    async def list_threads(self, pagination=None, filters=None):
        user_id = getattr(filters, "userId", None) if filters else None
        p = await self._get_pool()
        async with p.acquire() as con:
            ident_for_convos = None
            if user_id:
                # Chainlit may pass UUID object or UUID-ish string
                uid_str = str(user_id)
                # Only treat as UUID if it has '-' and is longer than 10 chars
                if "-" in uid_str and len(uid_str) > 10:
                    row = await con.fetchrow(
                        "SELECT identifier FROM users WHERE id=$1", uid_str
                    )
                    ident_for_convos = row["identifier"] if row else None
                else:
                    ident_for_convos = uid_str

            if ident_for_convos:
                rows = await con.fetch(
                    """
                    SELECT id, name, summary, created_at, updated_at
                    FROM threads
                    WHERE user_id=$1
                    ORDER BY updated_at DESC
                    """,
                    ident_for_convos,
                )
            else:
                rows = await con.fetch(
                    """
                    SELECT id, name, summary, created_at, updated_at
                    FROM threads
                    ORDER BY updated_at DESC
                    """
                )
        print(
            "Listing threads for user_id:", user_id, "-> identifier:", ident_for_convos
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
        # Validate that thread_id is a valid UUID
        try:
            uuid.UUID(str(thread_id))
        except Exception:
            # Not a valid UUID, do not proceed
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
                INSERT INTO threads (id, user_id, name, summary, created_at, updated_at)
                VALUES ($1, $2, 'New conversation', 'New conversation', now(), now())
                ON CONFLICT (id) DO NOTHING
                """,
                thread_id,
                author_id if role == "user" else "anonymous",
            )
            # If this is a user step from a real user, claim ownership
            if role == "user" and author_id != "anonymous":
                await con.execute(
                    """
                    UPDATE threads
                    SET user_id=$2, updated_at=now()
                    WHERE id=$1
                    AND (user_id IS NULL OR user_id='anonymous')
                    """,
                    thread_id,
                    author_id,
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
                "UPDATE threads SET updated_at=now() WHERE id=$1", thread_id
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
            await con.execute("DELETE FROM threads WHERE id=$1", thread_id)

    async def get_thread_author(self, thread_id: str) -> Optional[str]:
        p = await self._get_pool()
        async with p.acquire() as con:
            row = await con.fetchrow(
                "SELECT user_id FROM threads WHERE id=$1", thread_id
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
        # NEW: close Azure Search client session
        if self._search_client:
            await self._search_client.close()


# Optional hook if you want to reference "postgres_layer:get_data_layer" from config.toml
# def get_data_layer():
#     return PostgresDataLayer(os.getenv("POSTGRES_URI") or os.getenv("DATABASE_URL"))
def get_data_layer(*args, **kwargs):
    print("get_data_layer() called with:", args, kwargs)
    dsn = os.getenv("POSTGRES_URI")
    return PostgresDataLayer(dsn)


async def migrate():
    """
    This function will be called by `chainlit db migrate`.
    It should create required tables if they don't exist.
    """
    dsn = os.getenv("POSTGRES_URI") or os.getenv("DATABASE_URL")
    dl = PostgresDataLayer(dsn)
    p = await dl._get_pool()
    async with p.acquire() as con:
        await con.execute(
            """
        CREATE EXTENSION IF NOT EXISTS "pgcrypto";

        CREATE TABLE IF NOT EXISTS users (
          id TEXT PRIMARY KEY,
          identifier TEXT NOT NULL,
          email TEXT,
          name TEXT,
          meta JSONB,
          created_at TIMESTAMPTZ DEFAULT now(),
          updated_at TIMESTAMPTZ DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS threads (
          id UUID PRIMARY KEY,
          user_id TEXT NOT NULL,
          name TEXT,
          summary TEXT,
          created_at TIMESTAMPTZ DEFAULT now(),
          updated_at TIMESTAMPTZ DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS steps (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          thread_id UUID NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
          role TEXT,
          type TEXT,
          author_identifier TEXT,
          input TEXT,
          output TEXT,
          created_at TIMESTAMPTZ DEFAULT now(),
          updated_at TIMESTAMPTZ DEFAULT now()
        );
        """
        )
