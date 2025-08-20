from __future__ import annotations
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
      conversations(id uuid pk, user_id text, name text, summary text, created_at, updated_at)
      steps(id uuid pk, conversation_id uuid fk, role text, type text, author_identifier text,
            input text, output text, created_at, updated_at)
    """

    def __init__(self, dsn: str):
        super().__init__()
        if not dsn:
            raise RuntimeError("POSTGRES_URI or DATABASE_URL is required")
        self._dsn = dsn.replace("postgresql+asyncpg://", "postgresql://")
        self._pool_obj: Optional[asyncpg.Pool] = None

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
            if not row:
                return None
            u = User(identifier=row["identifier"], name=row["name"], email=row["email"])
            u.id = row["id"]
            return u

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
            FROM steps WHERE conversation_id=$1 ORDER BY created_at""",
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
                UPDATE conversations
                SET user_id=$2, updated_at=now()
                WHERE id=$1
                AND $3='user'
                AND $2 <> 'anonymous'
                AND (user_id IS NULL OR user_id='anonymous');
                INSERT INTO steps (id, conversation_id, role, type, author_identifier, input, output, created_at, updated_at)
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
        user_id = getattr(filters, "userId", None) if filters else None
        p = await self._get_pool()
        async with p.acquire() as con:
            ident_for_convos = None
            if user_id:
                # Chainlit may pass UUID object or UUID-ish string
                uid_str = str(user_id)
                if "-" in uid_str:  # looks like a UUID
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
                    FROM conversations
                    WHERE user_id=$1
                    ORDER BY updated_at DESC
                    """,
                    ident_for_convos,
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
        thread_id = step_dict.get("threadId") or step_dict.get("conversation_id")
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
            # If this is a user step from a real user, claim ownership
            if role == "user" and author_id != "anonymous":
                await con.execute(
                    """
                    UPDATE conversations
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
                INSERT INTO steps (id, conversation_id, role, type, author_identifier, input, output, created_at, updated_at)
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

        CREATE TABLE IF NOT EXISTS conversations (
          id UUID PRIMARY KEY,
          user_id TEXT NOT NULL,
          name TEXT,
          summary TEXT,
          created_at TIMESTAMPTZ DEFAULT now(),
          updated_at TIMESTAMPTZ DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS steps (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
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
