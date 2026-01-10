from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json
import sqlite3
import time
from typing import Iterable, Optional


@dataclass(frozen=True)
class Message:
    role: str
    content: str
    created_at: int
    metadata: dict[str, object]


@dataclass(frozen=True)
class Conversation:
    id: int
    user_id: int
    title: str
    created_at: int


class ConversationStore:
    """Gerencia histórico de conversas por usuário usando SQLite."""

    def __init__(self, db_path: Path) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self._db_path, detect_types=sqlite3.PARSE_DECLTYPES)

    def _initialize(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS conversations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    title TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    FOREIGN KEY(user_id) REFERENCES users(id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    conversation_id INTEGER NOT NULL,
                    role TEXT NOT NULL,
                    content TEXT NOT NULL,
                    metadata TEXT,
                    created_at INTEGER NOT NULL,
                    FOREIGN KEY(conversation_id) REFERENCES conversations(id)
                )
                """
            )

    def list_conversations(self, user_id: int) -> list[Conversation]:
        query = "SELECT id, user_id, title, created_at FROM conversations WHERE user_id = ? ORDER BY created_at DESC"
        with self._connect() as conn:
            rows = conn.execute(query, (user_id,)).fetchall()
        return [Conversation(id=row[0], user_id=row[1], title=row[2], created_at=row[3]) for row in rows]

    def create_conversation(self, user_id: int, title: str) -> Conversation:
        with self._connect() as conn:
            cursor = conn.execute(
                "INSERT INTO conversations (user_id, title, created_at) VALUES (?, ?, ?)",
                (user_id, title, int(time.time())),
            )
            conversation_id = cursor.lastrowid
        return Conversation(id=conversation_id, user_id=user_id, title=title, created_at=int(time.time()))

    def get_or_create_default(self, user_id: int) -> Conversation:
        conversations = self.list_conversations(user_id)
        if conversations:
            return conversations[0]
        title = time.strftime("Sessão principal - %d/%m/%Y")
        return self.create_conversation(user_id, title)

    def append_message(
        self,
        conversation_id: int,
        role: str,
        content: str,
        metadata: Optional[dict[str, object]] = None,
    ) -> None:
        serialized = json.dumps(metadata or {}, ensure_ascii=False)
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO messages (conversation_id, role, content, metadata, created_at) VALUES (?, ?, ?, ?, ?)",
                (conversation_id, role, content, serialized, int(time.time())),
            )

    def load_messages(self, conversation_id: int, *, limit: Optional[int] = None) -> list[Message]:
        query = "SELECT role, content, metadata, created_at FROM messages WHERE conversation_id = ? ORDER BY id"
        if limit is not None:
            query += " LIMIT ?"
            params: Iterable[object] = (conversation_id, limit)
        else:
            params = (conversation_id,)
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        payload: list[Message] = []
        for role, content, metadata, created_at in rows:
            metadata_dict: dict[str, object] = {}
            if metadata:
                try:
                    metadata_dict = json.loads(metadata)
                except json.JSONDecodeError:
                    metadata_dict = {"raw": metadata}
            payload.append(Message(role=role, content=content, created_at=created_at, metadata=metadata_dict))
        return payload

    def delete_conversation(self, conversation_id: int) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM messages WHERE conversation_id = ?", (conversation_id,))
            conn.execute("DELETE FROM conversations WHERE id = ?", (conversation_id,))
