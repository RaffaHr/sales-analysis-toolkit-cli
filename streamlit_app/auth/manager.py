from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import hashlib
import os
import sqlite3
import time


@dataclass(frozen=True)
class User:
    id: int
    username: str
    is_admin: bool


class AuthManager:
    """Controla autenticação básica usando SQLite.

    A implementação usa SHA-256 com salt armazenado junto ao hash. Para produção,
    substitua por um algoritmo dedicado (ex.: argon2 ou bcrypt) e políticas de senha mais rígidas.
    """

    def __init__(self, db_path: Path) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def authenticate(self, username: str, password: str) -> User | None:
        query = "SELECT id, username, password_hash, salt, is_admin FROM users WHERE username = ?"
        with self._connect() as conn:
            row = conn.execute(query, (username.strip().lower(),)).fetchone()
        if row is None:
            return None
        user_id, stored_username, password_hash, salt, is_admin = row
        candidate_hash = self._hash_password(password, salt)
        if password_hash != candidate_hash:
            return None
        return User(id=user_id, username=stored_username, is_admin=bool(is_admin))

    def register_user(self, username: str, password: str, *, is_admin: bool = False) -> User:
        username_normalized = username.strip().lower()
        salt = self._generate_salt()
        password_hash = self._hash_password(password, salt)
        with self._connect() as conn:
            cursor = conn.execute(
                "INSERT INTO users (username, password_hash, salt, is_admin, created_at) VALUES (?, ?, ?, ?, ?)",
                (username_normalized, password_hash, salt, int(is_admin), int(time.time())),
            )
            user_id = cursor.lastrowid
        return User(id=user_id, username=username_normalized, is_admin=is_admin)

    def ensure_default_admin(self, username: str, password: str) -> None:
        username_normalized = username.strip().lower()
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT id FROM users WHERE username = ?",
                (username_normalized,),
            ).fetchone()
            if existing is not None:
                return
            salt = self._generate_salt()
            password_hash = self._hash_password(password, salt)
            conn.execute(
                "INSERT INTO users (username, password_hash, salt, is_admin, created_at) VALUES (?, ?, ?, 1, ?)",
                (username_normalized, password_hash, salt, int(time.time())),
            )

    def list_users(self) -> list[User]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT id, username, is_admin FROM users ORDER BY username"
            ).fetchall()
        return [User(id=row[0], username=row[1], is_admin=bool(row[2])) for row in rows]

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self._db_path, detect_types=sqlite3.PARSE_DECLTYPES)

    def _initialize(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    salt TEXT NOT NULL,
                    is_admin INTEGER NOT NULL DEFAULT 0,
                    created_at INTEGER NOT NULL
                )
                """
            )

    @staticmethod
    def _generate_salt() -> str:
        return hashlib.sha256(os.urandom(32)).hexdigest()

    @staticmethod
    def _hash_password(password: str, salt: str) -> str:
        payload = f"{salt}:{password}".encode("utf-8")
        return hashlib.sha256(payload).hexdigest()
