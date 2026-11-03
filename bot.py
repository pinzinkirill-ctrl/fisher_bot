"""
Telegram Business Backup Bot.

Python 3.10+, aiogram 3.x, aiosqlite, aiohttp.
Запуск: python bot.py

Перед запуском задайте переменные окружения:
    BOT_TOKEN — токен основного Telegram-бота;
    CRYPTO_PAY_TOKEN — токен приложения Crypto Pay;
    ADMIN_ID — числовой Telegram ID администратора.

Необязательные переменные:
    DB_PATH — путь к SQLite-файлу, по умолчанию bot.db;
    CRYPTO_PAY_API_URL — по умолчанию https://pay.crypt.bot/api.
    Для testnet можно указать https://testnet-pay.crypt.bot/api.
"""

from __future__ import annotations

import asyncio
import logging
import logging.handlers
import os
import queue
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import aiohttp
import aiosqlite
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramAPIError
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.types import (
    BotCommand,
    BusinessConnection,
    BusinessMessagesDeleted,
    CallbackQuery,
    InlineKeyboardMarkup,
    Message,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder


logger = logging.getLogger(__name__)
router = Router()

SUBSCRIPTION_DAYS = 30
SUBSCRIPTION_AMOUNT_RUB = Decimal("99")
MESSAGE_RETENTION_HOURS = 48
CLEANUP_INTERVAL_SECONDS = 24 * 60 * 60


# ---------------------------------------------------------------------------
# Конфигурация и логирование
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Settings:
    """Настройки приложения, прочитанные из переменных окружения."""

    bot_token: str
    crypto_pay_token: str
    admin_id: int
    db_path: str
    crypto_pay_api_url: str

    @classmethod
    def from_env(cls) -> "Settings":
        bot_token = os.getenv("BOT_TOKEN", "").strip()
        crypto_pay_token = os.getenv("CRYPTO_PAY_TOKEN", "").strip()
        admin_id_raw = os.getenv("ADMIN_ID", "").strip()

        missing = [
            name
            for name, value in (
                ("BOT_TOKEN", bot_token),
                ("CRYPTO_PAY_TOKEN", crypto_pay_token),
                ("ADMIN_ID", admin_id_raw),
            )
            if not value
        ]
        if missing:
            raise RuntimeError(
                "Не заданы переменные окружения: " + ", ".join(missing)
            )

        try:
            admin_id = int(admin_id_raw)
        except ValueError as exc:
            raise RuntimeError("ADMIN_ID должен быть числовым Telegram ID") from exc

        return cls(
            bot_token=bot_token,
            crypto_pay_token=crypto_pay_token,
            admin_id=admin_id,
            db_path=os.getenv("DB_PATH", "bot.db").strip() or "bot.db",
            crypto_pay_api_url=os.getenv(
                "CRYPTO_PAY_API_URL", "https://pay.crypt.bot/api"
            ).rstrip("/"),
        )


def setup_async_logging() -> logging.handlers.QueueListener:
    """
    Настраивает неблокирующее логирование INFO через отдельный поток.

    Хэндлеры бота помещают записи в очередь, а QueueListener самостоятельно
    выводит их в stderr, не задерживая сетевые операции на записи логов.
    """

    log_queue: queue.Queue[logging.LogRecord] = queue.Queue()
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )
    )

    listener = logging.handlers.QueueListener(log_queue, console_handler)
    queue_handler = logging.handlers.QueueHandler(log_queue)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(queue_handler)

    listener.start()
    return listener


def utc_now() -> datetime:
    """Возвращает текущее время UTC с явным часовым поясом."""

    return datetime.now(timezone.utc)


def datetime_to_db(value: datetime) -> str:
    """Сериализует время в ISO 8601; все даты в проекте хранятся в UTC."""

    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def datetime_from_db(value: str | None) -> datetime | None:
    if not value:
        return None

    parsed = datetime.fromisoformat(value)
    # На случай ручного редактирования БД считаем дату без зоны датой UTC.
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def shorten(value: Any, limit: int) -> str:
    """Ограничивает пользовательский текст перед отправкой в Telegram."""

    text = str(value or "")
    if len(text) <= limit:
        return text
    return text[: max(1, limit - 1)] + "…"


def display_user(user_id: int, username: str | None = None) -> str:
    return f"@{username} (ID: {user_id})" if username else f"ID: {user_id}"


# ---------------------------------------------------------------------------
# SQLite
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class StoredMessage:
    id: int
    business_connection_id: str
    chat_id: int
    message_id: int
    text: str | None
    file_id: str | None
    file_type: str | None
    user_tg_id: int
    timestamp: int

    @classmethod
    def from_row(cls, row: aiosqlite.Row) -> "StoredMessage":
        return cls(**dict(row))


@dataclass(slots=True)
class BusinessOwner:
    connection_id: str
    owner_id: int
    owner_username: str | None
    owner_first_name: str | None
    user_chat_id: int
    is_enabled: bool

    @classmethod
    def from_row(cls, row: aiosqlite.Row) -> "BusinessOwner":
        return cls(
            connection_id=row["connection_id"],
            owner_id=row["owner_id"],
            owner_username=row["owner_username"],
            owner_first_name=row["owner_first_name"],
            user_chat_id=row["user_chat_id"],
            is_enabled=bool(row["is_enabled"]),
        )


class Database:
    """
    Асинхронная обёртка над SQLite.

    Одно соединение и asyncio.Lock исключают конкурентную запись из хэндлеров
    и фоновой очистки. WAL улучшает чтение во время записи.
    """

    def __init__(self, path: str) -> None:
        self.path = path
        self._connection: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()

    @property
    def connection(self) -> aiosqlite.Connection:
        if self._connection is None:
            raise RuntimeError("База данных ещё не подключена")
        return self._connection

    async def connect(self) -> None:
        if self.path != ":memory:":
            Path(self.path).expanduser().resolve().parent.mkdir(
                parents=True, exist_ok=True
            )

        self._connection = await aiosqlite.connect(self.path)
        self._connection.row_factory = aiosqlite.Row
        await self._connection.execute("PRAGMA journal_mode=WAL")
        await self._connection.execute("PRAGMA foreign_keys=ON")
        await self._connection.execute("PRAGMA busy_timeout=5000")

        await self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS business_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                business_connection_id TEXT NOT NULL,
                chat_id BIGINT NOT NULL,
                message_id INTEGER NOT NULL,
                text TEXT NULL,
                file_id TEXT NULL,
                file_type TEXT NULL,
                user_tg_id BIGINT NOT NULL,
                timestamp INTEGER NOT NULL,
                UNIQUE (business_connection_id, chat_id, message_id)
            );

            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                premium_until DATETIME NULL
            );

            -- Требуемые отдельные индексы по message_id и chat_id.
            CREATE INDEX IF NOT EXISTS idx_business_messages_message_id
                ON business_messages(message_id);
            CREATE INDEX IF NOT EXISTS idx_business_messages_chat_id
                ON business_messages(chat_id);

            -- Составной индекс ускоряет основной поиск конкретного сообщения.
            CREATE INDEX IF NOT EXISTS idx_business_messages_lookup
                ON business_messages(
                    business_connection_id, chat_id, message_id
                );
            CREATE INDEX IF NOT EXISTS idx_business_messages_timestamp
                ON business_messages(timestamp);

            -- Служебная таблица: без неё после перезапуска нельзя узнать,
            -- какому владельцу принадлежит business_connection_id.
            CREATE TABLE IF NOT EXISTS business_connections (
                connection_id TEXT PRIMARY KEY,
                owner_id BIGINT NOT NULL,
                owner_username TEXT NULL,
                owner_first_name TEXT NULL,
                user_chat_id BIGINT NOT NULL,
                is_enabled INTEGER NOT NULL,
                updated_at DATETIME NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_business_connections_owner
                ON business_connections(owner_id);

            -- Оплаченный счёт записывается сюда в той же транзакции, что и
            -- продление. PRIMARY KEY не даёт активировать один счёт дважды.
            CREATE TABLE IF NOT EXISTS paid_invoices (
                invoice_id INTEGER PRIMARY KEY,
                user_id BIGINT NOT NULL,
                paid_at DATETIME NOT NULL
            );
            """
        )
        await self._connection.commit()
        logger.info("SQLite подключена: %s", self.path)

    async def close(self) -> None:
        if self._connection is not None:
            await self._connection.close()
            self._connection = None
            logger.info("SQLite отключена")

    async def ensure_user(self, user_id: int) -> None:
        async with self._lock:
            await self.connection.execute(
                "INSERT OR IGNORE INTO users(user_id) VALUES (?)", (user_id,)
            )
            await self.connection.commit()

    async def upsert_business_connection(
        self, connection: BusinessConnection
    ) -> None:
        async with self._lock:
            await self.connection.execute(
                """
                INSERT INTO business_connections (
                    connection_id, owner_id, owner_username, owner_first_name,
                    user_chat_id, is_enabled, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(connection_id) DO UPDATE SET
                    owner_id = excluded.owner_id,
                    owner_username = excluded.owner_username,
                    owner_first_name = excluded.owner_first_name,
                    user_chat_id = excluded.user_chat_id,
                    is_enabled = excluded.is_enabled,
                    updated_at = excluded.updated_at
                """,
                (
                    connection.id,
                    connection.user.id,
                    connection.user.username,
                    connection.user.first_name,
                    connection.user_chat_id,
                    int(connection.is_enabled),
                    datetime_to_db(utc_now()),
                ),
            )
            await self.connection.commit()

    async def get_business_owner(
        self, connection_id: str
    ) -> BusinessOwner | None:
        async with self._lock:
            cursor = await self.connection.execute(
                """
                SELECT connection_id, owner_id, owner_username,
                       owner_first_name, user_chat_id, is_enabled
                FROM business_connections
                WHERE connection_id = ?
                """,
                (connection_id,),
            )
            row = await cursor.fetchone()
        return BusinessOwner.from_row(row) if row else None

    async def save_message(self, message: Message) -> None:
        """Сохраняет новую версию; UPSERT делает повторную доставку безопасной."""

        text, file_id, file_type = extract_message_content(message)
        sender_id = (
            message.from_user.id if message.from_user else message.chat.id
        )
        timestamp = int(message.date.timestamp())

        async with self._lock:
            await self.connection.execute(
                """
                INSERT INTO business_messages (
                    business_connection_id, chat_id, message_id, text,
                    file_id, file_type, user_tg_id, timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(business_connection_id, chat_id, message_id)
                DO UPDATE SET
                    text = excluded.text,
                    file_id = excluded.file_id,
                    file_type = excluded.file_type,
                    user_tg_id = excluded.user_tg_id,
                    timestamp = excluded.timestamp
                """,
                (
                    message.business_connection_id,
                    message.chat.id,
                    message.message_id,
                    text,
                    file_id,
                    file_type,
                    sender_id,
                    timestamp,
                ),
            )
            await self.connection.commit()

    async def get_message(
        self, connection_id: str, chat_id: int, message_id: int
    ) -> StoredMessage | None:
        async with self._lock:
            cursor = await self.connection.execute(
                """
                SELECT id, business_connection_id, chat_id, message_id,
                       text, file_id, file_type, user_tg_id, timestamp
                FROM business_messages
                WHERE business_connection_id = ?
                  AND chat_id = ?
                  AND message_id = ?
                """,
                (connection_id, chat_id, message_id),
            )
            row = await cursor.fetchone()
        return StoredMessage.from_row(row) if row else None

    async def get_premium_until(self, user_id: int) -> datetime | None:
        async with self._lock:
            cursor = await self.connection.execute(
                "SELECT premium_until FROM users WHERE user_id = ?", (user_id,)
            )
            row = await cursor.fetchone()
        return datetime_from_db(row["premium_until"]) if row else None

    async def has_active_premium(self, user_id: int) -> bool:
        premium_until = await self.get_premium_until(user_id)
        return premium_until is not None and premium_until > utc_now()

    async def activate_premium(
        self, user_id: int, invoice_id: int
    ) -> tuple[bool, datetime]:
        """
        Атомарно продлевает подписку и помечает счёт обработанным.

        Возвращает (True, новая_дата) только при первой активации счёта.
        BEGIN IMMEDIATE заранее берёт блокировку записи SQLite, поэтому два
        одновременных нажатия «Проверить оплату» не продлят срок дважды.
        """

        async with self._lock:
            await self.connection.execute("BEGIN IMMEDIATE")
            try:
                cursor = await self.connection.execute(
                    """
                    SELECT premium_until FROM users WHERE user_id = ?
                    """,
                    (user_id,),
                )
                user_row = await cursor.fetchone()

                cursor = await self.connection.execute(
                    "SELECT 1 FROM paid_invoices WHERE invoice_id = ?",
                    (invoice_id,),
                )
                already_paid = await cursor.fetchone()
                current_until = (
                    datetime_from_db(user_row["premium_until"])
                    if user_row
                    else None
                )

                if already_paid:
                    await self.connection.commit()
                    # Счёт уже обработан; существующая дата должна быть в БД.
                    return False, current_until or utc_now()

                now = utc_now()
                # Если подписка ещё активна, добавляем 30 дней к её окончанию,
                # а не обрезаем уже оплаченный пользователем срок.
                base = (
                    current_until
                    if current_until and current_until > now
                    else now
                )
                new_until = base + timedelta(days=SUBSCRIPTION_DAYS)
                new_until_db = datetime_to_db(new_until)
                paid_at_db = datetime_to_db(now)

                await self.connection.execute(
                    """
                    INSERT INTO users(user_id, premium_until) VALUES (?, ?)
                    ON CONFLICT(user_id) DO UPDATE SET
                        premium_until = excluded.premium_until
                    """,
                    (user_id, new_until_db),
                )
                await self.connection.execute(
                    """
                    INSERT INTO paid_invoices(invoice_id, user_id, paid_at)
                    VALUES (?, ?, ?)
                    """,
                    (invoice_id, user_id, paid_at_db),
                )
                await self.connection.commit()
                return True, new_until
            except Exception:
                await self.connection.rollback()
                raise

    async def cleanup_old_messages(self) -> int:
        """Удаляет только архив сообщений; подписки и связи не затрагиваются."""

        threshold = int(time.time()) - MESSAGE_RETENTION_HOURS * 60 * 60
        async with self._lock:
            cursor = await self.connection.execute(
                "DELETE FROM business_messages WHERE timestamp < ?",
                (threshold,),
            )
            await self.connection.commit()
            return cursor.rowcount

    async def get_stats(self) -> dict[str, int]:
        queries = {
            "users": "SELECT COUNT(*) FROM users",
            "premium_users": """
                SELECT COUNT(*) FROM users
                WHERE premium_until IS NOT NULL
                  AND julianday(premium_until) > julianday('now')
            """,
            "messages": "SELECT COUNT(*) FROM business_messages",
            "connections": """
                SELECT COUNT(*) FROM business_connections WHERE is_enabled = 1
            """,
        }
        result: dict[str, int] = {}
        async with self._lock:
            for name, query in queries.items():
                cursor = await self.connection.execute(query)
                row = await cursor.fetchone()
                result[name] = int(row[0])
        return result


# ---------------------------------------------------------------------------
# Crypto Pay
# ---------------------------------------------------------------------------


class CryptoPayError(Exception):
    """Понятная ошибка интеграции без раскрытия API-токена."""


class CryptoPayClient:
    def __init__(self, token: str, api_url: str) -> None:
        self.api_url = api_url
        self._headers = {"Crypto-Pay-API-Token": token}
        self._session: aiohttp.ClientSession | None = None

    async def start(self) -> None:
        timeout = aiohttp.ClientTimeout(total=30)
        self._session = aiohttp.ClientSession(timeout=timeout)

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def _request(
        self, method: str, parameters: dict[str, Any]
    ) -> Any:
        if self._session is None:
            raise RuntimeError("Crypto Pay client не запущен")

        try:
            async with self._session.post(
                f"{self.api_url}/{method}",
                json=parameters,
                headers=self._headers,
            ) as response:
                try:
                    data = await response.json(content_type=None)
                except (aiohttp.ContentTypeError, ValueError) as exc:
                    raise CryptoPayError(
                        f"Crypto Pay вернул не JSON (HTTP {response.status})"
                    ) from exc

                if response.status != 200 or not data.get("ok"):
                    error = data.get("error", "неизвестная ошибка")
                    raise CryptoPayError(
                        f"Crypto Pay {method}: HTTP {response.status}, {error}"
                    )
                return data.get("result")
        except aiohttp.ClientError as exc:
            raise CryptoPayError(f"Сетевая ошибка Crypto Pay: {exc}") from exc
        except asyncio.TimeoutError as exc:
            raise CryptoPayError("Crypto Pay не ответил за 30 секунд") from exc

    async def create_invoice(self, user_id: int) -> dict[str, Any]:
        """
        Создаёт фиатный счёт на 99 RUB с зачислением USDT.

        payload связывает счёт с Telegram ID; при проверке это значение
        обязательно сверяется повторно, а не доверяется только callback_data.
        """

        invoice = await self._request(
            "createInvoice",
            {
                "asset": "USDT",
                "fiat": "RUB",
                "currency_type": "fiat",
                "amount": str(SUBSCRIPTION_AMOUNT_RUB),
                "description": (
                    f"Подписка Telegram Business Backup на {SUBSCRIPTION_DAYS} дней"
                ),
                "payload": str(user_id),
                "expires_in": 3600,
            },
        )
        if not isinstance(invoice, dict):
            raise CryptoPayError("Crypto Pay вернул некорректный объект счёта")
        if not invoice.get("invoice_id") or not invoice.get("pay_url"):
            raise CryptoPayError("В ответе Crypto Pay нет invoice_id/pay_url")
        return invoice

    async def get_invoice(self, invoice_id: int) -> dict[str, Any] | None:
        result = await self._request(
            "getInvoices", {"invoice_ids": str(invoice_id)}
        )

        # По документации result — массив Invoice. Обработка items оставлена
        # для совместимости с прокси, которые возвращают постраничный объект.
        items = result.get("items", []) if isinstance(result, dict) else result
        if not isinstance(items, list):
            raise CryptoPayError("Crypto Pay вернул некорректный список счетов")

        for invoice in items:
            if int(invoice.get("invoice_id", 0)) == invoice_id:
                return invoice
        return None


# ---------------------------------------------------------------------------
# Работа с содержимым сообщений
# ---------------------------------------------------------------------------


def extract_message_content(
    message: Message,
) -> tuple[str | None, str | None, str | None]:
    """Извлекает текст/подпись и переиспользуемый Telegram file_id."""

    text = message.text or message.caption

    # animation проверяется до document: Telegram заполняет оба поля для GIF.
    if message.animation:
        return text, message.animation.file_id, "animation"
    if message.photo:
        return text, message.photo[-1].file_id, "photo"
    if message.video:
        return text, message.video.file_id, "video"
    if message.document:
        return text, message.document.file_id, "document"
    if message.audio:
        return text, message.audio.file_id, "audio"
    if message.voice:
        return text, message.voice.file_id, "voice"
    if message.video_note:
        return text, message.video_note.file_id, "video_note"
    if message.sticker:
        return text, message.sticker.file_id, "sticker"
    if message.text is not None:
        return message.text, None, "text"

    # Для контента без file_id сохраняем читаемое текстовое представление.
    if message.contact:
        name = " ".join(
            part
            for part in (
                message.contact.first_name,
                message.contact.last_name,
            )
            if part
        )
        return f"Контакт: {name}, {message.contact.phone_number}", None, "contact"
    if message.location:
        return (
            f"Геолокация: {message.location.latitude}, "
            f"{message.location.longitude}",
            None,
            "location",
        )
    if message.dice:
        return (
            f"Dice: {message.dice.emoji}, значение {message.dice.value}",
            None,
            "dice",
        )
    if message.poll:
        return f"Опрос: {message.poll.question}", None, "poll"

    content_type = str(message.content_type)
    return text or f"[{content_type}]", None, content_type


def stored_content_description(record: StoredMessage) -> str:
    if record.text:
        return record.text
    return f"[{record.file_type or 'сообщение без текста'}]"


async def send_archived_content(
    bot: Bot, chat_id: int, record: StoredMessage, header: str
) -> None:
    """Отправляет сохранённый текст либо медиа по ранее сохранённому file_id."""

    if not record.file_id:
        await bot.send_message(
            chat_id,
            f"{header}\n{shorten(stored_content_description(record), 3500)}",
        )
        return

    # Подпись к медиа в Telegram ограничена 1024 символами.
    caption = shorten(record.text, 1024) if record.text else None
    await bot.send_message(chat_id, header)

    if record.file_type == "photo":
        await bot.send_photo(chat_id, record.file_id, caption=caption)
    elif record.file_type == "video":
        await bot.send_video(chat_id, record.file_id, caption=caption)
    elif record.file_type == "document":
        await bot.send_document(chat_id, record.file_id, caption=caption)
    elif record.file_type == "audio":
        await bot.send_audio(chat_id, record.file_id, caption=caption)
    elif record.file_type == "voice":
        await bot.send_voice(chat_id, record.file_id, caption=caption)
    elif record.file_type == "animation":
        await bot.send_animation(chat_id, record.file_id, caption=caption)
    elif record.file_type == "video_note":
        # sendVideoNote не поддерживает caption.
        await bot.send_video_note(chat_id, record.file_id)
    elif record.file_type == "sticker":
        # sendSticker также не поддерживает caption.
        await bot.send_sticker(chat_id, record.file_id)
    else:
        await bot.send_message(
            chat_id,
            f"Тип медиа: {record.file_type}\nfile_id: {record.file_id}",
        )


async def get_authorized_owner(
    db: Database, connection_id: str | None
) -> BusinessOwner | None:
    """Единая провер Business-связи и активной подписки для всех событий."""

    if not connection_id:
        return None

    owner = await db.get_business_owner(connection_id)
    if owner is None:
        logger.warning(
            "Неизвестный business_connection_id=%s; событие пропущено",
            connection_id,
        )
        return None
    if not owner.is_enabled:
        logger.info("Business-связь отключена: %s", connection_id)
        return None
    if not await db.has_active_premium(owner.owner_id):
        logger.info("Подписка владельца %s неактивна", owner.owner_id)
        return None
    return owner


# ---------------------------------------------------------------------------
# Пользовательские команды и оплата
# ---------------------------------------------------------------------------


def payment_keyboard(invoice_id: int, user_id: int, pay_url: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="💳 Оплатить 99 ₽", url=pay_url)
    builder.button(
        text="🔄 Проверить оплату",
        callback_data=f"paycheck:{invoice_id}:{user_id}",
    )
    builder.adjust(1)
    return builder.as_markup()


def start_keyboard(bot_username: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(
        text="💼 Подключить Telegram Business",
        url=f"https://t.me/{bot_username}?startbusiness=connect",
    )
    builder.button(text="💳 Купить подписку", callback_data="buy_subscription")
    builder.adjust(1)
    return builder.as_markup()


@router.message(CommandStart(), F.chat.type == "private")
async def start_handler(message: Message, bot: Bot, db: Database) -> None:
    if message.from_user:
        await db.ensure_user(message.from_user.id)

    me = await bot.get_me()
    await message.answer(
        "👋 Это бот архива Telegram Business.\n\n"
        "После подключения и активации подписки бот сохраняет входящие "
        "сообщения и сообщает вам об их изменении или удалении.\n\n"
        "🔐 Конфиденциальность: записи в базе автоматически удаляются "
        "через 48 часов. Уведомления, уже отправленные в Telegram, и копии "
        "удалённых сообщений у администратора этой очисткой не удаляются. "
        "Используйте архив только с учётом применимых правил и уведомлений "
        "собеседникам.\n\n"
        f"Тариф: {SUBSCRIPTION_AMOUNT_RUB} ₽ / {SUBSCRIPTION_DAYS} дней. "
        "Для работы нажмите /start в личном чате с ботом и подключите его "
        "в настройках Telegram Business.",
        reply_markup=start_keyboard(me.username or ""),
    )


@router.message(Command("pay"), F.chat.type == "private")
async def pay_handler(
    message: Message, crypto: CryptoPayClient, db: Database
) -> None:
    if not message.from_user:
        return

    await db.ensure_user(message.from_user.id)
    try:
        invoice = await crypto.create_invoice(message.from_user.id)
    except CryptoPayError as exc:
        logger.exception("Не удалось создать счёт для %s", message.from_user.id)
        await message.answer(
            "Не удалось создать счёт. Попробуйте позже или напишите /support."
        )
        return

    await message.answer(
        f"Счёт на {SUBSCRIPTION_AMOUNT_RUB} ₽ создан. После оплаты нажмите "
        "«Проверить оплату».",
        reply_markup=payment_keyboard(
            int(invoice["invoice_id"]),
            message.from_user.id,
            str(invoice["pay_url"]),
        ),
    )


@router.callback_query(F.data == "buy_subscription")
async def buy_subscription_callback(
    callback: CallbackQuery, crypto: CryptoPayClient, db: Database
) -> None:
    await callback.answer()
    try:
        invoice = await crypto.create_invoice(callback.from_user.id)
    except CryptoPayError:
        logger.exception(
            "Не удалось создать счёт для %s", callback.from_user.id
        )
        await callback.bot.send_message(
            callback.from_user.id,
            "Не удалось создать счёт. Попробуйте позже или напишите /support.",
        )
        return

    await db.ensure_user(callback.from_user.id)
    await callback.bot.send_message(
        callback.from_user.id,
        f"Счёт на {SUBSCRIPTION_AMOUNT_RUB} ₽ создан. После оплаты нажмите "
        "«Проверить оплату».",
        reply_markup=payment_keyboard(
            int(invoice["invoice_id"]),
            callback.from_user.id,
            str(invoice["pay_url"]),
        ),
    )


@router.callback_query(F.data.startswith("paycheck:"))
async def check_payment_handler(
    callback: CallbackQuery,
    crypto: CryptoPayClient,
    db: Database,
    admin_id: int,
) -> None:
    try:
        _, invoice_id_raw, owner_id_raw = callback.data.split(":", maxsplit=2)
        invoice_id = int(invoice_id_raw)
        invoice_owner_id = int(owner_id_raw)
    except (ValueError, AttributeError):
        await callback.answer("Некорректные данные счёта", show_alert=True)
        return

    # Кнопку из пересланного сообщения не сможет использовать другой человек.
    if callback.from_user.id != invoice_owner_id:
        await callback.answer("Этот счёт принадлежит другому пользователю", show_alert=True)
        return

    await callback.answer("Проверяю платёж…")

    try:
        invoice = await crypto.get_invoice(invoice_id)
    except CryptoPayError:
        logger.exception("Ошибка проверки счёта %s", invoice_id)
        await callback.bot.send_message(
            callback.from_user.id,
            "Не удалось проверить платёж. Попробуйте ещё раз через минуту.",
        )
        return

    if invoice is None:
        await callback.bot.send_message(callback.from_user.id, "Счёт не найден.")
        return

    # Проверяем принадлежность и параметры счёта по ответу самого Crypto Pay.
    if str(invoice.get("payload", "")) != str(callback.from_user.id):
        logger.warning("Счёт %s не принадлежит %s", invoice_id, callback.from_user.id)
        await callback.bot.send_message(callback.from_user.id, "Счёт не принадлежит вам.")
        return

    if invoice.get("status") != "paid":
        status = str(invoice.get("status", "unknown"))
        await callback.bot.send_message(
            callback.from_user.id,
            f"Платёж пока не подтверждён. Текущий статус: {status}.",
        )
        return

    try:
        amount = Decimal(str(invoice.get("amount")))
    except (InvalidOperation, TypeError, ValueError):
        amount = Decimal("0")

    expected_invoice = (
        amount == SUBSCRIPTION_AMOUNT_RUB
        and invoice.get("currency_type") == "fiat"
        and invoice.get("fiat") == "RUB"
        and invoice.get("asset") == "USDT"
    )
    if not expected_invoice:
        logger.error("Счёт %s оплачен, но параметры не совпадают", invoice_id)
        await callback.bot.send_message(
            callback.from_user.id,
            "Параметры счёта требуют ручной проверки. Напишите /support.",
        )
        return

    activated, premium_until = await db.activate_premium(
        callback.from_user.id, invoice_id
    )
    if not activated:
        await callback.bot.send_message(
            callback.from_user.id,
            "Этот счёт уже был использован для активации подписки. "
            f"Подписка действует до {premium_until.strftime('%d.%m.%Y %H:%M UTC')}.",
        )
        return

    await callback.bot.send_message(
        callback.from_user.id,
        f"🎉 Оплата прошла! Подписка активирована на {SUBSCRIPTION_DAYS} дней.\n"
        f"Действует до {premium_until.strftime('%d.%m.%Y %H:%M UTC')}.",
    )

    try:
        await callback.bot.send_message(
            admin_id,
            "💰 Новая оплата подписки\n"
            f"Пользователь: {display_user(callback.from_user.id, callback.from_user.username)}\n"
            f"Счёт: {invoice_id}\n"
            f"Подписка до: {premium_until.strftime('%d.%m.%Y %H:%M UTC')}",
        )
    except TelegramAPIError:
        # Ошибка уведомления админа не отменяет уже подтверждённую оплату.
        logger.exception("Не удалось уведомить администратора об оплате")


@router.message(Command("subscription"), F.chat.type == "private")
async def subscription_handler(message: Message, db: Database) -> None:
    if not message.from_user:
        return

    premium_until = await db.get_premium_until(message.from_user.id)
    if premium_until and premium_until > utc_now():
        await message.answer(
            "✅ Подписка активна до "
            f"{premium_until.strftime('%d.%m.%Y %H:%M UTC')}."
        )
    else:
        await message.answer("Подписка неактивна. Купить её можно командой /pay.")


# ---------------------------------------------------------------------------
# Поддержка и администрирование
# ---------------------------------------------------------------------------


@router.message(Command("support"), F.chat.type == "private")
async def support_handler(
    message: Message,
    command: CommandObject,
    bot: Bot,
    admin_id: int,
) -> None:
    if not message.from_user:
        return
    if not command.args:
        await message.answer("Использование: /support ваш текст обращения")
        return

    try:
        await bot.send_message(
            admin_id,
            "🆘 Обращение в поддержку\n"
            f"От: {display_user(message.from_user.id, message.from_user.username)}\n"
            f"Текст:\n{shorten(command.args, 3500)}",
        )
    except TelegramAPIError:
        logger.exception("Не удалось передать обращение администратору")
        await message.answer("Поддержка временно недоступна. Попробуйте позже.")
        return

    await message.answer("Обращение отправлено администратору.")


@router.message(Command("reply"), F.chat.type == "private")
async def admin_reply_handler(
    message: Message,
    command: CommandObject,
    bot: Bot,
    admin_id: int,
) -> None:
    if not message.from_user or message.from_user.id != admin_id:
        return
    if not command.args:
        await message.answer("Использование: /reply USER_ID текст ответа")
        return

    try:
        user_id_raw, reply_text = command.args.split(maxsplit=1)
        user_id = int(user_id_raw)
    except (ValueError, TypeError):
        await message.answer("Использование: /reply USER_ID текст ответа")
        return

    try:
        await bot.send_message(
            user_id, f"💬 Ответ поддержки:\n{shorten(reply_text, 3500)}"
        )
    except TelegramAPIError:
        logger.exception("Не удалось отправить ответ пользователю %s", user_id)
        await message.answer("Не удалось отправить ответ этому пользователю.")
        return

    await message.answer("Ответ отправлен.")


@router.message(Command("admin"), F.chat.type == "private")
async def admin_handler(message: Message, db: Database, admin_id: int) -> None:
    if not message.from_user or message.from_user.id != admin_id:
        return

    stats = await db.get_stats()
    await message.answer(
        "📊 Статистика бота\n"
        f"Пользователей: {stats['users']}\n"
        f"Активных подписок: {stats['premium_users']}\n"
        f"Активных Business-связей: {stats['connections']}\n"
        f"Сообщений в архиве: {stats['messages']}\n\n"
        "Ответ пользователю: /reply USER_ID текст"
    )


# ---------------------------------------------------------------------------
# Telegram Business
# ---------------------------------------------------------------------------


@router.business_connection()
async def business_connection_handler(
    connection: BusinessConnection, db: Database
) -> None:
    # Telegram присылает это событие при подключении/отключении бота.
    await db.upsert_business_connection(connection)
    logger.info(
        "Business-связь %s: owner=%s enabled=%s",
        connection.id,
        connection.user.id,
        connection.is_enabled,
    )


@router.business_message()
async def business_message_handler(message: Message, db: Database) -> None:
    owner = await get_authorized_owner(db, message.business_connection_id)
    if owner is None:
        return

    # Архивируем именно входящие сообщения собеседника, а не сообщения,
    # которые владелец Business-аккаунта отправил сам.
    if message.from_user and message.from_user.id == owner.owner_id:
        return

    await db.save_message(message)


@router.edited_business_message()
async def edited_business_message_handler(
    message: Message, bot: Bot, db: Database
) -> None:
    owner = await get_authorized_owner(db, message.business_connection_id)
    if owner is None:
        return
    if message.from_user and message.from_user.id == owner.owner_id:
        return

    old = await db.get_message(
        message.business_connection_id or "",
        message.chat.id,
        message.message_id,
    )
    if old is None:
        # Если бот не видел исходное событие, сохраняем текущую версию как базу
        # для следующих изменений, но не выдумываем прежнее содержимое.
        await db.save_message(message)
        return

    new_text, _, new_type = extract_message_content(message)
    new_description = new_text or f"[{new_type or 'сообщение без текста'}]"

    try:
        await bot.send_message(
            owner.user_chat_id,
            "✏️ Изменено!\n"
            f"Чат: {message.chat.id}, сообщение: {message.message_id}\n\n"
            f"Было: {shorten(stored_content_description(old), 1400)}\n\n"
            f"Стало: {shorten(new_description, 1400)}",
        )
    except TelegramAPIError:
        # Не обновляем запись после неудачной отправки: старая версия ещё нужна.
        logger.exception(
            "Не удалось отправить изменение владельцу %s", owner.owner_id
        )
        return

    await db.save_message(message)


@router.deleted_business_messages()
async def deleted_business_messages_handler(
    event: BusinessMessagesDeleted,
    bot: Bot,
    db: Database,
    admin_id: int,
) -> None:
    owner = await get_authorized_owner(db, event.business_connection_id)
    if owner is None:
        return

    for message_id in event.message_ids:
        record = await db.get_message(
            event.business_connection_id, event.chat.id, message_id
        )
        if record is None:
            logger.info(
                "Удалённое сообщение не найдено: connection=%s chat=%s message=%s",
                event.business_connection_id,
                event.chat.id,
                message_id,
            )
            continue

        owner_header = (
            "🗑 Удалено! Оригинал сохранён.\n"
            f"Чат: {event.chat.id}, сообщение: {message_id}"
        )
        try:
            await send_archived_content(
                bot, owner.user_chat_id, record, owner_header
            )
        except TelegramAPIError:
            logger.exception(
                "Не удалось отправить удалённое сообщение владельцу %s",
                owner.owner_id,
            )

        admin_header = (
            "🗑 Копия удалённого сообщения для поддержки\n"
            f"Владелец Business: {display_user(owner.owner_id, owner.owner_username)}\n"
            f"Чат: {event.chat.id}, сообщение: {message_id}"
        )
        try:
            await send_archived_content(bot, admin_id, record, admin_header)
        except TelegramAPIError:
            logger.exception("Не удалось отправить копию администратору")


# ---------------------------------------------------------------------------
# Фоновые задачи и запуск
# ---------------------------------------------------------------------------


async def cleanup_task(db: Database) -> None:
    """Запускает очистку сразу при старте и далее один раз в сутки."""

    while True:
        try:
            deleted = await db.cleanup_old_messages()
            logger.info("Очистка архива завершена: удалено %s записей", deleted)
        except Exception:
            # Ошибка SQLite не должна останавливать фоновую задачу навсегда.
            logger.exception("Ошибка фоновой очистки архива")

        await asyncio.sleep(CLEANUP_INTERVAL_SECONDS)


async def run_bot(settings: Settings) -> None:
    db = Database(settings.db_path)
    crypto = CryptoPayClient(
        settings.crypto_pay_token, settings.crypto_pay_api_url
    )
    bot = Bot(
        token=settings.bot_token,
        default=DefaultBotProperties(parse_mode=None),
    )
    cleanup: asyncio.Task[None] | None = None

    await db.connect()
    await crypto.start()

    try:
        await bot.set_my_commands(
            [
                BotCommand(command="start", description="Подключить бота"),
                BotCommand(command="pay", description="Купить подписку"),
                BotCommand(command="subscription", description="Статус подписки"),
                BotCommand(command="support", description="Написать в поддержку"),
            ]
        )

        dispatcher = Dispatcher()
        dispatcher.include_router(router)
        cleanup = asyncio.create_task(cleanup_task(db))

        logger.info("Бот запущен; используется long polling")
        # dependency injection aiogram передаст db, crypto и admin_id в те
        # хэндлеры, где соответствующие аргументы указаны в сигнатуре.
        await dispatcher.start_polling(
            bot,
            db=db,
            crypto=crypto,
            admin_id=settings.admin_id,
        )
    finally:
        if cleanup is not None:
            cleanup.cancel()
            await asyncio.gather(cleanup, return_exceptions=True)
        await crypto.close()
        await db.close()
        await bot.session.close()


if __name__ == "__main__":
    log_listener = setup_async_logging()
    try:
        asyncio.run(run_bot(Settings.from_env()))
    except KeyboardInterrupt:
        logger.info("Остановка по Ctrl+C")
    finally:
        log_listener.stop()
