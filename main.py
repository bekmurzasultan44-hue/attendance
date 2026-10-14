"""Быстрая отметка студентов на лекции.
Все данные хранятся в оперативной памяти и пропадают при перезапуске сервера.
Запуск: uvicorn main:app --host 0.0.0.0 --port 8000
"""
import asyncio
import hashlib
import hmac
import re
import secrets
import time
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

BASE_DIR = Path(__file__).parent
templates = Jinja2Templates(directory=BASE_DIR / "templates")

STEP_SECONDS = 10            # период смены кода (сек)
MAX_WRONG = 2                # сколько раз устройство может ошибиться в коде
MAX_FAILS = 40               # общий лимит неверных попыток с одного IP...
FAIL_WINDOW = 60             # ...за это число секунд (защита от обхода)
SECRET = secrets.token_bytes(32)  # новый секрет при каждом запуске


# ---------- Генерация кода ----------
def current_step() -> int:
    return int(time.time() // STEP_SECONDS)


def code_for_step(step: int) -> str:
    digest = hmac.new(SECRET, str(step).encode(), hashlib.sha256).digest()
    return f"{int.from_bytes(digest[:4], 'big') % 10000:04d}"


def normalize(text: str) -> str:
    text = re.sub(r"\s+", " ", text.strip()).casefold()
    return text.replace("ё", "е")


# ---------- Состояние сессии (в памяти) ----------
class Session:
    def __init__(self) -> None:
        self.attendees: list[dict] = []
        self.keys: set[tuple[str, str]] = set()
        self.used_devices: set[str] = set()   # устройства, которые уже отметились
        self.wrong: dict[str, int] = {}       # device_id -> число неверных кодов
        self.sockets: set[WebSocket] = set()
        self.fails: dict[str, list[float]] = {}

    def snapshot(self) -> dict:
        step = current_step()
        return {
            "type": "state",
            "code": code_for_step(step),
            "step_seconds": STEP_SECONDS,
            "next_change_at": (step + 1) * STEP_SECONDS,
            "server_now": time.time(),
            "attendees": self.attendees,
        }

    async def broadcast(self, payload: dict) -> None:
        dead = []
        for ws in list(self.sockets):
            try:
                await ws.send_json(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.sockets.discard(ws)

    def too_many_fails(self, ip: str) -> bool:
        now = time.time()
        recent = [t for t in self.fails.get(ip, []) if now - t < FAIL_WINDOW]
        self.fails[ip] = recent
        return len(recent) >= MAX_FAILS

    def add_fail(self, ip: str) -> None:
        self.fails.setdefault(ip, []).append(time.time())


session = Session()


async def ticker() -> None:
    """Рассылает новый код преподавателю ровно в момент смены интервала."""
    while True:
        next_change = (current_step() + 1) * STEP_SECONDS
        await asyncio.sleep(max(0.0, next_change - time.time()) + 0.05)
        await session.broadcast(session.snapshot())


@asynccontextmanager
async def lifespan(_: FastAPI):
    task = asyncio.create_task(ticker())
    yield
    task.cancel()


app = FastAPI(lifespan=lifespan)


# ---------- Страницы ----------
@app.get("/")
async def index():
    return RedirectResponse("/student")


@app.get("/teacher")
async def teacher_page(request: Request):
    return templates.TemplateResponse(request, "teacher.html")


@app.get("/student")
async def student_page(request: Request):
    return templates.TemplateResponse(request, "student.html")


# ---------- WebSocket для преподавателя ----------
@app.websocket("/ws/teacher")
async def teacher_ws(ws: WebSocket):
    await ws.accept()
    session.sockets.add(ws)
    try:
        await ws.send_json(session.snapshot())
        while True:
            await ws.receive_text()  # держим соединение; "ping" от клиента
    except WebSocketDisconnect:
        pass
    finally:
        session.sockets.discard(ws)


# ---------- API ----------
DEVICE_RE = re.compile(r"[A-Za-z0-9-]{16,64}")


class CheckIn(BaseModel):
    full_name: str
    group: str
    code: str
    device_id: str = ""


def reply(status: int, ok: bool, message: str, device: str | None = None, **extra):
    resp = JSONResponse({"ok": ok, "message": message, **extra}, status_code=status)
    if device:
        resp.set_cookie("device_id", device, max_age=60 * 60 * 24 * 365,
                        samesite="lax")
    return resp


@app.post("/api/checkin")
async def checkin(data: CheckIn, request: Request):
    ip = request.client.host if request.client else "unknown"
    name = re.sub(r"\s+", " ", data.full_name.strip())
    group = re.sub(r"\s+", " ", data.group.strip())
    code = data.code.strip()

    # Идентификатор устройства: из тела запроса и из cookie
    candidates = [data.device_id, request.cookies.get("device_id", "")]
    devices = {d for d in candidates if DEVICE_RE.fullmatch(d or "")}
    if not devices:
        return reply(400, False, "Не удалось определить устройство. Обновите страницу.")
    primary = data.device_id if data.device_id in devices else next(iter(devices))

    if len(name) < 3 or len(name) > 100:
        return reply(400, False, "Введите ФИО полностью.", primary)
    if not group or len(group) > 30:
        return reply(400, False, "Введите номер группы.", primary)
    if not re.fullmatch(r"\d{4}", code):
        return reply(400, False, "Код должен состоять из 4 цифр.", primary)

    # С одного устройства — одна отметка
    if devices & session.used_devices:
        return reply(409, False, "С этого устройства уже была отметка.", primary, locked=True)

    # Устройство исчерпало попытки
    if any(session.wrong.get(d, 0) >= MAX_WRONG for d in devices):
        return reply(403, False, "Попытки закончились. Обратитесь к преподавателю.",
                     primary, locked=True, attempts_left=0)
    if session.too_many_fails(ip):
        return reply(429, False, "Слишком много неверных попыток. Подождите минуту.", primary)

    step = current_step()
    valid = [code_for_step(step), code_for_step(step - 1)]  # текущий и предыдущий
    if not any(hmac.compare_digest(code, v) for v in valid):
        session.add_fail(ip)
        for d in devices:
            session.wrong[d] = session.wrong.get(d, 0) + 1
        left = max(0, MAX_WRONG - session.wrong[primary])
        if left == 0:
            return reply(403, False, "Неверный код. Попытки закончились, обратитесь к преподавателю.",
                         primary, locked=True, attempts_left=0)
        return reply(403, False, f"Неверный или устаревший код. Осталось попыток: {left}.",
                     primary, attempts_left=left)

    key = (normalize(name), normalize(group))
    if key in session.keys:
        return reply(409, False, "Этот студент уже отмечен на паре.", primary)

    entry = {"name": name, "group": group,
             "time": datetime.now().strftime("%H:%M:%S")}
    session.keys.add(key)
    session.used_devices |= devices
    session.attendees.append(entry)
    await session.broadcast({"type": "attendee", "attendee": entry,
                             "total": len(session.attendees)})
    return reply(200, True, f"Готово, {name}! Вы отмечены.", primary, locked=True)


@app.post("/api/reset")
async def reset():
    """Сброс списка (начало новой пары)."""
    session.attendees.clear()
    session.keys.clear()
    session.used_devices.clear()
    session.wrong.clear()
    await session.broadcast(session.snapshot())
    return {"ok": True}
