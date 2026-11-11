import psutil
import asyncio
import json
from datetime import datetime
from fastapi import FastAPI, WebSocket, Request
import asyncpg
import redis.asyncio as redis
from zoneinfo import ZoneInfo

app = FastAPI()

# Настройки подключения
DB_USER = "admin"
DB_PASS = "secretpassword"
DB_NAME = "monitoring_db"
DB_HOST = "postgres"
REDIS_HOST = "redis"

pool = None
redis_client = None

@app.on_event("startup")
async def startup():
    global pool, redis_client
    # Ждем Postgres
    while True:
        try:
            pool = await asyncpg.create_pool(user=DB_USER, password=DB_PASS, database=DB_NAME, host=DB_HOST, port=5432)
            break
        except Exception:
            await asyncio.sleep(2)
            
    # Ждем Redis
    while True:
        try:
            redis_client = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
            await redis_client.ping()
            break
        except Exception:
            await asyncio.sleep(2)

    # ОЧИЩАЕМ REDIS
    await redis_client.flushall()
    print("Redis очищен!", flush=True)

    # Создаем таблицу
    async with pool.acquire() as connection:
        await connection.execute('''
            CREATE TABLE IF NOT EXISTS file_events (
                id SERIAL PRIMARY KEY,
                event_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                username VARCHAR(100),
                action VARCHAR(20),
                file_path VARCHAR(255),
                size_mb NUMERIC(10, 2), 
                elapsed_ms INTEGER
            )
        ''')
    print("Все базы данных подключены!", flush=True)

@app.post("/webhook")
async def receive_webhook(request: Request):
    data = await request.json()
    username = data.get("username", "Неизвестно")
    action = data.get("action", "unknown").upper()
    file_path = data.get("virtual_path", "Неизвестный файл")
    size_bytes = data.get("file_size", 0)
    elapsed_ms = data.get("elapsed", 0)
    moscow_time = datetime.now(ZoneInfo("Europe/Moscow"))
    current_time = datetime.now().strftime("%H:%M:%S")
    size_kb = size_bytes / 1024
    size_mb = size_kb / 1024
    
    print(f"\n [{current_time}] ДЕЙСТВИЕ: {action}", flush=True)
    print(f"   ├─ Пользователь: {username}", flush=True)
    print(f"   ├─ Файл: {file_path}", flush=True)
    if size_mb >= 1.0:
        print(f"   ├─ Размер: {size_mb:.2f} MB", flush=True)
    else:
        print(f"   ├─ Размер: {size_kb:.2f} KB", flush=True)
    print(f"   └─ Время операции: {elapsed_ms} мс", flush=True)
    
    #  Пишем в историю (Postgres)
    async with pool.acquire() as connection:
        await connection.execute('''
            INSERT INTO file_events (username, action, file_path, size_mb, elapsed_ms)
            VALUES ($1, $2, $3, $4, $5)
        ''', username, action, file_path, round(size_mb, 2), elapsed_ms)
    
    #  Пишем в кэш (Redis) для таблицы
    event_data = {
        "time": current_time,
        "username": username,
        "action": action,
        "file_path": file_path.split("/")[-1],
        "size_mb": round(size_mb, 2)
    }
    await redis_client.lpush("recent_events", json.dumps(event_data))
    await redis_client.ltrim("recent_events", 0, 9)
    
    return {"status": "ok"}

# API для получения событий
@app.get("/api/events")
async def get_events(limit: int = 10):
    if limit <= 10:
        # Берем из быстрого Redis
        events = await redis_client.lrange("recent_events", 0, limit - 1)
        return [json.loads(e) for e in events]
    else:
        # Если просят больше 10 (глубокую историю), идем в Postgres
        async with pool.acquire() as connection:
            rows = await connection.fetch('''
                SELECT username, action, file_path, size_mb, 
                       to_char(event_time, 'HH24:MI:SS') as time 
                FROM file_events ORDER BY id DESC LIMIT $1
            ''', limit)
            # Преобразуем в удобный формат
            return [{"username": r["username"], "action": r["action"], 
                     "file_path": r["file_path"].split("/")[-1], 
                     "size_mb": float(r["size_mb"]), "time": r["time"]} for r in rows]

async def get_network_speed(websocket: WebSocket):
    #  Отправляем историю из Redis при подключении
    history = await redis_client.lrange("graph_history", 0, -1)
    for point in reversed(history):
        await websocket.send_json(json.loads(point))

    #  Шлем текущие данные
    while True:
        net_io_1 = psutil.net_io_counters()
        await asyncio.sleep(1)
        net_io_2 = psutil.net_io_counters()
        
        upload_kb = (net_io_2.bytes_sent - net_io_1.bytes_sent) / 1024
        download_kb = (net_io_2.bytes_recv - net_io_1.bytes_recv) / 1024
        
        if upload_kb < 10: upload_kb = 0.0
        if download_kb < 10: download_kb = 0.0
        
        data = {
            "time": datetime.now().strftime("%H:%M:%S"),
            "upload_kb": round(upload_kb, 2),
            "download_kb": round(download_kb, 2)
        }
        
        # Сохраняем в Redis (храним только 30 последних точек)
        await redis_client.lpush("graph_history", json.dumps(data))
        await redis_client.ltrim("graph_history", 0, 29)
        
        yield data

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    async for data in get_network_speed(websocket):
        try:
            await websocket.send_json(data)
        except Exception:
            break
