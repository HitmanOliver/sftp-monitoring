# Берем официальный легковесный образ Python
FROM python:3.11-slim

# Устанавливаем рабочую директорию внутри контейнера
WORKDIR /app

# Копируем файл с зависимостями и устанавливаем их
COPY requirements.txt .
RUN pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple --trusted-host pypi.tuna.tsinghua.edu.cn --default-timeout=300 -r requirements.txt

# Копируем наш скрипт в контейнер
COPY monitor.py .

# Команда, которая запустится при старте контейнера
CMD ["uvicorn", "monitor:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
