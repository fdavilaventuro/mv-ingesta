# Python slim
FROM python:3.11-slim

# Variables de entorno para Python
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8

WORKDIR /app

# Copiar e instalar dependencias mínimas
COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copiar el código
COPY main.py .

# Montaje opcional de credenciales AWS
VOLUME ["/root/.aws"]

# Comando por defecto
CMD ["python", "main.py"]
