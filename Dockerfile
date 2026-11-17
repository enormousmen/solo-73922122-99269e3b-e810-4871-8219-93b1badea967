FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

EXPOSE 8000

# Single uvicorn process: the connection pool lives in-process and is
# intentionally fixed at two connections (see app/pool.py).
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
