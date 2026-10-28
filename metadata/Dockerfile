FROM python:3.11-slim
WORKDIR /app
COPY metadata/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common ./common
COPY metadata/db.py metadata/main.py ./
EXPOSE 9000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "9000"]