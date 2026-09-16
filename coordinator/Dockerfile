FROM python:3.11-slim
WORKDIR /app
COPY coordinator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shared/ /app/shared/
COPY coordinator/main.py .
CMD ["python", "-u", "main.py"]
