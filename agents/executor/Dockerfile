FROM python:3.11-slim
WORKDIR /app
COPY agents/executor/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shared/ /app/shared/
COPY agents/base_agent.py .
COPY agents/executor/main.py .
CMD ["python", "-u", "main.py"]
