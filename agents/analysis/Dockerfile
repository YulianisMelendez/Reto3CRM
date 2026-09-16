FROM python:3.11-slim
WORKDIR /app
COPY agents/analysis/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shared/ /app/shared/
COPY agents/base_agent.py .
COPY agents/analysis/main.py .
CMD ["python", "-u", "main.py"]
