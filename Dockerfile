FROM python:3.11-slim
WORKDIR /workspace
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV PYTHONPATH=/workspace/src
CMD ["uvicorn", "src.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
