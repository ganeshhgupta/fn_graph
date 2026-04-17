FROM python:3.11-slim
WORKDIR /app
COPY fn_graph/examples/deploy/worker/requirements.txt .
RUN pip install -r requirements.txt
COPY fn_graph/ /app/fn_graph/
COPY fn_graph/examples/deploy/worker/server.py .
ENV PYTHONPATH=/app
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000"]
