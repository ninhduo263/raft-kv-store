FROM python:3.12-slim

WORKDIR /app
COPY node.py .

ENV PYTHONUNBUFFERED=1
EXPOSE 8000

CMD ["python", "node.py"]
