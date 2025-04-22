FROM python:3.9-slim
WORKDIR /app
COPY generator.py .
CMD ["python", "generator.py"]
