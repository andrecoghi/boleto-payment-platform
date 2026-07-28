FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --quiet -r requirements.txt
COPY app.py .

EXPOSE 8080
CMD ["python", "app.py"]
