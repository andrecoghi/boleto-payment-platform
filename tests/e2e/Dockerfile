FROM python:3.12-slim

WORKDIR /tests
COPY requirements.txt .
RUN pip install --no-cache-dir --quiet -r requirements.txt
COPY . .

CMD ["pytest", "-v", "--color=yes"]
