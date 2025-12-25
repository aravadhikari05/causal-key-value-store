FROM python:3.12-slim
WORKDIR /cse138_assignment3
COPY clock.py .
COPY value.py .
COPY main.py .
COPY requirements.txt .
RUN pip install -r requirements.txt
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8081"]
EXPOSE 8081