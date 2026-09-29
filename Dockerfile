FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
# The campus/proxy TLS root is trusted by the host but not by the minimal
# Python base image. Restrict the bootstrap exception to the two PyPI hosts.
RUN pip install --no-cache-dir --trusted-host pypi.org --trusted-host files.pythonhosted.org -r requirements.txt
COPY src ./src
CMD ["python", "-m", "src.producers.meter_producer"]
