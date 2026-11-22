FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src/ ./src/

RUN pip install ".[serve,cassandra]" \
    && pip check \
    && python -c "import featureforge.service_api; print('Service import: OK')"

RUN useradd --create-home --uid 10001 featureforge
USER featureforge

EXPOSE 8080
ENTRYPOINT ["featureforge-serve"]
CMD ["--host", "0.0.0.0", "--port", "8080"]
