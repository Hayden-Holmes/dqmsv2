# Build wheels with all cloud extras, then copy only the installed packages into a slim runtime.
FROM python:3.12-slim AS build
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir ".[cloud]"

FROM python:3.12-slim
COPY --from=build /usr/local /usr/local
RUN useradd --system --uid 10001 --no-create-home dqms
USER dqms
ENV PORT=8080 DQMS_LOG_LEVEL=INFO PYTHONUNBUFFERED=1
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s \
  CMD python -c "import os,urllib.request as u; u.urlopen(f'http://127.0.0.1:{os.environ[\"PORT\"]}/healthz', timeout=2)"
# Request logging is done by the app (JSON, with request ids), so uvicorn's access log is off.
CMD ["sh", "-c", "exec uvicorn --factory dqms.api.app:app_factory --host 0.0.0.0 --port ${PORT} --no-access-log"]
