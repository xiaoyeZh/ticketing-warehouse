FROM python:3.12.16-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY ticketing ./ticketing
COPY warehouse ./warehouse
COPY migrations ./migrations
COPY seeds ./seeds
COPY tests ./tests
COPY scripts ./scripts
USER 10001:10001
ENTRYPOINT ["python", "-m", "ticketing"]
CMD ["--help"]
