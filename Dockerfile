# 1. Base image with Java 17 for Spark compatibility
FROM openjdk:17-jdk-slim

# 2. Install Python 3 and necessary system dependencies
RUN apt-get update && \
    apt-get install -y python3-venv python3-pip curl && \
    rm -rf /var/lib/apt/lists/*

# 3. Create a non-root user, home directory, and app directory in a single layer
RUN useradd --create-home appuser && \
    mkdir -p /home/appuser/app && \
    chown -R appuser:appuser /home/appuser/app

# 4. Switch to appuser (non-root) and set working directory
USER appuser
WORKDIR /home/appuser/app

# 5. Copy only requirements.txt first (for Docker cache)
COPY --chown=appuser:appuser requirements.txt .

# 6. Create a Python virtual environment as appuser
RUN python3 -m venv venv

# 7. Ensure venv's python/pip are used
ENV PATH="/home/appuser/app/venv/bin:$PATH"

# 8. Install PySpark and other Python dependencies inside the venv
RUN pip install --upgrade pip && \
    pip install pyspark==3.5.0 && \
    pip install --no-cache-dir -r requirements.txt && \
    rm -rf ~/.cache/pip

# 9. Copy application source code and tests into the image (as appuser)
COPY --chown=appuser:appuser src/ ./src
COPY --chown=appuser:appuser tests/ ./tests
COPY --chown=appuser:appuser Makefile pyproject.toml .env ./

# 10. Ensure Python can import from /home/appuser/app
ENV PYTHONPATH=/home/appuser/app

# 11. Default command: run the full pipeline (install, lint, test, run)
CMD ["make", "all"]
