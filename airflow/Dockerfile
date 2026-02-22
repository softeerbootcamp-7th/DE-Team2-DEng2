FROM apache/airflow:2.10.5-python3.12

USER root

# --------------------------------------------------
# 1. 시스템 패키지 + Chromium + Driver
# --------------------------------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    gnupg \
    && install -m 0755 -d /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
       https://download.docker.com/linux/debian $(. /etc/os-release && echo $VERSION_CODENAME) stable" \
       > /etc/apt/sources.list.d/docker.list \
    && apt-get update && apt-get install -y --no-install-recommends \
    wget \
    unzip \
    docker-ce-cli \
    fonts-liberation \
    chromium \
    chromium-driver \
    libgbm1 \
    libnss3 \
    libatk-bridge2.0-0 \
    libgtk-3-0 \
    libasound2 \
    libx11-xcb1 \
    libxcomposite1 \
    libxcursor1 \
    libxdamage1 \
    libxi6 \
    libxtst6 \
    libglib2.0-0 \
    libdrm2 \
    libxrandr2 \
    xdg-utils \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# --------------------------------------------------
# 2. Python 패키지
# --------------------------------------------------
RUN pip install --no-cache-dir \
    pandas \
    playwright \
    pyproj \
    python-dotenv \
    sqlalchemy \
    alembic \
    openpyxl \
    selenium \
    webdriver-manager \
    blinker==1.6.3 \
    selenium-wire

# --------------------------------------------------
# 3. Playwright 브라우저 설치
# --------------------------------------------------
RUN playwright install chromium
