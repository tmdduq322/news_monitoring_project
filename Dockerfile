FROM apache/airflow:2.7.3-python3.10

# 1. Root 권한으로 시스템 패키지 설치
USER root

# 2. 필수 시스템 패키지 설치
# --fix-missing 옵션을 추가하여 다운로드 실패 시 재시도하거나 오류를 방지합니다.
RUN apt-get update && \
    apt-get install -y --no-install-recommends --fix-missing \
    wget \
    gnupg \
    unzip \
    default-jdk \
    default-libmysqlclient-dev \
    build-essential \
    pkg-config && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 3. Chrome 브라우저 설치
# [핵심 수정] 여기에도 --fix-missing을 추가하여 의존성 설치 중 끊김을 방지합니다.
RUN apt-get update && \
    wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    apt-get install -y --fix-missing ./google-chrome-stable_current_amd64.deb && \
    ln -sf /usr/bin/google-chrome /usr/bin/chromium && \
    rm google-chrome-stable_current_amd64.deb && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 4. JAVA_HOME 환경변수 설정
ENV JAVA_HOME /usr/lib/jvm/default-java

# 5. Airflow 유저로 전환하여 Python 패키지 설치
USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --user --no-cache-dir -r /requirements.txt
