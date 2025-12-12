FROM apache/airflow:2.7.3-python3.10

COPY requirements.txt /requirements.txt
RUN pip install --user --no-cache-dir -r /requirements.txt

USER root

# 1. 시스템 패키지 업데이트 및 Java(JDK), 필수 유틸리티 설치 (한 번에 실행)
RUN apt-get update && \
    apt-get install -y wget gnupg unzip default-jdk

# 2. Chrome 브라우저 설치
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    apt install -y ./google-chrome-stable_current_amd64.deb && \
    ln -sf /usr/bin/google-chrome /usr/bin/chromium && \
    rm google-chrome-stable_current_amd64.deb

# 4. JAVA_HOME 환경변수 설정 (KoNLPy용)
ENV JAVA_HOME /usr/lib/jvm/default-java

USER airflow