FROM apache/airflow:2.7.3-python3.10

COPY requirements.txt /requirements.txt
RUN pip install --user --no-cache-dir -r /requirements.txt

USER root

# Install Chromium
RUN apt-get update && \
    apt-get install -y wget gnupg unzip && \
    wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    apt install -y ./google-chrome-stable_current_amd64.deb && \
    ln -sf /usr/bin/google-chrome /usr/bin/chromium

# Install ChromeDriver (버전은 chromium 버전에 맞춰야 함)
RUN wget https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/138.0.7204.183/linux64/chromedriver-linux64.zip && \
    unzip chromedriver-linux64.zip && \
    mv chromedriver-linux64/chromedriver /usr/local/bin/chromedriver && \
    chmod +x /usr/local/bin/chromedriver && \
    rm -rf chromedriver-linux64*


USER airflow
