# 📰 News Copy Detection Pipeline
> **커뮤니티 뉴스 불펌 탐지 및 네티즌 관심 뉴스 추적 데이터 파이프라인**

이 프로젝트는 뽐뿌, 클리앙, 인벤 등 국내 주요 24개 커뮤니티의 게시글을 수집하고, 네이버 뉴스 API와 텍스트 유사도 분석(TF-IDF)을 통해 **국내 네티즌의 관심 뉴스 기사를 탐지**하여 DB에 적재하는 자동화 시스템입니다.

---

## 🏗️ Architecture

### **Hybrid Infrastructure**
* **Local Server (Worker Node):** Apache Airflow, Docker, Selenium Grid
    * **역할:** 데이터 수집(Crawling), 자연어 처리(NLP), 유사도 분석, S3 업로드 수행
* **AWS Cloud (Storage & DB):**
    * **S3:** Raw Data(CSV), Processed Data, Extracted Results 등 단계별 데이터 저장 (Data Lake)
    * **RDS (MySQL):** 최종 분석 결과 및 메타데이터 저장 (Data Warehouse)

### **Pipeline Flow (Airflow DAG)**
1.  **Parallel Crawling:** 24개 커뮤니티를 트래픽 규모에 따라 3개 그룹(Heavy, Medium, Light)으로 나누어 멀티프로세싱 병렬 수집
2.  **Data Merge:** 분산 수집된 Raw Data를 날짜별로 병합
3.  **Preprocessing:** 텍스트 정제(Cleaning) 및 데이터 포맷팅 (Excel 변환)
4.  **Original Article Extraction:**
    * 네이버 뉴스 API를 활용한 원문 검색
    * TF-IDF & Cosine Similarity 기반 본문 유사도(Copy Rate) 산출
    * 결과 데이터 S3 업로드
5.  **Load to DB:** S3에 저장된 최종 분석 결과를 AWS RDS(MySQL)로 적재

---

## 🛠️ Tech Stack

| Category | Technology |
| --- | --- |
| **Orchestration** | Apache Airflow 2.x |
| **Language** | Python 3.10+ |
| **Crawling** | Selenium (Chrome/Headless), Requests, BeautifulSoup |
| **Data Processing** | Pandas, S3FS, SQLAlchemy |
| **Analysis (NLP)** | Scikit-learn (TF-IDF), KonLPy (Okt) |
| **Infrastructure** | Docker, Docker Compose |
| **Cloud (AWS)** | S3, RDS (MySQL) |

---

## 📂 Project Structure

```bash
news_monitoring_project/
├── dags/
│   └── news_copy_detection_pipeline.py  # Airflow DAG 정의 (TaskGroup, Schedule)
├── scripts/                             # Airflow Task에서 실행되는 메인 스크립트
│   ├── crawl_all_sites.py               # 멀티프로세싱 크롤링 진입점
│   ├── merge_all_raw_csv.py             # 수집 데이터 병합
│   ├── process_data.py                  # 전처리 (processing 모듈 호출)
│   ├── extract_original.py              # 원문 추적 (extraction 모듈 호출)
│   └── save_to_db.py                    # S3 -> RDS 데이터 적재
├── crawlers/                            # 커뮤니티별 크롤러 모듈 (24개 사이트)
│   ├── pp_crawler.py                    # 뽐뿌 크롤러
│   ├── clien_crawler.py                 # 클리앙 크롤러
│   ├── ...                              # (기타 사이트 크롤러들)
│   └── utils.py                         # 크롤링 공통 유틸리티
├── extraction/                          # 원문 추출 및 유사도 분석 코어 로직
│   ├── core_utils.py                    # 드라이버 설정, 텍스트 정제, 유사도 계산 함수
│   └── main_script.py                   # 멀티프로세싱 원문 매칭 상세 로직
├── processing/                          # 데이터 전처리 로직
│   └── process_file.py                  # 엑셀 변환 및 필터링 클래스
├── db/                                  # DB 관련 유틸리티
│   └── save_DB.py                       # 로컬 DB 테스트 및 연결 설정
├── config/                              # 설정 파일
│   ├── airflow.cfg                      # Airflow 설정
│   └── search_keywords_2025.xlsx        # 수집 대상 검색어 및 언론사 목록
├── docker-compose.yaml                  # Airflow 및 인프라 구성
├── Dockerfile                           # 커스텀 이미지 빌드 설정
└── requirements.txt                     # 의존성 패키지 목록

```
---


## 🚀 Installation & Setup

### 1. Prerequisites
이 프로젝트를 실행하기 위해서는 아래 도구들이 설치되어 있어야 합니다.
* **Docker** & **Docker Compose** (필수)
* **Python 3.10+** (로컬 개발 시)

### 2. Environment Variables (.env)

프로젝트 루트에 .env 파일을 생성하고 아래 정보를 입력해야 합니다.

```bash

# AWS S3 Configuration
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
S3_BUCKET_NAME=news-monitoring-bucket

# Database Configuration (AWS RDS)
DB_HOST=your-rds-endpoint.amazonaws.com
DB_PORT=3306
DB_USER=admin
DB_PASSWORD=your_password
DB_NAME=airflow_db

# Naver API (For News Search)
NAVER_CLIENT_ID=your_client_id
NAVER_CLIENT_SECRET=your_client_secret

# Local Config
CHROMEDRIVER_PATH=/usr/bin/chromedriver


```


## 3. Run with Docker
Airflow 컨테이너 및 필요한 서비스를 실행합니다.
```bash
# 1. 이미지 빌드 및 실행
docker-compose up -d --build

# 2. 실행 상태 확인
docker-compose ps
```

## 4. Usage
1. **Airflow 웹 접속**: http://localhost:8080

2. **DAG 활성화**: news_copy_detection_pipeline DAG를 Unpause(ON) 상태로 변경

3. **실행 (Trigger)**: Trigger DAG 버튼을 클릭하여 파이프라인 실행

4. **모니터링**: Graph View에서 각 태스크(Crawl -> Merge -> Process -> Extract -> Save)의 진행 상황 확인