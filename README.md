# 📰 News Copy Detection Pipeline
> **커뮤니티 뉴스 불펌 탐지 및 원문 추적 데이터 파이프라인**

이 프로젝트는 국내 주요 커뮤니티(뽐뿌, 클리앙, 인벤 등)의 게시글을 수집하고, 네이버 뉴스 API와 텍스트 유사도 분석(TF-IDF)을 통해 **뉴스 기사 무단 전재(불펌)** 여부를 탐지하여 DB에 적재하는 자동화 시스템입니다.


---

## 🏗️ Architecture

### **Hybrid Infrastructure**
* **Local Server (Worker Node):** Apache Airflow, Docker, Selenium Grid
    * 역할: 데이터 수집(Crawling), 자연어 처리(NLP), 유사도 분석, AWS S3 업로드
* **AWS Cloud (Storage & DB):**
    * **S3:** Raw Data(CSV), Processed Data, Extracted Results 저장 (중간 저장소)
    * **RDS (MySQL):** 최종 분석 결과 메타데이터 저장 (최종 저장소)

### **Pipeline Flow (Airflow DAG)**
1.  **Parallel Crawling:** 24개 커뮤니티를 트래픽 규모에 따라 3개 그룹(Heavy, Medium, Light)으로 나누어 병렬 수집
2.  **Data Merge:** 분산 수집된 Raw Data 병합
3.  **Preprocessing:** 텍스트 정제 및 포맷팅 (Excel 변환)
4.  **Original Article Extraction:**
    * 네이버 뉴스 API를 활용한 원문 검색
    * TF-IDF & Cosine Similarity 기반 본문 유사도(Copy Rate) 산출
    * 결과 데이터 S3 업로드
5.  **Load to DB:** S3에 저장된 분석 결과를 AWS RDS(MySQL)로 적재

---

## 🛠️ Tech Stack

* **Orchestration:** Apache Airflow 2.x
* **Language:** Python 3.10+
* **Crawling:** Selenium (Chrome/Headless), Requests, BeautifulSoup
* **Data Processing:** Pandas, S3FS, SQLAlchemy
* **Analysis (NLP):** Scikit-learn (TF-IDF), KonLPy (Okt)
* **Infrastructure:** Docker, Docker Compose
* **Cloud (AWS):** S3, RDS (MySQL)

---

## 📂 Project Structure

```bash
.
├── dags/
│   └── news_copy_detection_pipeline.py  # Airflow DAG 정의
├── scripts/
│   ├── crawl_all_sites.py      # 멀티프로세싱 크롤링 진입점
│   ├── merge_all_raw_csv.py    # 데이터 병합
│   ├── process_data.py         # 전처리
│   ├── extract_original.py     # 원문 추적 및 유사도 분석
│   └── save_to_db.py           # S3 -> RDS 데이터 적재
├── crawlers/                   # 사이트별 크롤러 모듈 (24개)
├── extraction/
│   ├── core_utils.py           # 드라이버 설정, 텍스트 정제, 유사도 계산
│   └── main_script.py          # 원문 추출 상세 로직
├── config/
│   └── search_keywords_2025.xlsx # 수집 대상 검색어 설정
├── docker-compose.yml          # Airflow 인프라 구성
└── requirements.txt            # 의존성 패키지