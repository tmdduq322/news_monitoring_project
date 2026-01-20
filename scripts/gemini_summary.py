import os
import sys
import time
import re
import pymysql
import requests
import argparse
import google.generativeai as genai
from datetime import datetime

# 1. 환경 변수 로드
keys_env = os.getenv("GEMINI_API_KEYS")
if not keys_env:
    keys_env = os.getenv("GEMINI_API_KEY")

API_KEYS = keys_env.split(',') if keys_env else []
current_key_index = 0

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_PAGE_ID = os.getenv("NOTION_PAGE_ID") 
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# 로그 출력 함수
def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)

if not API_KEYS:
    log("❌ GEMINI_API_KEYS가 설정되지 않았습니다.")
    sys.exit(1)

if not NOTION_PAGE_ID:
    log("❌ NOTION_PAGE_ID가 설정되지 않았습니다.")
    sys.exit(1)

# 초기 설정
def configure_genai(key_index):
    global model
    try:
        current_key = API_KEYS[key_index].strip()
        genai.configure(api_key=current_key)
        model = genai.GenerativeModel('gemini-2.5-flash')
        log(f"🔑 API Key #{key_index + 1} 적용 완료 (총 {len(API_KEYS)}개)")
    except Exception as e:
        log(f"❌ API Key 설정 중 오류: {e}")
        sys.exit(1)

configure_genai(current_key_index)

def get_yesterday_data(target_date):
    """DB에서 해당 날짜의 주요 기사 제목 + URL 추출"""
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, db=DB_NAME, charset='utf8mb4')
    try:
        with conn.cursor() as cursor:
            sql = f"""
                SELECT keyword, title, original_article_url
                FROM news_posts 
                WHERE DATE(crawled_at) = '{target_date}'
                ORDER BY copy_rate DESC LIMIT 100
            """
            cursor.execute(sql)
            results = cursor.fetchall()
            
            formatted_data = []
            for row in results:
                keyword = row[0]
                title = row[1]
                url = row[2] if row[2] else "URL 없음"
                formatted_data.append(f"- [{keyword}] {title} (URL: {url})")
            return formatted_data
    except Exception as e:
        log(f"❌ DB 조회 실패: {e}")
        sys.exit(1)
    finally:
        conn.close()

def generate_summary(data_list):
    """제미나이를 이용한 트렌드 요약 생성"""
    global current_key_index
    if not data_list:
        return "데이터가 없어 요약을 생성할 수 없습니다."

    context = "\n".join(data_list)
    
    # [프롬프트 수정] 링크 형식을 더 명확하게 지시
    prompt = f"""
    너는 뉴스 데이터 분석가야. 아래 데이터를 바탕으로 트렌드를 요약해줘.
    
    [데이터]
    {context}

    [형식]
    💡 오늘의 핵심 이슈 (5가지)
    1. (이슈 1 - 2줄 요약)
    2. (이슈 2)
    3. (이슈 3)
    4. (이슈 4)
    5. (이슈 5)

    🔥 트렌드 분석
    (관심사 분석)

    📰 주요 뉴스 바로가기 (5개 추천)
    - [기사 제목 전체](기사 URL)
    - [기사 제목 전체](기사 URL)
    - [기사 제목 전체](기사 URL)
    - [기사 제목 전체](기사 URL)
    - [기사 제목 전체](기사 URL)

    [주의사항]
    1. 마크다운 문법(굵게 등) 금지. 단, 링크는 반드시 [제목](주소) 형식을 지킬 것.
    2. 링크 생성 시 [제목]과 (주소) 사이에 띄어쓰기를 하지 마시오.
    """
    
    max_retries = 3
    attempt = 0
    
    while attempt < max_retries:
        try:
            log(f"🤖 Gemini 요청 시작 (Key #{current_key_index + 1}, 시도 {attempt + 1})...")
            response = model.generate_content(prompt)
            # 링크 포맷([])을 제외한 나머지 마크다운 제거
            text = response.text.replace("**", "").replace("##", "").replace("###", "")
            return text
            
        except Exception as e:
            error_msg = str(e)
            if "429" in error_msg or "Quota" in error_msg or "ResourceExhausted" in error_msg:
                log(f"⚠️ 현재 키(#{current_key_index + 1}) 한도 초과!")
                if len(API_KEYS) > 1:
                    current_key_index = (current_key_index + 1) % len(API_KEYS)
                    log(f"♻️ 다음 키(#{current_key_index + 1})로 교체합니다...")
                    configure_genai(current_key_index)
                    time.sleep(2)
                    continue 
                else:
                    wait_time = 60
                    log(f"⏳ 예비 키 없음. {wait_time}초 대기...")
                    time.sleep(wait_time)
                    attempt += 1
            else:
                log(f"⚠️ 오류: {error_msg}")
                time.sleep(10)
                attempt += 1
            
    log("❌ 실패: 모든 재시도 소진.")
    sys.exit(1)

# 👇 [핵심 기능 강화] 하이퍼링크 파싱 로직 업그레이드
def parse_markdown_to_notion_blocks(text):
    blocks = []
    lines = text.split('\n')
    
    # 패턴 1: 정상적인 마크다운 링크 [제목](주소) - 띄어쓰기 허용
    # \[([^\]]+)\] : 대괄호 안의 내용 (제목)
    # \s* : 중간에 공백이 있어도 됨
    # \((https?://[^)]+)\) : 소괄호 안의 http로 시작하는 주소
    link_pattern = re.compile(r'\[(.*?)\]\s*\((https?://.*?)\)')
    
    # 패턴 2: 괄호만 쳐진 URL (백업용) -> "텍스트 (URL)" 형태
    fallback_pattern = re.compile(r'(.*)\s*\((https?://.*?)\)')

    for line in lines:
        line = line.strip()
        if not line: continue
        
        # 블록 타입 결정
        if line.startswith("- "):
            block_type = "bulleted_list_item"
            content = line[2:]
        elif line[0].isdigit() and line[1:3] == ". ":
            block_type = "numbered_list_item"
            content = line[3:]
        elif line.startswith("💡") or line.startswith("🔥") or line.startswith("📰"):
            block_type = "heading_3"
            content = line
        else:
            block_type = "paragraph"
            content = line

        rich_text = []
        
        # 1. 마크다운 링크 패턴 시도 [제목](주소)
        match = link_pattern.search(content)
        
        # 2. 실패 시 백업 패턴 시도 "제목 (주소)"
        if not match:
            fallback_match = fallback_pattern.search(content)
            # URL 형식이 맞고, 앞부분(제목)이 너무 짧지 않으면 링크로 인정
            if fallback_match:
                match = fallback_match

        if match:
            # 링크가 있는 경우
            # group(1): 제목, group(2): URL
            title_text = match.group(1).replace("[", "").replace("]", "").strip() # 제목에 남은 대괄호 제거
            url_text = match.group(2).strip()
            
            # 링크 앞부분 텍스트 (있다면)
            pre_text = content[:match.start()].strip()
            if pre_text:
                rich_text.append({"type": "text", "text": {"content": pre_text + " "}})
                
            # 링크 부분 (클릭 가능하게)
            rich_text.append({
                "type": "text",
                "text": {
                    "content": title_text,
                    "link": {"url": url_text} # 🔗 하이퍼링크 적용
                }
            })
        else:
            # 링크가 없는 일반 텍스트
            rich_text.append({"type": "text", "text": {"content": content}})

        blocks.append({
            "object": "block",
            "type": block_type,
            block_type: {
                "rich_text": rich_text
            }
        })
        
    return blocks

def create_summary_page_in_notion(summary_text, target_date):
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    # 파싱된 블록 생성
    content_blocks = parse_markdown_to_notion_blocks(summary_text)

    payload = {
        "parent": {"page_id": NOTION_PAGE_ID}, 
        "properties": {
            "title": { 
                "title": [
                    {"text": {"content": f"🤖 {target_date} AI 요약 리포트"}}
                ]
            }
        },
        "children": [
            {
                "object": "block",
                "type": "callout",
                "callout": {
                    "rich_text": [{"type": "text", "text": {"content": "Gemini 2.5 Flash 뉴스 요약"}}],
                    "icon": {"emoji": "📰"},
                    "color": "gray_background"
                }
            },
            {
                "object": "block",
                "type": "divider",
                "divider": {}
            }
        ] + content_blocks
    }
    
    url = "https://api.notion.com/v1/pages"
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        log(f"✅ 노션 리포트 생성 완료: {target_date}")
        
    except requests.exceptions.HTTPError as err:
        log(f"❌ 노션 요청 실패: {err}")
        log(f"응답 내용: {response.text}")
        sys.exit(1)

def run_gemini_pipeline(target_date):
    news_data = get_yesterday_data(target_date)
    log(f"데이터 {len(news_data)}건 조회됨.")
    
    if not news_data:
        log("데이터가 없어 종료합니다.")
        return

    summary = generate_summary(news_data)
    log("Gemini 요약 완료.")
    
    create_summary_page_in_notion(summary, target_date)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="데이터 조회 대상 날짜 (YYYY-MM-DD)")
    args = parser.parse_args()

    if args.date:
        run_gemini_pipeline(args.date)
    else:
        log("⚠️ 날짜(--date) 파라미터가 필요합니다.")
        sys.exit(1)