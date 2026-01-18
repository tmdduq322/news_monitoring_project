import pandas as pd
import requests
import os
import sys
import logging

# [핵심] .env에서 환경 변수 로드
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
DATABASE_ID = os.getenv("NOTION_DATABASE_ID")

def upload_to_notion(file_path, score_threshold=0.3):
    # Notion 설정 확인
    if not NOTION_TOKEN or not DATABASE_ID:
        print("❌ 오류: NOTION_TOKEN 또는 DATABASE_ID가 설정되지 않았습니다.")
        return

    # 1. 결과 데이터 로드
    try:
        df = pd.read_csv(file_path) if file_path.endswith('.csv') else pd.read_excel(file_path)
    except Exception as e:
        print(f"❌ 파일 로드 실패: {e}")
        return
    
    # 2. 유사도 0.3 이상 필터링 (LaTeX: $0.3$)
    filtered_df = df[df['score'] >= score_threshold]
    print(f"📊 총 {len(filtered_df)}개의 유의미한 데이터 추출 완료 (기준점: {score_threshold})")
    
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }

    for _, row in filtered_df.iterrows():
        # 3. 노션 데이터베이스 속성 매핑
        payload = {
            "parent": { "database_id": DATABASE_ID },
            "properties": {
                "제목": { "title": [{ "text": { "content": row['게시물 제목'] } }] },
                "유사도": { "number": round(float(row['score']), 4) },
                "URL": { "url": row['게시물 URL'] },
                "플랫폼": { "select": { "name": row['플랫폼'] } },
                "등록일": { "date": { "start": str(row['게시물 등록일자']).replace('.', '-') } }
            }
        }
        
        # 4. API 전송
        response = requests.post("https://api.notion.com/v1/pages", headers=headers, json=payload)
        if response.status_code == 200:
            print(f"✅ 업로드 성공: {row['게시물 제목'][:20]}...")
        else:
            print(f"⚠️ 업로드 실패: {response.status_code} - {response.text}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("사용법: python3 upload_to_notion.py [파일경로]")
    else:
        upload_to_notion(sys.argv[1])