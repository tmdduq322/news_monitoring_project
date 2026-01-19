# process_file.py
import re
import pandas as pd

def filter_untrusted_posts(all_data, untrusted_file, trusted_file):
    # 비신탁사 및 매체사 도메인 불러오기
    df_untrusted = pd.read_excel(untrusted_file)
    df_trusted = pd.read_excel(trusted_file)

    untrusted_copyrights = df_untrusted["저작권 문구"].dropna().tolist()
    untrusted_domains = df_untrusted["도메인"].dropna().tolist()
    trusted_domains = df_trusted["도메인"].dropna().tolist()

    def should_remove(post_content):
        post_content = str(post_content)
        contains_untrusted_copyright = any(c in post_content for c in untrusted_copyrights)
        contains_untrusted_domain = any(d in post_content for d in untrusted_domains)
        contains_trusted_domain = any(t in post_content for t in trusted_domains)
        return (contains_untrusted_copyright or contains_untrusted_domain) and not contains_trusted_domain

    # 결측값 방지
    content_series = all_data["게시물 내용"].fillna("")
    mask = content_series.apply(should_remove)
    df_filtered = all_data[~mask]

    # 데이터가 없으면 빈 DataFrame 반환
    if df_filtered.empty:
        df_filtered = all_data.iloc[0:0]

    return df_filtered


def filter_da(df_filtered):
    def has_valid_da(text):
        """
        본문에 '~다.' 형태가 포함되어 있는지 확인하는 함수
        """
        text = str(text)
        # '다' 뒤에 공백이 있거나 붙어서 점(.)이 나오는 경우 찾기
        matches = list(re.finditer(r"다\s*\.", text))
        
        for match in matches:
            start = match.start()
            
            # [예외] '니다.'는 보통 존댓말/댓글이므로 기사체로 보지 않고 패스
            # (만약 '니다.'도 포함해서 수집하고 싶으면 이 if문을 지우세요)
            if start >= 1 and text[start - 1] == "니":
                continue  
            
            # '~다.' 발견됨 -> 이 글은 우리가 원하는 글임!
            return True
            
        # 끝까지 뒤졌는데 '~다.'가 없음 -> 우리가 원하는 글이 아님
        return False

    def should_remove(title, content):
        """
        삭제 여부를 결정하는 함수 (True면 삭제, False면 유지)
        """
        # 1. 제목이나 내용에 '다.'가 포함되어 있으면 -> 삭제하지 않음(False)
        if has_valid_da(title) or has_valid_da(content):
            return False 
        
        # 2. '다.'가 없으면 -> 삭제함(True)
        return True

    # 결측값 처리
    content_series = df_filtered["게시물 내용"].fillna("")
    title_series = df_filtered["게시물 제목"].fillna("")

    # apply 함수로 각 행 검사
    mask = df_filtered.apply(lambda row: should_remove(row['게시물 제목'], row['게시물 내용']), axis=1)
    
    # mask가 True인(삭제해야 할) 애들을 제외(~)하고 남김
    df_result = df_filtered[~mask]

    # 데이터가 없으면 빈 DataFrame 반환
    if df_result.empty:
        df_result = df_filtered.iloc[0:0]

    return df_result