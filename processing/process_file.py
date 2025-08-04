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
        text = str(text)
        matches = list(re.finditer(r"다\.", text))
        for match in matches:
            start = match.start()
            if start >= 1 and text[start - 1] == "니":
                continue  # '니다.'이면 무시
            return True
        return False

    def should_remove(title, content):
        has_valid = has_valid_da(title) or has_valid_da(content)
        has_cartoon = "만평" in title or "만평" in content
        return not has_valid or has_cartoon

    mask = df_filtered.apply(lambda row: should_remove(row["게시물 제목"], row["게시물 내용"]), axis=1)
    df_final = df_filtered[~mask]
    return df_final
