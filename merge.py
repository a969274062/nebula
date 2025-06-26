import pandas as pd
from rapidfuzz import process, fuzz

# === Step 1: 读取 Excel 文件 ===
df1 = pd.read_excel("output.xlsx")
df2 = pd.read_excel("筛选论文信息表.xlsx")

col1 = '论文标题'
col2 = '论文标题'


# === Step 2: 清洗文本（统一格式）===
def clean_text(text):
    if isinstance(text, str):
        return (
            text.replace('/', '')
            .replace('_', ' ')
            .replace('：', ':')
            .replace('...', '')
            .replace('…', '')
            .replace(' ', '')
            .lower()
        )
    return text


df1['cleaned'] = df1[col1].apply(clean_text)
df2['cleaned'] = df2[col2].apply(clean_text)

# 给 df1 和 df2 的列名加前缀，防止重复
df1_prefixed = df1.add_prefix("df1_")


# === Step 3: 模糊匹配每一行 ===
match_results = []

for i, row in df1_prefixed.iterrows():
    query = row['df1_cleaned']
    match, score, idx = process.extractOne(query, df2['cleaned'], scorer=fuzz.token_sort_ratio)

    matched_row = df2.iloc[idx]

    # 合并两行，避免列冲突
    combined_row = pd.concat([row.drop(labels='df1_cleaned'), matched_row.drop(labels='cleaned')])
    combined_row['相似度'] = score
    match_results.append(combined_row)

# === Step 4: 输出合并结果 ===
merged_df = pd.DataFrame(match_results)
merged_df.to_excel("相似度匹配_合并结果.xlsx", index=False)
