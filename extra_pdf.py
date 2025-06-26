import os
import PyPDF2
from openpyxl import Workbook
from tempfile import TemporaryDirectory
import pdfplumber
import re
def clean_plumber_text(text):
    # 删除中文之间多余的空格：如 “威 胁 事 件” => “威胁事件”
    text = re.sub(r'(?<=[\u4e00-\u9fff])\s+(?=[\u4e00-\u9fff])', '', text)

    # 删除英文字母之间的多余空格（如 T a c t i c => Tactic）
    text = re.sub(r'(?<=[A-Za-z])\s+(?=[A-Za-z])', '', text)

    # 删除换行符，合并成段（也可以保留段落格式，见下方注释）
    text = re.sub(r'\n+', '', text)

    # 可选：移除参考文献编号如 [1]、[2]
    # text = re.sub(r'\[\d+\]', '', text)

    return text.strip()
def extract_text_from_pdf(pdf_path):
    full_text = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            # 获取页面宽度用于分栏
            width = page.width

            # 左栏区域
            left = page.within_bbox((0, 0, width / 2, page.height))
            left_text = left.extract_text() if left else ""

            # 右栏区域
            right = page.within_bbox((width / 2, 0, width, page.height))
            right_text = right.extract_text() if right else ""

            # 清洗空格等（可用之前的 clean_plumber_text）
            combined = clean_plumber_text(left_text + right_text)
            full_text += combined + "\n"
    return full_text

def clean_filename(filename):
    base = os.path.splitext(filename)[0]
    return base.rsplit("_", 1)[0]  # 删除最后一个下划线及之后部分
def clean_text_for_excel(text):
    # 删除所有非法的控制字符（ASCII 0–31，除了换行符和制表符）
    cleaned = re.sub(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]", "", text)
    return cleaned



def save_pdfs_to_excel(folder_path, output_excel_path):
    wb = Workbook()
    ws = wb.active
    ws.title = "PDF文本内容"
    ws.append(["文件名", "提取的文本"])

    for filename in os.listdir(folder_path):
        if filename.lower().endswith(".pdf"):
            pdf_path = os.path.join(folder_path, filename)
            print(f"正在处理：{filename}")
            text = extract_text_from_pdf(pdf_path)
            cleaned_name = clean_filename(filename)
            safe_text = clean_text_for_excel(text) if text else "[无可提取文本]"
            ws.append([cleaned_name, safe_text])

    wb.save(output_excel_path)
    print(f"\n✅ 所有 PDF 文本已保存为：{output_excel_path}")

# 使用示例（请替换为你的文件夹路径）
pdf_folder = "/root/VscodeProject/PythonProject/nebula_data/111"
output_excel = "./output2.xlsx"
save_pdfs_to_excel(pdf_folder, output_excel)
