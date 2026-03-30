# -*- coding: utf-8 -*-
from docx import Document
from docx.shared import Pt, Inches, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
import os

doc_path = r'C:\Users\28414\Desktop\本学期期末作业\spark\spark改进版本.docx'
output_path = r'C:\Users\28414\Desktop\本学期期末作业\spark\spark改进版本.docx'

doc = Document(doc_path)

changes_made = []

for i, para in enumerate(doc.paragraphs):
    text = para.text
    
    if '(col("timestamp") % 86400 / 3600)' in text:
        para.text = para.text.replace(
            '(col("timestamp") % 86400 / 3600)',
            'hour(from_unixtime(col("timestamp")))'
        )
        changes_made.append(f"[{i}] 更新模块二：小时计算逻辑修复")
    
    if 'query.awaitTermination()' in text and 'STREAM_TIMEOUT' not in text:
        para.text = para.text.replace(
            'query.awaitTermination()',
            'query.awaitTermination(AppConfig.STREAM_TIMEOUT)'
        )
        changes_made.append(f"[{i}] 更新模块三：超时配置外部化")

print("=" * 60)
print("文档修改记录：")
print("=" * 60)
for change in changes_made:
    print(change)

doc.save(output_path)
print("=" * 60)
print(f"文档已保存至: {output_path}")
