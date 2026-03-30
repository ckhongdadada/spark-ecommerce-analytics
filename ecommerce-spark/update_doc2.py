# -*- coding: utf-8 -*-
from docx import Document
from docx.shared import Pt, Inches, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml.ns import qn
import os

doc_path = r'C:\Users\28414\Desktop\本学期期末作业\spark\spark改进版本.docx'
output_path = r'C:\Users\28414\Desktop\本学期期末作业\spark\spark改进版本.docx'

doc = Document(doc_path)

def update_paragraph_text(para, old_text, new_text):
    if old_text in para.text:
        para.text = para.text.replace(old_text, new_text)
        return True
    return False

changes_made = []

insert_points = {}

for i, para in enumerate(doc.paragraphs):
    text = para.text
    
    if 'timestamp % 86400 / 3600' in text:
        if update_paragraph_text(para,
            'timestamp % 86400 / 3600',
            'hour(from_unixtime(col("timestamp")))'):
            changes_made.append(f"[{i}] 更新模块二：小时计算逻辑修复")
    
    if 'awaitTermination(60000)' in text:
        if update_paragraph_text(para,
            'awaitTermination(60000)',
            'awaitTermination(AppConfig.STREAM_TIMEOUT)'):
            changes_made.append(f"[{i}] 更新模块三：超时配置外部化")
    
    if 'classOf[com.ecommerce.core.UserBehaviorLog]' in text:
        if update_paragraph_text(para,
            'classOf[com.ecommerce.core.UserBehaviorLog]',
            'classOf[UserBehaviorLog],\n        classOf[CleanBehavior],\n        classOf[SalesReport],\n        classOf[UserNode],\n        classOf[BuyEdge]'):
            changes_made.append(f"[{i}] 更新Kryo注册：添加更多类")
    
    if '3.3  公共数据模型' in text:
        insert_points['after_3_3'] = i
    
    if '表3-1' in text and '核心文件' in text:
        insert_points['table_3_1'] = i

print("=" * 60)
print("文档修改记录：")
print("=" * 60)
for change in changes_made:
    print(change)

print("\n插入点：")
for k, v in insert_points.items():
    print(f"  {k}: 段落 {v}")

doc.save(output_path)
print("=" * 60)
print(f"文档已保存至: {output_path}")
