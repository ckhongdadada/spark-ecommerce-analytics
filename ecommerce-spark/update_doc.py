# -*- coding: utf-8 -*-
from docx import Document
from docx.shared import Pt, Inches, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml.ns import qn
import os

doc_path = r'C:\Users\28414\Desktop\本学期期末作业\spark\spark基本版本.docx'
output_path = r'C:\Users\28414\Desktop\本学期期末作业\spark\spark改进版本.docx'

doc = Document(doc_path)

def update_paragraph(para, old_text, new_text):
    if old_text in para.text:
        for run in para.runs:
            if old_text in run.text:
                run.text = run.text.replace(old_text, new_text)
                return True
        para.text = para.text.replace(old_text, new_text)
        return True
    return False

changes_made = []

for i, para in enumerate(doc.paragraphs):
    text = para.text
    
    if '单例工厂、全局配置中心与公共数据模型三项关键抽象' in text:
        if update_paragraph(para, 
            '单例工厂、全局配置中心与公共数据模型三项关键抽象',
            '单例工厂、全局配置中心、公共数据模型、统一日志框架与配置验证机制五项关键抽象'):
            changes_made.append(f"[{i}] 更新摘要：添加日志框架和配置验证")
    
    if '系统缺乏完整的数据质量监控体系' in text:
        if update_paragraph(para,
            '其四，系统缺乏完整的数据质量监控体系',
            '其四，系统已添加数据模型校验和配置验证机制，保障数据质量'):
            changes_made.append(f"[{i}] 更新总结：数据质量监控已完善")
    
    if 'MLlib模型未进行超参数调优' in text:
        if update_paragraph(para,
            '其二，MLlib模型未进行超参数调优，可引入CrossValidator进行网格搜索',
            '其二，MLlib模型已添加类别权重处理数据不平衡问题，后续可引入CrossValidator进行网格搜索'):
            changes_made.append(f"[{i}] 更新总结：MLlib类别不平衡已处理")
    
    if 'hour = (timestamp % 86400 / 3600).toInt' in text:
        if update_paragraph(para,
            'hour = (timestamp % 86400 / 3600).toInt',
            'hour = java.time.Instant.ofEpochSecond(timestamp).atZone(java.time.ZoneId.of(AppConfig.TIMEZONE)).getHour'):
            changes_made.append(f"[{i}] 更新模块一：时区处理")
    
    if 'ZoneId.of("Asia/Shanghai")' in text:
        if update_paragraph(para,
            'ZoneId.of("Asia/Shanghai")',
            'ZoneId.of(AppConfig.TIMEZONE)'):
            changes_made.append(f"[{i}] 更新模块一：时区配置外部化")
    
    if 'uid.hashCode.toLong & Long.MaxValue' in text:
        if update_paragraph(para,
            'uid.hashCode.toLong & Long.MaxValue',
            'org.apache.spark.util.Utils.nonNegativeHash(uid)'):
            changes_made.append(f"[{i}] 更新模块四：Hash算法优化")
    
    if 'println(s"[M1' in text:
        if update_paragraph(para,
            'println(s"[M1',
            'logger.info(s"[M1'):
            changes_made.append(f"[{i}] 更新日志输出")
    
    if 'println(s"[M2' in text:
        if update_paragraph(para,
            'println(s"[M2',
            'logger.info(s"[M2'):
            changes_made.append(f"[{i}] 更新日志输出")
    
    if 'println(s"[M3' in text:
        if update_paragraph(para,
            'println(s"[M3',
            'logger.info(s"[M3'):
            changes_made.append(f"[{i}] 更新日志输出")
    
    if 'println(s"[M4' in text:
        if update_paragraph(para,
            'println(s"[M4',
            'logger.info(s"[M4'):
            changes_made.append(f"[{i}] 更新日志输出")
    
    if 'println(s"[M5' in text:
        if update_paragraph(para,
            'println(s"[M5',
            'logger.info(s"[M5'):
            changes_made.append(f"[{i}] 更新日志输出")

print("=" * 60)
print("文档修改记录：")
print("=" * 60)
for change in changes_made:
    print(change)

doc.save(output_path)
print("=" * 60)
print(f"文档已保存至: {output_path}")
