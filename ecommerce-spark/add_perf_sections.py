# -*- coding: utf-8 -*-
from docx import Document
from docx.shared import Pt, Inches, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
import os

doc_path = r'C:\Users\28414\Desktop\本学期期末作业\spark\spark改进版本.docx'
output_path = r'C:\Users\28414\Desktop\本学期期末作业\spark\spark改进版本.docx'

doc = Document(doc_path)

changes_made = []

insert_after_para = None
insert_index = None

for i, para in enumerate(doc.paragraphs):
    if '8.4  数据倾斜处理' in para.text:
        insert_after_para = para
        insert_index = i
        break

if insert_index:
    new_sections = [
        "",
        "8.5  Kryo序列化优化",
        "Spark默认使用Java序列化，效率较低。本项目配置Kryo序列化器，并预注册所有自定义case class，显著减少序列化开销和传输数据量。",
        "",
        "  Scala   ·   12 lines",
        "  1  val conf = new SparkConf()",
        "  2    .set(\"spark.serializer\",",
        "  3      \"org.apache.spark.serializer.KryoSerializer\")",
        "  4    .registerKryoClasses(Array(",
        "  5      classOf[UserBehaviorLog],",
        "  6      classOf[CleanBehavior],",
        "  7      classOf[SalesReport],",
        "  8      classOf[UserNode],",
        "  9      classOf[BuyEdge],",
        " 10      classOf[Array[String]]",
        " 11    ))",
        "",
        "8.6  AQE自适应查询优化",
        "Spark 3.x引入的自适应查询执行（AQE）可在运行时动态调整查询计划。本项目启用AQE和分区合并，自动处理数据倾斜和小文件问题。",
        "",
        "  Scala   ·   4 lines",
        "  1  .set(\"spark.sql.adaptive.enabled\", \"true\")",
        "  2  .set(\"spark.sql.adaptive.coalescePartitions.enabled\",",
        "  3    \"true\")",
        "  4  .set(\"spark.sql.adaptive.skewJoin.enabled\", \"true\")",
        "",
    ]
    
    for j, content in enumerate(new_sections):
        new_para = doc.add_paragraph(content)
        insert_after_para._element.addnext(new_para._element)
        insert_after_para = new_para
    
    changes_made.append(f"在段落 {insert_index} 后添加 8.5/8.6 新章节")

print("=" * 60)
print("文档修改记录：")
print("=" * 60)
for change in changes_made:
    print(change)

doc.save(output_path)
print("=" * 60)
print(f"文档已保存至: {output_path}")
