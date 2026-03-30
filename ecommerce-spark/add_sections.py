# -*- coding: utf-8 -*-
from docx import Document
from docx.shared import Pt, Inches, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml.ns import qn
from docx.oxml import OxmlElement
import os

doc_path = r'C:\Users\28414\Desktop\本学期期末作业\spark\spark改进版本.docx'
output_path = r'C:\Users\28414\Desktop\本学期期末作业\spark\spark改进版本.docx'

doc = Document(doc_path)

changes_made = []

insert_after_para = None
insert_index = None

for i, para in enumerate(doc.paragraphs):
    if '3.3  ETL处理流程' in para.text and i > 100:
        insert_after_para = para
        insert_index = i
        break

if insert_index:
    new_sections = [
        "",
        "3.4  统一日志框架",
        "系统采用SLF4J + Logback作为统一日志框架，通过Logging trait实现日志能力的混入。所有模块统一使用logger.info/warn/error替代println，支持日志级别控制和按天滚动存储。",
        "",
        "  Scala   ·   10 lines",
        "  1  trait Logging {",
        "  2    @transient private var _logger: Logger = _",
        "  3    protected def logger: Logger = {",
        "  4      if (_logger == null) {",
        "  5        _logger = LoggerFactory.getLogger(getClass.getName)",
        "  6      }",
        "  7      _logger",
        "  8    }",
        "  9  }",
        "",
        "3.5  配置验证机制",
        "AppConfig提供validate()方法，在系统启动时自动验证配置项合法性，包括端口范围验证（1-65535）、比例参数验证（0-1范围）、正整数验证（分区数、批大小等）。",
        "",
        "  Scala   ·   10 lines",
        "  1  def validate(): Unit = {",
        "  2    require(STREAM_WINDOW > 0, s\"STREAM_WINDOW 必须大于 0\")",
        "  3    require(SOCKET_PORT > 0 && SOCKET_PORT <= 65535,",
        "  4      s\"SOCKET_PORT 必须在 1-65535 范围内\")",
        "  5    require(ML_TRAIN_RATIO > 0 && ML_TRAIN_RATIO < 1,",
        "  6      s\"ML_TRAIN_RATIO 必须在 (0, 1) 范围内\")",
        "  7    require(SHUFFLE_PARTITIONS > 0,",
        "  8      s\"SHUFFLE_PARTITIONS 必须大于 0\")",
        "  9  }",
        "",
        "3.6  数据模型校验",
        "所有case class内置require()验证逻辑，确保数据质量。UserBehaviorLog验证behavior字段合法性（pv/buy/cart/fav），CleanBehavior验证hour（0-23）、dayOfWeek（1-7），SalesReport验证数值非负、转化率范围。",
        "",
        "  Scala   ·   8 lines",
        "  1  case class UserBehaviorLog(userId: String, itemId: String,",
        "  2    category: String, behavior: String, timestamp: Long) {",
        "  3    require(userId.nonEmpty, s\"userId 不能为空\")",
        "  4    require(Behaviors.VALID_BEHAVIORS.contains(behavior),",
        "  5      s\"behavior 必须是 pv/buy/cart/fav 之一\")",
        "  6    require(timestamp > 0, s\"timestamp 必须大于 0\")",
        "  7  }",
        "",
    ]
    
    for j, content in enumerate(new_sections):
        new_para = doc.add_paragraph(content)
        insert_after_para._element.addnext(new_para._element)
        insert_after_para = new_para
    
    changes_made.append(f"在段落 {insert_index} 后添加 3.4/3.5/3.6 新章节")

print("=" * 60)
print("文档修改记录：")
print("=" * 60)
for change in changes_made:
    print(change)

doc.save(output_path)
print("=" * 60)
print(f"文档已保存至: {output_path}")
