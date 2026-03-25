#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import os
import sys
import time
import uuid
from typing import Dict, Any, Optional, List

import pika
from pika.exceptions import AMQPConnectionError, AMQPChannelError
from requests.exceptions import RequestException

from copaw_client import CoPawClient, CoPawConfig

# 使用CoPaw v0.1.0.post1
RABBITMQ_URL = os.getenv(
    "RABBITMQ_URL", "amqp://guest:147258369aB@r.pisiewang.top:45672/"
)
CACHEHOSP_QUEUE = "CacheHosp"
COPAW_BASE_URL = os.getenv("COPAW_BASE_URL", "http://localhost:8088")

PROMPT_TEMPLATE = r"""
对 __HOSPITAL_NAME__ 官网进行抓取，提取该医院所有可见医生的详细信息，经过结构化字段提取和数据核验后，将结果逐条实时推送到 RabbitMQ 队列。

---

## 一、执行步骤（按顺序执行）

### 1. 探索官网结构
- 先通过搜索引擎找到官网正确地址
- 分析网站导航，找到"专家介绍"、"医生团队"、"专家团队"、"科室医生"等入口
- **识别医生列表页的布局类型**（重要）：
  - 表格布局：医生信息以行列表格展示
  - 卡片布局：每个医生独立卡片形式
  - 混合布局：上述两种的结合
- 识别医生列表页和详情页的URL模式
- 确认是否存在分页机制（传统分页/滚动加载/点击加载更多）
**DOM结构分析：**
- 查看页面HTML结构
- 记录医生列表的容器元素（class/id）
- 记录各字段的选择器路径
- 检查是否存在动态加载内容（AJAX/JavaScript渲染）
### 2. 抓取医生列表

**根据布局类型选择提取策略：**

**表格布局提取：**
- 定位表格容器：`table.doctor-list-table` 或类似
- 遍历每行：`tbody/tr`
- 提取字段：姓名（第1列）、科室（第2列）、职称（第3列）等
- 收集详情链接：通常在姓名列的`<a>`标签

**卡片布局提取：**
- 定位卡片容器：`div.doctor-list` 或 `div.doctor-cards`
- 遍历每个卡片：`div.doctor-card`
- 提取字段：从卡片的各个子元素提取
- 收集详情链接：通常在卡片底部或标题

**分页处理：**
- 传统分页：点击页码按钮，观察URL变化规律
- 滚动加载：滚动到底部，监控网络请求中的数据接口
- 点击加载：点击"加载更多"按钮，分析请求参数

**遍历策略：**
- 遍历所有科室/专业分类，确保不遗漏
- 记录每个科室的医生数量，用于验证完整性
- 收集所有医生详情页URL，去重后保存

### 3. 抓取医生详情

**关键：必须完整遍历页面所有区域！不要遗漏任何信息！**

**页面区域遍历顺序（必须全部检查）：**
1. 顶部基本信息区（姓名、职称、科室等）
2. 键值对信息区（学历、职称、专业方向等）
3. 个人简介区
4. 社会职务区
5. 专业特长区
6. 主要成果区
7. 业务经历区（**重要：包含教育背景！**）
8. 其他区域

---

**识别页面布局类型（非常重要！）：**

**布局类型A - 键值对布局**（常见形式）：
页面以"字段名：值"的形式展示信息，例如：
```
学 历：本科
开诊科室：全科医学科
专业方向：内分泌疾病治疗
职称/职务：主任医师
工作年限：从事临床工作30余年
擅 长：擅长糖尿病、甲状腺病、冠心病、高血压、胃肠道疾病的诊断和治疗
```
**键值对布局提取规则：**
- 遍历页面中的所有"键：值"对
- 根据键名映射到对应字段：
  - "学历" → education
  - "开诊科室"、"科室"、"所属科室" → display_department
  - "专业方向" → specialty 方向部分
  - "职称"、"职务"、"职称/职务" → title（**必须完整提取，不能省略"副"字！**）
  - "工作年限"、"从业年限" → years_of_experience
  - "擅长"、"专业特长"、"诊疗特长" → specialty
  - "毕业院校"、"毕业学校" → alma_mater
  - "学位" → degree
- **关键：不要遗漏任何键值对！每个键值对都要提取并映射**
- **职称提取特别注意：键值对中的职称值必须原样保留，如"副主任医师"就是"副主任医师"，不能改成"主任医师"**

**布局类型B - 段落简介布局**：
页面以段落文字形式展示医生介绍，需要从文本中识别提取

**布局类型C - 混合布局**：
同时包含键值对和段落简介，两部分都要提取

---

**基础信息区**（顶部或左侧）：
   - 医生姓名（h1/h2标题）
   - 职称（标签或介绍文本，也可能在键值对中）
   - 所属科室（链接或文本，也可能在键值对中如"开诊科室"）
   - 行政职务（如"科室主任"，可能在键值对的"职务"中）
   - 医生照片

**专业特长区**：
   - 擅长疾病列表（特别注意：可能在键值对"擅长"中）
   - 诊疗范围描述
   - 专业方向（可能在键值对中）

**个人简介区**：
   - 医生简介原文（intro）：必须完整提取
   - 教育背景（毕业院校、学历、学位）：
     **优先从键值对中提取：**
     - "学历：本科" → education="本科"
     - "毕业院校：郑州大学" → alma_mater="郑州大学"
     - "学位：硕士" → degree="硕士"
   - 工作经历
   - 从业年限：**优先从键值对中提取**，如"工作年限：从事临床工作30余年"
   - 学术任职
   - 科研成果

**简介提取特别注意：**
- 简介区域可能在页面中下部，需要滚动查看完整内容
- 有些医院简介放在折叠区域，需要点击"展开"或"查看更多"
- 简介可能包含多个段落，必须全部提取，不要截断
- 常见位置关键词：个人简介、医生介绍、专业特长、学术成就、教育经历

**毕业院校提取特别注意：**
- **优先级1：键值对中的"毕业院校"、"毕业学校"**
- **优先级2：业务经历/工作经历区域（非常重要！）**
  - **时间段格式**：`1981-1984 新乡医学院` → alma_mater="新乡医学院"
  - **时间段+学历格式**：`1997-2000郑州大学...硕士研究生` → alma_mater="郑州大学", education="硕士"
  - **时间段+博士格式**：`2003-2006郑州大学...博士研究生` → alma_mater="郑州大学", education="博士"
  - 提取模式：时间段（如1981-1984）后紧跟的院校名称
  - 常见院校关键词：大学、学院、医学院、医科大学
- **优先级3：简介文本中的关键词匹配**
  - 关键词：毕业、院校、大学、学院、就读
  - 格式示例："毕业于郑州大学"、"郑州大学医学院"、"本科毕业于河南医科大学"
- **提取规则：**
  - 必须提取完整的院校名称，如"河南医科大学"、"郑州大学"、"北京协和医学院"
  - 不要提取专业名称，只要院校名
  - 如果有多个毕业院校，优先提取最高学历对应的院校（如博士>硕士>本科）

**字段提取规则：**

**必须准确提取的字段：**
- 医生姓名：2-4个汉字，从标题或首行提取
  **重要：姓名必须与职称、科室分离！常见错误模式：**
  - "张海玲副主任医师" → 姓名="张海玲"，职称="副主任医师"
  - "王明主任" → 姓名="王明"，职称="主任"或"主任医师"
  - "李华副主任医" → 姓名="李华"，职称="副主任医师"
  - "段永壮骨" → **错误！** 姓名="段永壮"，科室="骨"（姓名不能包含科室名！）
  - 职称前缀识别：副、主、副主 - 这些通常是职称的一部分，不属于姓名
  - 职称关键词：主任医师、副主任医师、主治医师、医师、护士长、教授、副教授
  - **科室关键词不能出现在姓名中：骨、内科、外科、妇科、产科、儿科、口腔、眼科、耳鼻喉、皮肤、精神、肿瘤等**
  - **如果姓名和职称连在一起，必须分离：先提取2-4个汉字作为姓名，剩余部分作为职称候选**
  - **姓名提取位置优先级：**
    1. 页面标题区域的医生姓名（通常在照片下方）
    2. 个人简介开头的人名
    3. 不要从URL或页面其他位置提取

- 科室：展示科室名
  **从以下位置提取：**
  - 键值对中的"开诊科室"、"科室"、"所属科室"
  - 页面上的科室链接或文本

- 学历（education）：**必须从多个位置查找**
  **提取优先级：**
  1. **键值对中的"学历"**（最优先）
  2. **个人简介中的学历关键词**（常见！）
     - 直接学历词：博士、硕士、本科、研究生、大专
     - 学历组合词：研究生学历、本科学历、博士学历、硕士学历、大专学历
     - 例如："阎宗毅，副主任医师，研究生学历" → education="研究生"
     - 例如："张梦真，女，教授，博士，主任医师" → education="博士"
     - 例如："本科学历，主治医师" → education="本科"
  3. **业务经历/工作经历区域的学历关键词**
     - 模式：时间段 + 院校 + 学历类型
     - 例如：`2003-2006郑州大学...博士研究生` → education="博士"
     - 例如：`1997-2000郑州大学...硕士研究生` → education="硕士"
  **常见值：**本科、硕士、博士、研究生、大专
  **注意：**"研究生学历" → education="研究生"，不是"硕士"

- 工作年限（years_of_experience）：**必须从多个位置查找**
  **提取优先级：**
  1. **键值对中的"工作年限"、"从业年限"**（最优先）
  2. **个人简介文本中的年限关键词**（常见！）
     - 模式：`从事...工作XX年`、`从业XX年`、`临床工作XX年`
     - 例如：`从事神经外科近20年` → years_of_experience=20
     - 例如：`从事临床工作30余年` → years_of_experience=30
     - 例如：`从业20余年` → years_of_experience=20
     - 关键词：从事、从业、工作、余年、近、余
  3. **业务经历中的时间推算**
     - 例如：`1984-1997工作` → 可推算约13年
  **提取规则：**
  - 提取数字部分，忽略"近"、"余"等修饰词
  - 如果有多个工作年限描述，取最近/最准确的
  - 如果找不到，设为 null，不要设为 0

- 擅长（specialty）：**优先从键值对提取**
   - 键名匹配：擅长、专业特长、诊疗特长、主治
   - 提取完整内容

- 职称（title）：**必须准确提取，特别注意区分正高和副高**
  **提取优先级：**
  1. **键值对中的"职称"**（注意：如果值是"省内知名专家"等非标准职称，需从简介补充）
  2. **个人简介中的职称关键词**
     - 在简介开头通常有职称信息
     - 例如："张梦真，女，教授，博士，主任医师" → title="主任医师"（或"教授、主任医师"）
     - 关键词：主任医师、副主任医师、主治医师、教授、副教授、研究员等
  **关键区分：**
     - "副主任医师" ≠ "主任医师"（注意"副"字！）
     - "副主任医师" → title="副主任医师"
     - "主任医师" → title="主任医师"
     - "主治医师" → title="主治医师"
  **提取规则：**
     - 必须完整提取职称文本，不能省略"副"字
     - 如果键值对中写"职称/职务：副主任医师"，则title="副主任医师"
     - 如果键值对中写"职称/职务：省内知名专家"等非标准职称，需从简介中提取真正的职称
     - 常见职称全称：主任医师、副主任医师、主治医师、住院医师、医师、主任护师、副主任护师、主管护师、护师、护士、教授、副教授、研究员、副研究员

- 简介：完整的个人介绍原文
  **重要：简介通常在以下位置：**
  - 页面中标注"个人简介"、"医生简介"、"人物介绍"等标题下的内容
  - 包含教育背景、工作经历、学术成就等段落
  - **必须提取完整内容，不能遗漏**

- 毕业院校（alma_mater）：**必须从多个位置查找**
  **提取优先级：**
  1. **键值对中的"毕业院校"、"毕业学校"**（最优先）
  2. **业务经历/工作经历区域的文本**（常见）
     - 模式：年份 + 院校名 + 专业 + "毕业"
     - 例如："1988年河南医科大学临床医学系毕业后" → alma_mater="河南医科大学"
     - 例如："毕业于郑州大学医学院" → alma_mater="郑州大学"
  3. **个人简介中的教育背景描述**
     - 关键词：毕业、院校、大学、学院、就读、毕业于
  **提取规则：**
  - 提取完整院校名称，不要专业名
  - "河南医科大学" ✓ "河南医科大学临床医学系" ✗
  - 多个院校时，取最高学历对应的院校

- 从非结构化文本中提取结构化字段

### 4. 数据核验与推送
- 根据获取到的医生详情页URL，验证数据合法性，重点检查一下科室名，是否与医院官网的科室名一致
- 验证数据合法性，重点检查一下科室名，是否与医院官网的科室名一致
- 推送到 RabbitMQ 队列
- 本地备份（防止推送失败）

---

## 二、队列配置

```
RABBITMQ_URL: amqp://guest:147258369aB@r.pisiewang.top:45672/
QUEUE_NAME: DoctorResult
```
---

## 三、推送数据格式

```json
{
  "name": "医生姓名",
  "hospital": "医院全称",
  "standard_department": "标准科室名",
  "display_department": "展示科室名",
  "title": "职称",
  "administrative_position": "行政职务",
  "academic_title": "学术头衔",
  "education": "学历",
  "degree": "学位",
  "alma_mater": "毕业院校",
  "years_of_experience": 28,
  "intro": "医生简介原文",
  "specialty": "擅长领域原文",
  "confidence_score": 100.0,
  "otherpropertys": "{\"confidence_name\": \"官网\", \"source_kind\": \"official\", \"seed_url\": \"官网首页URL\", \"root_domain\": \"域名\", \"url\": \"医生详情页URL\", \"match_score\": 1.0, \"page_record_time\": null, \"fetch_method\": \"playwright\"}",
  "source_model": "VW5W"
}
```

---

## 四、结构化字段提取规则

### 使用医生信息提取模板.md
## 五、数据核验规则

### 必须通过的核验：

1. **姓名格式**：2-4个汉字，且不包含以下词汇：
   - 一直致力、教授、硕士、博士、主任医师、副主任医师、友情链接
   - 大学名称（如：山东大学、北京医学院）
   - **职称前缀词：副、主（作为开头时）、副主任、副主**
   - **科室关键词：骨、内科、外科、妇科、产科、儿科、口腔、眼科、耳鼻喉、皮肤、精神、肿瘤**
   - **常见错误模式检测：**
     - 如果姓名以"副"、"主任"、"医师"结尾，说明姓名和职称没有分离
     - 如果姓名以"骨"、"内科"、"外科"等结尾，说明姓名和科室没有分离
     - 例如"张海玲副"、"王明副主任" 都是错误的，应重新提取
     - 例如"段永壮骨" 是错误的，姓名="段永壮"，科室="骨"

2. **医院名称**：必须包含目标医院名称

3. **URL格式**：必须是有效的医生详情页URL
4. **科室名称**：必须与医院官网的科室名一致
5. **职称准确性**：必须与页面显示完全一致
   - 如果页面显示"副主任医师"，提取结果必须是"副主任医师"，不能是"主任医师"
   - 如果页面显示"主任医师"，提取结果必须是"主任医师"
   - 注意"副"字不能遗漏：副主任医师 ≠ 主任医师
   - 常见错误：把"副主任医师"误提取为"主任医师"

### 核验失败的处理：
- 记录失败原因和URL
- 不推送到队列
- 输出警告日志
- 重新理解详情页的内容，重新提取内容，并发送到队列
---

## 六、错误处理

1. **网络错误**：重试3次，间隔递增（1s, 2s, 4s）
2. **解析错误**：记录异常，跳过该医生，继续下一个
3. **推送失败**：本地备份，记录失败列表
4. **分页遍历**：设置最大页数限制（如50页），防止无限循环

---

# 七、网页内容提取策略

### 1. 页面类型识别与选择器策略

**表格布局特征：**

典型HTML结构：
```html
<table class="doctor-list-table">
    <thead>
        <tr>
            <th>医生姓名</th>
            <th>科室</th>
            <th>职称</th>
            <th>专业特长</th>
        </tr>
    </thead>
    <tbody>
        <tr class="doctor-row">
            <td class="doctor-name"><a href="/doctor/1001">张华</a></td>
            <td class="department">心血管内科</td>
            <td class="title">主任医师</td>
            <td class="specialty">冠心病、高血压</td>
        </tr>
    </tbody>
</table>
```

XPath选择器：
```
基础路径：//table[contains(@class, 'doctor-list')]//tbody/tr
姓名：./td[contains(@class, 'name')]//a/text()
科室：./td[contains(@class, 'department')]/text()
职称：./td[contains(@class, 'title')]/text()
详情链接：./td[contains(@class, 'name')]//a/@href
```

**卡片布局特征：**

典型HTML结构：
```html
<div class="doctor-list">
    <div class="doctor-card">
        <div class="card-header">
            <img class="avatar" src="/images/doctor/2001.jpg"/>
            <div class="basic-info">
                <h3 class="name">王建国</h3>
                <span class="title">主任医师</span>
            </div>
        </div>
        <div class="card-body">
            <div class="dept-value">骨科</div>
            <div class="specialty-value">颈椎病、腰椎间盘突出</div>
        </div>
        <div class="card-footer">
            <a href="/doctor/detail/2001" class="btn-detail">查看详情</a>
        </div>
    </div>
</div>
```

XPath选择器：
```
基础路径：//div[contains(@class, 'doctor-list')]//div[contains(@class, 'doctor-card')]
姓名：./div[contains(@class, 'header')]//h3/text()
职称：./div[contains(@class, 'header')]//span[contains(@class, 'title')]/text()
科室：./div[contains(@class, 'body')]//div[contains(@class, 'dept')]/text()
详情链接：./div[contains(@class, 'footer')]//a/@href
```

**通用选择器（兜底方案）：**
```
姓名：//h1/text() | //h2[contains(@class, 'name')]/text()
科室：//div[contains(@class, 'department')]/text() | //a[contains(@class, 'dept')]/text()
职称：//span[contains(@class, 'title')]/text() | //div[contains(@class, 'zhicheng')]/text()
简介：//div[contains(@class, 'intro')]//text() | //div[contains(@class, 'desc')]//text()
擅长：//div[contains(@class, 'specialty')]//text() | //div[contains(@class, 'good-at')]//text()
```

**键值对布局特征（非常重要！）：**

页面以"字段名：值"形式展示，常见HTML结构：
```html
<div class="doctor-info">
    <div class="info-item">
        <span class="label">学　　历：</span>
        <span class="value">本科</span>
    </div>
    <div class="info-item">
        <span class="label">开诊科室：</span>
        <span class="value">全科医学科</span>
    </div>
    <div class="info-item">
        <span class="label">职称/职务：</span>
        <span class="value">主任医师</span>
    </div>
    <div class="info-item">
        <span class="label">工作年限：</span>
        <span class="value">从事临床工作30余年</span>
    </div>
    <div class="info-item">
        <span class="label">擅　　长：</span>
        <span class="value">擅长糖尿病、甲状腺病、冠心病、高血压、胃肠道疾病的诊断和治疗</span>
    </div>
</div>
```

**键值对提取策略：**
1. 遍历所有包含"键：值"格式的元素
2. 根据键名（label）匹配对应字段
3. 提取值（value）部分

**键名匹配规则：**
```
学历相关键名：学历、教育程度、最高学历
科室相关键名：开诊科室、科室、所属科室、门诊科室
职称相关键名：职称、职务、职称/职务、专业技术职务
年限相关键名：工作年限、从业年限、临床工作年限
擅长相关键名：擅长、专业特长、诊疗特长、主治疾病
毕业院校键名：毕业院校、毕业学校、院校
学位键名：学位、学术学位
专业方向键名：专业方向、专业领域、研究方向
```

**提取方法：**
- 正则匹配：`(.+?)[:：]\s*(.+)` 提取键和值
- 或使用选择器：查找包含特定文本的元素，然后提取相邻的值元素

**从业务经历/工作经历文本中提取毕业院校：**
业务经历区域常见格式：
```
业务经历
1988年河南医科大学临床医学系毕业后留校工作至今。2007年获得博士学位。
```
提取方法：
1. 查找"业务经历"、"工作经历"、"学习经历"等标题下的内容
2. 使用正则匹配：`(\d{4}年)?(.+?大学|.+?学院|.+?医科大学|.+?医学院).+?毕业`
3. 提取院校名称部分作为 alma_mater

**业务经历时间段格式（常见）：**
```
业务经历
1981-1984 新乡医学院
1984-1997郑州市第五人民医院工作，历任住院医师、主治医师
1997-2000郑州大学第一附属医院妇产科硕士研究生
2003-2006郑州大学妇科肿瘤专业博士研究生
```
提取规则：
- 时间段 + 空格 + 院校名 → alma_mater
- 时间段 + 院校名 + 硕士研究生 → education="硕士", alma_mater="院校名"
- 时间段 + 院校名 + 博士研究生 → education="博士", alma_mater="院校名"
- 示例：`1981-1984 新乡医学院` → alma_mater="新乡医学院"
- 示例：`2003-2006郑州大学妇科肿瘤专业博士研究生` → education="博士", alma_mater="郑州大学"

**从个人简介文本中提取学历（常见）：**
简介中常见格式：
```
个人简介
阎宗毅，副主任医师，研究生学历，山东省男科委员...
张梦真，女，教授，博士，主任医师，硕士生导师...
```
提取规则：
- 直接学历词：博士、硕士、本科、大专
- 学历组合词：研究生学历 → education="研究生"
- 示例：`阎宗毅，副主任医师，研究生学历` → education="研究生"
- 示例：`张梦真，女，教授，博士，主任医师` → education="博士"
- 注意：`研究生学历` = `研究生`（不是硕士）

**从个人简介文本中提取工作年限（常见）：**
简介中常见格式：
```
个人简介
王雅栋，首都医科大学神经外科博士，从事神经外科近20年，曾在SCI、中华系列杂志发表过文章10余篇。
```
提取规则：
- 模式：`从事...XX年`、`从业XX年`、`临床工作XX年`
- 示例：`从事神经外科近20年` → years_of_experience=20
- 示例：`从事临床工作30余年` → years_of_experience=30
- 示例：`从业20余年` → years_of_experience=20
- 注意：忽略"近"、"余"等修饰词，只提取数字

### 2. 分页机制处理

**传统页码分页：**
```html
<div class="pagination">
    <a href="?page=1" class="prev">上一页</a>
    <a href="?page=1">1</a>
    <span class="current">2</span>
    <a href="?page=3">3</a>
    <a href="?page=3" class="next">下一页</a>
</div>
```

处理策略：
- 提取最大页码：`//div[@class='pagination']//a[last()-1]/text()`
- 构造分页URL：替换page参数
- 或点击分页按钮，等待新数据加载

**滚动加载：**
处理策略：
- 滚动到底部：`window.scrollTo(0, document.body.scrollHeight)`
- 等待新数据加载：监控元素数量变化
- 监控网络请求，找到数据API接口
- 循环直到没有新数据加载

**点击加载更多：**
```html
<button class="load-more-btn" onclick="loadMore()">加载更多</button>
```

处理策略：
- 点击按钮：`page.click('.load-more-btn')`
- 等待新元素：`page.wait_for_selector('.doctor-item:nth-child(20)')`
- 循环直到按钮消失或禁用

### 3. 动态内容处理

**识别动态加载：**
- 页面初始HTML内容很少
- 网络面板显示XHR/Fetch请求
- 元素在滚动或点击后出现

**Playwright处理策略：**
```python
# 等待元素加载
page.wait_for_selector('.doctor-item', timeout=10000)

# 滚动触发懒加载
for i in range(5):  # 滚动5次
    page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
    page.wait_for_timeout(1000)

# 点击展开按钮
expand_btn = page.query_selector('.expand-btn')
if expand_btn:
    expand_btn.click()
    page.wait_for_timeout(500)

# 网络拦截获取API数据
def handle_response(response):
    if '/api/doctor' in response.url:
        data = response.json()
        # 直接使用API返回的JSON数据

page.on('response', handle_response)
```



## 八、注意事项

1. **只从官网获取数据**，不要查找第三方网站
2. **抓取所有可见医生**，不遗漏任何科室
3. **字段缺失时用 null 填充**，不要编造数据
4. **结构化字段提取**，提取字段要充分

---
"""


copaw_client: Optional[CoPawClient] = None
processed_count = 0
my_consumer_id = f"scraper_{uuid.uuid4().hex[:8]}"


def get_rabbitmq_connection():
    parameters = pika.URLParameters(RABBITMQ_URL)
    parameters.heartbeat = 600
    parameters.blocked_connection_timeout = 7200
    connection = pika.BlockingConnection(parameters)
    return connection


def safe_ack(channel, delivery_tag, requeue=False):
    """
    安全确认消息

    Args:
        channel: RabbitMQ channel
        delivery_tag: 消息的 delivery tag
        requeue: 是否重新入队（False=确认删除，True=nack并重新入队）

    Returns:
        bool: 是否成功确认
    """
    if channel is None:
        print(f"⚠️  Channel 为 None，无法确认消息 (delivery_tag={delivery_tag})")
        sys.stdout.flush()
        return False

    try:
        if requeue:
            channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
            print(f"📤 消息已 nack 并重新入队 (delivery_tag={delivery_tag})")
        else:
            channel.basic_ack(delivery_tag=delivery_tag)
            print(f"✅ 消息已 ack 确认删除 (delivery_tag={delivery_tag})")
        sys.stdout.flush()
        return True

    except Exception as e:
        print(f"❌ RabbitMQ ack 操作失败: {type(e).__name__}: {e}")
        print(f"   delivery_tag={delivery_tag}, requeue={requeue}")
        sys.stdout.flush()
        return False


def process_hospital(channel, hospital_data: Dict[str, Any], delivery_tag):
    global copaw_client, processed_count

    hospital_name = hospital_data.get("hosp_name", "")
    stand_name = hospital_data.get("stand_name", hospital_name)
    cache_url = hospital_data.get("cache_url", "")
    trigger_dataid = hospital_data.get("trigger_dataid", "")
    person_count = hospital_data.get("person_count", 0)

    print(f"\n{'=' * 70}")
    print(f"🏥 [{my_consumer_id}] 开始处理医院")
    print(f"{'=' * 70}")
    print(f"  医院名称: {hospital_name}")
    print(f"  标准名称: {stand_name}")
    print(f"  缓存 URL: {cache_url}")
    print(f"  触发ID: {trigger_dataid or '无'}")
    print(f"  人员数量: {person_count}")
    print(f"  Delivery Tag: {delivery_tag}")
    print(f"{'=' * 70}\n")
    sys.stdout.flush()

    if not hospital_name:
        print("❌ 医院名称为空，跳过")
        safe_ack(channel, delivery_tag)
        return

    if copaw_client is None:
        print("❌ CoPaw 客户端未初始化")
        safe_ack(channel, delivery_tag, requeue=True)
        return

    prompt = PROMPT_TEMPLATE.replace("__HOSPITAL_NAME__", hospital_name)
    prompt_preview = prompt[:200].replace("\n", " ")
    print(f"📝 Prompt 预览: {prompt_preview}...")
    print(f"📏 Prompt 总长度: {len(prompt)} 字符\n")
    sys.stdout.flush()

    start_time = time.time()
    tool_calls = 0
    content_length = 0
    errors = []

    try:
        print("🤖 发送任务到 CoPaw Agent...")
        print(f"   API 地址: {copaw_client.config.base_url}")
        print(f"   超时时间: {copaw_client.config.timeout} 秒")
        print("-" * 70)
        sys.stdout.flush()

        events = copaw_client.chat(prompt)

        print("\n📥 接收 CoPaw 响应:\n")
        sys.stdout.flush()

        for event in events:
            event_type = event.get("type", "unknown")

            if event_type == "thinking_start":
                msg_id = event.get("data", {}).get("msg_id")
                print(f"\n💭 Agent 开始思考... (msg_id: {msg_id})")
                sys.stdout.flush()

            elif event_type == "content_delta":
                text = event.get("data", {}).get("delta", "")
                if text:
                    print(text, end="", flush=True)
                    content_length += len(text)

            elif event_type == "tool_call":
                tool_calls += 1
                tool_data = event.get("data", {})
                tool_name = tool_data.get("name", "unknown")
                tool_args = tool_data.get("arguments", {})
                print(f"\n\n🔧 [{tool_calls}] 工具调用: {tool_name}")
                if tool_args:
                    print(f"   参数: {json.dumps(tool_args, ensure_ascii=False)[:200]}")
                sys.stdout.flush()

            elif event_type == "tool_result":
                tool_data = event.get("data", {})
                result_preview = json.dumps(tool_data, ensure_ascii=False)[:200]
                print(f"   ✅ 工具结果: {result_preview}...\n")
                sys.stdout.flush()

            elif event_type == "error":
                error_msg = event.get("data", "未知错误")
                errors.append(error_msg)
                print(f"\n❌ 错误: {error_msg}")
                sys.stdout.flush()

            elif event_type == "response":
                status = event.get("data", {}).get("status", "unknown")
                if status == "completed":
                    print("\n\n✅ Agent 响应完成")
                else:
                    print(f"\n📊 响应状态: {status}")
                sys.stdout.flush()

        elapsed_time = time.time() - start_time

        print("\n" + "=" * 70)
        print(f"📊 任务统计:")
        print(f"  - 处理时间: {elapsed_time:.2f} 秒")
        print(f"  - 输出长度: {content_length} 字符")
        print(f"  - 工具调用: {tool_calls} 次")
        print(f"  - 错误数量: {len(errors)} 次")
        print("=" * 70)
        sys.stdout.flush()

        ack_success = safe_ack(channel, delivery_tag)
        if ack_success:
            processed_count += 1
            print(f"📊 [{my_consumer_id}] 总计已处理: {processed_count} 条消息\n")
        else:
            print(
                f"⚠️  消息确认失败，消息可能仍留在队列中 (delivery_tag={delivery_tag})\n"
            )
        sys.stdout.flush()

    except KeyboardInterrupt:
        print("\n⚠️  用户中断任务")
        print(f"⏱️  已运行: {time.time() - start_time:.2f} 秒")
        sys.stdout.flush()
        safe_ack(channel, delivery_tag, requeue=True)
        raise
    except RequestException as e:
        elapsed = time.time() - start_time
        print(f"\n❌ CoPaw 请求失败 (运行 {elapsed:.2f} 秒)")
        print(f"   错误类型: {type(e).__name__}")
        print(f"   错误信息: {str(e)}")
        sys.stdout.flush()
        safe_ack(channel, delivery_tag, requeue=True)
    except BrokenPipeError as e:
        elapsed = time.time() - start_time
        print(f"\n❌ 连接断开 (运行 {elapsed:.2f} 秒)")
        print(f"   错误: {str(e)}")
        sys.stdout.flush()
        safe_ack(channel, delivery_tag, requeue=True)
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"\n❌ 处理任务时出错 (运行 {elapsed:.2f} 秒)")
        print(f"   错误类型: {type(e).__name__}")
        print(f"   错误信息: {str(e)}")
        import traceback

        print(f"   堆栈追踪:\n{traceback.format_exc()}")
        sys.stdout.flush()
        safe_ack(channel, delivery_tag, requeue=True)


def callback(ch, method, properties, body):
    try:
        raw_body = body.decode("utf-8")
        hospital_data = json.loads(raw_body)

        print(f"\n{'🔻' * 35}")
        print(f"📨 收到新消息")
        print(f"   Delivery Tag: {method.delivery_tag}")
        print(f"   Routing Key: {method.routing_key}")
        print(f"   消息大小: {len(raw_body)} 字节")
        print(f"{'🔻' * 35}")
        sys.stdout.flush()

        process_hospital(ch, hospital_data, method.delivery_tag)
        sys.stdout.flush()

    except json.JSONDecodeError as e:
        print(f"\n{'❌' * 35}")
        print(f"❌ JSON 解析失败")
        print(f"   错误位置: 行 {e.lineno}, 列 {e.colno}")
        print(f"   错误信息: {e.msg}")
        print(f"   原始内容: {body.decode('utf-8')[:200]}...")
        print(f"{'❌' * 35}")
        sys.stdout.flush()
        safe_ack(ch, method.delivery_tag, requeue=False)
    except Exception as e:
        print(f"\n{'❌' * 35}")
        print(f"❌ 处理消息时出错")
        print(f"   错误类型: {type(e).__name__}")
        print(f"   错误信息: {str(e)}")
        print(f"{'❌' * 35}")
        sys.stdout.flush()
        safe_ack(ch, method.delivery_tag, requeue=True)


def check_queue_status(channel, queue_name: str):
    result = channel.queue_declare(queue=queue_name, durable=True, passive=True)
    message_count = result.method.message_count
    consumer_count = result.method.consumer_count

    print(f"📊 队列状态:")
    print(f"  - 待处理消息: {message_count}")
    print(f"  - 活跃消费者: {consumer_count}")
    sys.stdout.flush()

    if consumer_count > 1:
        print(f"\n⚠️  警告: 检测到 {consumer_count - 1} 个其他消费者!")
        print("   消息可能被其他消费者抢走")
        print(f"   本消费者ID: {my_consumer_id}")
        sys.stdout.flush()

    return message_count, consumer_count


def ensure_queue_exists(channel, queue_name: str):
    channel.queue_declare(queue=queue_name, durable=True)


def main():
    global copaw_client, processed_count

    print("\n" + "=" * 70)
    print("🚀 医生信息抓取器启动")
    print("=" * 70)
    print(f"📍 消费者 ID: {my_consumer_id}")
    print(f"📍 RabbitMQ URL: {RABBITMQ_URL.replace('@', '***@')}")
    print(f"📥 消费队列: {CACHEHOSP_QUEUE}")
    print(f"🤖 CoPaw API: {COPAW_BASE_URL}")
    print(f"⚙️  Prefetch Count: 10 (高优先级模式)")
    print(f"⏱️  心跳间隔: 600 秒")
    print(f"⏱️  连接超时: 7200 秒")
    print("=" * 70)
    sys.stdout.flush()

    config = CoPawConfig(base_url=COPAW_BASE_URL, timeout=3600.0)
    copaw_client = CoPawClient(config)

    print("\n🔍 检查 CoPaw API 连接...")
    sys.stdout.flush()

    if not copaw_client.health_check():
        print(f"\n❌ 无法连接到 CoPaw API: {COPAW_BASE_URL}")
        print("   请检查:")
        print("   1. CoPaw 服务是否运行")
        print("   2. API 地址是否正确")
        print("   3. 网络连接是否正常")
        sys.exit(1)

    print("✅ CoPaw API 连接成功\n")
    sys.stdout.flush()

    retry_count = 0

    while True:
        connection = None
        try:
            retry_count += 1
            print(f"\n🔄 连接 RabbitMQ (第 {retry_count} 次)...")
            sys.stdout.flush()

            connection = get_rabbitmq_connection()
            channel = connection.channel()

            channel.basic_qos(prefetch_count=10)

            ensure_queue_exists(channel, CACHEHOSP_QUEUE)
            message_count, consumer_count = check_queue_status(channel, CACHEHOSP_QUEUE)

            consumer_tag = channel.basic_consume(
                queue=CACHEHOSP_QUEUE,
                on_message_callback=callback,
                consumer_tag=my_consumer_id,
                auto_ack=False,
            )

            print(f"\n✅ RabbitMQ 连接成功")
            print(f"   消费者标签: {consumer_tag}")
            print(f"   队列: {CACHEHOSP_QUEUE}")
            print(f"   待处理消息: {message_count}")
            print(f"   活跃消费者: {consumer_count}")

            if consumer_count > 1:
                print(f"\n⚠️  警告: 检测到 {consumer_count - 1} 个其他消费者!")
                print("   消息可能被竞争分配")

            print(f"\n📊 已处理消息总数: {processed_count}")
            print("\n" + "=" * 70)
            print("🟢 开始监听队列消息... (按 Ctrl+C 停止)")
            print("=" * 70 + "\n")
            sys.stdout.flush()

            channel.start_consuming()

        except KeyboardInterrupt:
            print("\n\n" + "=" * 70)
            print("⚠️  用户中断，正在停止...")
            print(f"📊 本次会话处理消息: {processed_count} 条")
            print("=" * 70)
            sys.stdout.flush()
            break
        except AMQPConnectionError as e:
            print(f"\n❌ RabbitMQ 连接错误")
            print(f"   错误: {str(e)}")
            print(f"   ⏳ 5秒后重试... (重试次数: {retry_count})")
            sys.stdout.flush()
            time.sleep(5)
        except AMQPChannelError as e:
            print(f"\n❌ RabbitMQ Channel 错误")
            print(f"   错误: {str(e)}")
            print(f"   ⏳ 5秒后重连... (重试次数: {retry_count})")
            sys.stdout.flush()
            time.sleep(5)
        except Exception as e:
            print(f"\n❌ 未知错误")
            print(f"   错误类型: {type(e).__name__}")
            print(f"   错误信息: {str(e)}")
            print(f"   ⏳ 5秒后重试... (重试次数: {retry_count})")
            sys.stdout.flush()
            time.sleep(5)
        finally:
            if connection and connection.is_open:
                try:
                    connection.close()
                    print("🔌 RabbitMQ 连接已关闭")
                    sys.stdout.flush()
                except Exception:
                    pass


if __name__ == "__main__":
    main()
