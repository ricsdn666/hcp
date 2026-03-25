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
#使用CoPaw v0.1.0.post1
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

 **基础信息区**（顶部或左侧）：
   - 医生姓名（h1/h2标题）
   - 职称（标签或介绍文本）
   - 所属科室（链接或文本），优先选择页面展示的科室名，而不是导航科室名
   - 行政职务（如"科室主任"）
   - 医生照片

**专业特长区**：
   - 擅长疾病列表
   - 诊疗范围描述
   - 特色技术

**个人简介区**：
   - 教育背景（毕业院校、学历、学位）
   - 工作经历
   - 从业年限
   - 学术任职
   - 科研成果


**字段提取规则：**

**必须准确提取的字段：**
- 医生姓名：2-4个汉字，从标题或首行提取
- 科室：展示科室名，几乎所有的医生详情页都有
- 简介：完整的个人介绍原文
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

2. **医院名称**：必须包含目标医院名称

3. **URL格式**：必须是有效的医生详情页URL
4. **科室名称**：必须与医院官网的科室名一致
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
    try:
        if channel and channel.is_open:
            if requeue:
                channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
            else:
                channel.basic_ack(delivery_tag=delivery_tag)
            return True
    except Exception as e:
        print(f"⚠️  RabbitMQ 操作失败: {e}")
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

        if safe_ack(channel, delivery_tag):
            processed_count += 1
            print(f"\n✅ 消息已确认 (Delivery Tag: {delivery_tag})")
            print(f"📊 [{my_consumer_id}] 总计已处理: {processed_count} 条消息\n")
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
