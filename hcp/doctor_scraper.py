#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
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

# 使用 CoPaw v0.1.0.post1
RABBITMQ_URL = os.getenv(
    "RABBITMQ_URL", "amqp://guest:147258369aB@r.pisiewang.top:45672/"
)
CACHEHOSP_QUEUE = "CacheHosp"

PROMPT_TEMPLATE = r"""
对 __HOSPITAL_NAME__ 官网进行抓取，提取该医院所有可见医生的详细信息，经过结构化字段提取和数据核验后，将结果逐条实时推送到 RabbitMQ 队列。

**重要：抓取完成后必须将结果推送到 DoctorResult 队列！**

**极其重要：必须抓取医生详情页，严禁抓取列表页！**
- ❌ 列表页特征：包含多个医生卡片/头像/条目（如 `/expertIntroduction/list`）
- ✅ 详情页特征：单个医生的完整介绍页面（如 `/doctor/123`、`/expert/detail/456`）

---

## 一、执行步骤（按顺序执行）

### 1. 探索官网结构
- 先通过搜索引擎找到官网正确地址
- 分析网站导航，找到"专家介绍"、"医生团队"、"专家团队"、"科室医生"等入口
- **识别医生列表页的布局类型**（重要）：
  - 表格布局：医生信息以行列表格展示
  - 卡片布局：每个医生独立卡片形式
  - 混合布局：上述两种的结合
- **区分列表页和详情页的 URL 模式**（关键）：
  - 列表页通常包含：`list`、`/doctor/`（带科室参数）、`/expert/`（带分页参数）
  - 详情页通常包含：`detail`、`/doctor/数字 ID`、`/expert/数字 ID`、`/view/`
  - **重点警惕**：URL 中包含 `#/` 的 SPA 页面，需要进一步分析 hash 路由
- 确认是否存在分页机制（传统分页/滚动加载/点击加载更多）
**DOM 结构分析：**
- 查看页面 HTML 结构
- 记录医生列表的容器元素（class/id）
- 记录各字段的选择器路径
- 检查是否存在动态加载内容（AJAX/JavaScript 渲染）

### 1.5. 识别并避免列表页（关键步骤）

**列表页特征识别：**
- 页面包含多个医生信息块（通常 10 个以上）
- URL 包含 `/list`、`/index`、`/all` 等关键词
- 页面标题包含"列表"、"大全"、"所有"等词汇
- 每个医生信息都包裹在可点击的卡片/链接中

**详情页特征识别：**
- 页面只展示一个医生的完整信息
- 包含医生详细介绍：姓名、科室、职称、简介、擅长领域、出诊时间等
- URL 通常包含医生 ID（数字）或医生姓名（拼音）
- 页面标题格式："{医生姓名}-{科室}-{医院名称}"

**遇到列表页的处理策略：**
1. **点击医生卡片**：逐个点击医生卡片，进入详情页
2. **提取详情页 URL**：从卡片的 `href` 属性获取详情页链接
3. **追踪跳转**：观察点击后的 URL 变化，记录详情页 URL 模式
4. **批量收集**：收集所有详情页 URL 后，逐个访问抓取信息

**列表页 → 详情页 典型模式：**
```
列表页：https://www.example.com/#/guideline/expertIntroduction/list
详情页：https://www.example.com/#/guideline/expertDetail/123
         https://www.example.com/#/doctor/view/456
         https://www.example.com/expert/detail?id=789
```

### 2. 抓取医生列表

**根据布局类型选择提取策略：**

**表格布局提取：**
- 定位表格容器：`table.doctor-list-table` 或类似
- 遍历每行：`tbody/tr`
- 提取字段：姓名（第 1 列）、科室（第 2 列）、职称（第 3 列）等
- 收集详情链接：通常在姓名列的 `<a>` 标签

**卡片布局提取：**
- 定位卡片容器：`div.doctor-list` 或 `div.doctor-cards`
- 遍历每个卡片：`div.doctor-card`
- 提取字段：从卡片的各个子元素提取
- 收集详情链接：通常在卡片标题或"查看详情"按钮

**分页处理：**
- 传统分页：点击页码按钮，观察 URL 变化规律
- 滚动加载：滚动到底部，监控网络请求中的数据接口
- 点击加载：点击"加载更多"按钮，分析请求参数

**遍历策略：**
- 遍历所有科室/专业分类，确保不遗漏
- 记录每个科室的医生数量，用于验证完整性
- 收集所有医生详情页 URL，去重后保存

**关键验证**：收集 URL 后，随机抽查 3-5 个 URL，确认是详情页而非列表页


### 3. 抓取医生详情页并核验
- **逐页访问每个医生详情页 URL**
- **验证当前页面为详情页**：
  - 页面只包含一个医生的信息
  - 包含详细介绍内容（不是简单卡片）
  - 页面标题包含医生姓名
- 提取完整信息后，验证数据合法性：
  - 重点检查科室名，是否与医院官网的科室名一致
  - 检查是否有"返回列表"、"上一篇/下一篇"等详情页导航元素
- 推送到 RabbitMQ 队列
- 本地备份（防止推送失败）

---

## 二、队列配置

```
RABBITMQ_URL: amqp://guest:147258369aB@r.pisiewang.top:45672/
QUEUE_NAME: DoctorResult
```

**重要说明：**
- 抓取到的每个医生信息必须立即推送到 DoctorResult 队列
- 不要等所有医生抓取完成后再批量推送
- 每抓取一个医生就推送一个，确保数据实时性
- 推送前验证数据完整性（姓名、科室、职称必填）
---

## 三、推送数据格式

```json
{
  "name": "医生姓名",
  "hospital": "医院全称",
  "origin_hosp": "__HOSPITAL_NAME__",
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

1. **医院名称**：必须包含目标医院名称

2. **URL 格式**：必须是有效的医生详情页 URL

3. **详情页验证**（关键）：
   - 页面标题应包含医生姓名
   - 页面只包含一个医生的完整信息
   - 不应包含多个医生卡片/列表项
   - URL 不应包含 `/list`、`/index`、`/all` 等列表页关键词
   - 如果是 SPA 应用（URL 含 `#/`），hash 路由应包含 `detail`、`view`、`info` 等详情关键词

4. **数据完整性核验**：
   - 至少包含以下字段之一：姓名、科室、职称
   - `intro` 或 `specialty` 至少有一个不为空
   - 如果有详情页链接，应与抓取时的 URL 一致

### 核验失败的处理：
- 记录失败原因和 URL
- 不推送到队列
- 输出警告日志
- **如果发现是列表页而非详情页**：
  - 重新分析该 URL 的页面结构
  - 识别出医生卡片/列表项
  - 点击卡片进入真正的详情页
  - 重新提取内容，并发送到队列
---

## 六、错误处理

1. **网络错误**：重试3次，间隔递增（1s, 2s, 4s）
2. **解析错误**：记录异常，跳过该医生，继续下一个
3. **推送失败**：本地备份，记录失败列表
4. **分页遍历**：设置最大页数限制（如50页），防止无限循环

---

# 七、网页内容提取策略

### 1. 分页机制处理

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
   - **禁止抓取的第三方平台**：
     - 好大夫在线 (haodf.com)
     - 微医/挂号网 (weiyi.com, guahao.com)
     - 京东健康 (jd.com)
     - 平安健康 (pingan.com)
     - 阿里健康 (alihealth.com)
     - 春雨医生 (chunyu.me)
     - 丁香医生 (dxy.cn)
     - 有来医生 (youlaiyixue.com)
     - 其他非医院官方网站
   - **只抓取医院官方网站的医生信息**
   - **如果官网没有某个医生的信息，不要从第三方平台补充**
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
        print(f"   API 地址：{copaw_client.config.base_url}")
        print(f"   超时时间：{copaw_client.config.timeout} 秒")
        print("-" * 70)
        sys.stdout.flush()

        # 消息立即 ack（出队），不等待 CoPaw 完成
        ack_success = safe_ack(channel, delivery_tag)
        if ack_success:
            processed_count += 1
            print(f"\n✅ 消息已确认出队 (Delivery Tag: {delivery_tag})")
            print(f"📊 [{my_consumer_id}] 总计已处理：{processed_count} 条消息\n")
        else:
            print(f"\n⚠️  消息确认失败 (delivery_tag={delivery_tag})\n")
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
                print(f"\n\n🔧 [{tool_calls}] 工具调用：{tool_name}")
                if tool_args:
                    print(f"   参数：{json.dumps(tool_args, ensure_ascii=False)[:200]}")
                sys.stdout.flush()

            elif event_type == "tool_result":
                tool_data = event.get("data", {})
                result_preview = json.dumps(tool_data, ensure_ascii=False)[:200]
                print(f"   ✅ 工具结果：{result_preview}...\n")
                sys.stdout.flush()

            elif event_type == "error":
                error_msg = event.get("data", "未知错误")
                errors.append(error_msg)
                print(f"\n❌ 错误：{error_msg}")
                sys.stdout.flush()

            elif event_type == "response":
                status = event.get("data", {}).get("status", "unknown")
                if status == "completed":
                    print("\n\n✅ Agent 响应完成 - 准备消费下一个消息")
                else:
                    print(f"\n📊 响应状态：{status}")
                sys.stdout.flush()

        elapsed_time = time.time() - start_time

        print("\n" + "=" * 70)
        print(f"📊 任务统计:")
        print(f"  - 处理时间：{elapsed_time:.2f} 秒")
        print(f"  - 输出长度：{content_length} 字符")
        print(f"  - 工具调用：{tool_calls} 次")
        print(f"  - 错误数量：{len(errors)} 次")
        print("=" * 70)
        print("\n✅ 当前消息处理完成，等待下一个消息...\n")
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

    # 解析命令行参数
    parser = argparse.ArgumentParser(
        description="医生信息抓取器 - 从 RabbitMQ 消费医院数据并抓取医生信息"
    )
    parser.add_argument(
        "--copaw-url",
        type=str,
        default="http://localhost:8088",
        help="CoPaw API 地址 (默认：http://localhost:8088)",
    )
    args = parser.parse_args()

    copaw_base_url = args.copaw_url

    print("\n" + "=" * 70)
    print("🚀 医生信息抓取器启动")
    print("=" * 70)
    print(f"📍 消费者 ID: {my_consumer_id}")
    print(f"📍 RabbitMQ URL: {RABBITMQ_URL.replace('@', '***@')}")
    print(f"📥 消费队列：{CACHEHOSP_QUEUE}")
    print(f"🤖 CoPaw API: {copaw_base_url}")
    print(f"⚙️  Prefetch Count: 10 (高优先级模式)")
    print(f"⏱️  心跳间隔：600 秒")
    print(f"⏱️  连接超时：7200 秒")
    print("=" * 70)
    sys.stdout.flush()

    config = CoPawConfig(base_url=copaw_base_url, timeout=3600.0)
    copaw_client = CoPawClient(config)

    print("\n🔍 检查 CoPaw API 连接...")
    sys.stdout.flush()

    if not copaw_client.health_check():
        print(f"\n❌ 无法连接到 CoPaw API: {copaw_base_url}")
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
