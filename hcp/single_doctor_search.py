#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import json
import os
import sys
import time
import uuid
from typing import Dict, Any, Optional, List

### python doctor_scraper.py --copaw-url "http://localhost:8088"
import pika
from pika.exceptions import AMQPConnectionError, AMQPChannelError
from requests.exceptions import RequestException

from copaw_client import CoPawClient, CoPawConfig

# 使用 CoPaw v0.1.0.post1
RABBITMQ_URL = os.getenv(
    "RABBITMQ_URL", "amqp://guest:147258369aB@r.pisiewang.top:45672/"
)
CACHEHOSP_QUEUE = "SearchDoctorPro"

PROMPT_TEMPLATE = r"""

**⚠️ 重要限制（严格执行）：**
1. **只能使用百度搜索（https://www.baidu.com）**
2. **搜索次数不得超过5次**
3. **禁止使用其他搜索引擎或直接访问医疗平台**
4. **效率优先：快速抓取、快速推送**
5. **URL 必须验证：填写前访问 URL 确认页面包含医生，验证失败则留空**

---

搜索目标：

- 医生姓名：{__name__}
- 医院名称：{__hospital__}
- 科室名称：{__dept__}

---

**重要：抓取完成后必须将结果推送到 DoctorResult 队列！**
**找到信息立即推送，不要等待完整数据或反复验证！**


## 执行步骤

1. **搜索阶段**
   - **只使用百度搜索（https://www.baidu.com）**
   - **禁止使用其他搜索引擎（Google、必应、搜狗等）或医疗平台直接访问**
   - 搜索关键词："{__name__} {__hospital__} {__dept__}" 或 "{__name__} {__hospital__}"
   - **最多搜索5次，不超过5次**
   - 从百度搜索结果中点击相关链接获取医生信息
   - 优先点击医院官网、好大夫、百度健康等权威医疗网站的链接

2. **快速验证（简化版）**
   - 仅确认医生姓名是否匹配
   - 其他信息不做深度验证，直接提取即可
   - **不要浪费时间去核对医院全称、科室匹配等细节**

3. **输出阶段**
   - 快速提取信息并结构化为 JSON 格式
   - **URL 处理规则（严格执行）**：
     1. 如果找到医生详情页 URL，**必须访问验证**
     2. 验证方法：访问 URL，确认页面包含目标医生姓名
     3. **验证成功**：页面包含目标医生 → otherpropertys.url 填写该 URL
     4. **验证失败**：页面不包含目标医生 → 该 URL 无效，otherpropertys.url 必须留空
     5. **没有找到 URL**：otherpropertys.url 直接留空
   - **URL 无效或为空不影响其他数据推送**
   - **在搜索次数限制内，优先尝试找到有效的 URL**

---

## 字段定义

| 字段                    | 类型           | 说明                             | 枚举值                                                                                                                                                                                   |
| ----------------------- | -------------- | -------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| name                    | string         | 医生姓名（与页面标题一致）       | -                                                                                                                                                                                        |
| hospital                | string         | 医院全称                         | -                                                                                                                                                                                        |
| standard_department     | string         | 标准科室名                       | -                                                                                                                                                                                        |
| display_department      | string         | 展示科室名                       | -                                                                                                                                                                                        |
| title                   | string \| null | 职称                             | 主任医师/副主任医师/主治医师/主管医师/医师/医士/住院医师/助理医师/主任药师/副主任药师/主管药师/药师/药士/主任护师/副主任护师/主管护师/护师/护士/主任技师/副主任技师/主管技师/技师/技士等 |
| administrative_position | string \| null | 行政职务                         | 书记/院长/局长/所长/经理/站长/主任/科长/处长/主管/组长/护士长/总住院医师/负责人/支部书记/主任助理/带组医生/办公室主任/床位医生/进修医生/院长助理等                                       |
| academic_title          | string \| null | 学术头衔                         | 教授/副教授/讲师/助教/研究员/副研究员/助理研究员/研究实习员                                                                                                                              |
| education               | string \| null | 学历                             | 博士研究生/硕士研究生/本科/大专/中专/高中/初中及以下                                                                                                                                     |
| degree                  | string \| null | 学位                             | 博士/硕士/学士等                                                                                                                                                                         |
| alma_mater              | string \| null | 毕业院校（仅校名，不含院系专业） | -                                                                                                                                                                                        |
| years_of_experience     | int \| null    | 从业年限（无法确定时返回 null） | 数字，如 28。如无法获取则返回 null                                                                                                                                                                              |
| intro                   | string \| null | 医生简介原文                     | 完整原文                                                                                                                                                                                 |
| specialty               | string \| null | 擅长领域原文                     | 完整原文                                                                                                                                                                                 |
| **otherpropertys.url**  | **string**     | **医生详情页 URL（必须验证）**   | **必须访问验证页面包含目标医生，验证失败或不找到则留空。绝不填写无效URL**                                                                                                                  |
| **source_model**        | **string**     | **数据来源模型（固定值）**       | **必须固定为 "J9S"，不能修改为其他任何值**                                                                                                                                               |

---


## 注意事项

1. **数据真实性**
   - 必须基于实际搜索结果填写，不可编造
   - 未找到的字段填 `null`
   - 只使用百度搜索获取的数据
   
2. **搜索限制（严格执行）**
   - **只能使用百度搜索**
   - **搜索次数不得超过5次**
   - 禁止使用其他搜索引擎或直接访问医疗平台
   - 如5次搜索后仍未找到医生信息，推送已找到的部分数据
   
3. **效率优先原则**
   - **不要花太多时间验证数据准确性（医院、科室、职称等）**
   - 医院名称、科室名称提取即可，不必反复核对
   - 职称按页面显示填写即可，不必严格核对枚举值
   - **快速抓取、快速推送，效率优先**
   - **URL 必须验证有效：访问 URL 确认页面包含目标医生**

4. **特殊情况处理（简化版）**
   - 如医生不在指定医院，直接返回找到的医院信息即可，无需额外标注
   - 如科室不匹配，直接返回找到的科室信息即可，无需额外标注
   - 如信息不完整，直接推送已找到的信息即可
   - **不要浪费时间处理特殊情况，直接推送数据**
   - **即使找不到医生详情页 URL，只要能获取到医生信息（姓名、科室、职称等），依然推送数据到队列**
   - **如果5次搜索后仍未找到任何信息，推送空数据标记为未找到**

5. **从业年限处理（简化版）**
   - 页面有显示就填写，没有就返回 `null`
   - **不要花时间推算或计算从业年限**
   - **不要编造数据，无法确定时返回 null 即可**


## 二、队列配置

```
RABBITMQ_URL: amqp://guest:147258369aB@r.pisiewang.top:45672/
QUEUE_NAME: DoctorResult
```

**重要说明：**
- 抓取到的每个医生信息必须立即推送到 DoctorResult 队列
- **不要等待或反复验证（除 URL 外），找到信息立即推送**
- 每抓取一个医生就推送一个，确保数据实时性
- **只要有医生姓名，就可以推送数据**
- **URL 处理规则（严格执行）**：
  - **填写 URL 前必须验证：访问 URL，确认页面包含医生**
  - **验证成功**：可以填写该 URL
  - **验证失败**：URL 无效，必须留空
  - **未找到 URL**：直接留空
  - **绝不推送无效 URL**
- **URL 无效或为空不影响其他数据推送**
- **在 5 次搜索限制内优先找有效 URL，找不到则留空推送**
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
  "otherpropertys": "{\"confidence_name\": \"网站名称\", \"source_kind\": \"official\", \"seed_url\": \"网站首页URL\", \"root_domain\": \"域名\", \"url\": \"【必填】医生详情页完整URL\", \"match_score\": 1.0, \"page_record_time\": null, \"fetch_method\": \"playwright\"}",
  "source_model": "J9S"
}
```

**⚠️ 重要提醒：**
- `otherpropertys.url` **必须验证，绝不填写未验证的 URL**
- **验证流程**：
  1. 找到候选 URL → 2. 访问 URL → 3. 搜索医生姓名 → 4. 验证结果
- **验证成功**：页面包含医生 → 可以填写该 URL
- **验证失败**：页面不包含医生 → URL 无效，必须留空
- **未找到 URL**：直接留空
- **URL 无效或为空不影响其他数据推送**
- **绝不推送无效的 URL**
- **在 5 次搜索限制内优先找有效 URL**
- **`source_model` 字段值必须固定为 "J9S"，绝对不能修改为其他任何值**

---

## 四、数据核验规则

**核验原则：除 URL 外不做深度验证，效率优先**

1. **姓名检查**：至少有医生姓名（其他字段缺失不影响推送）

2. **URL 有效性验证（严格执行）**：
   - **填写 URL 前必须验证，绝不填写未验证的 URL**
   - 验证步骤：
     1. 找到候选 URL
     2. 访问该 URL
     3. 在页面中搜索医生姓名
     4. 页面包含医生姓名 → URL 有效，可以填写
     5. 页面不包含医生姓名 → URL 无效，必须留空
   - **验证失败时 URL 字段留空，绝不填写无效 URL**
   - **没有找到 URL 时字段留空**
   - **在 5 次搜索限制内优先找有效 URL，找不到则留空推送**

3. **source_model 固定值**：
   - **source_model 必须为 "J9S"**
   - 这是唯一必须严格验证的字段

**核验失败的处理：**
- 只有姓名完全为空才不推送
- **URL 无效或为空不影响推送，其他字段正常推送**
- **绝不推送无效的 URL**
- **其他任何字段缺失都不影响推送**
- 找到信息立即推送，效率优先
---

## 六、错误处理（效率优先）

1. **网络错误**：不重试，直接跳过，继续下一个搜索结果
2. **解析错误**：不处理，直接跳过，继续搜索
3. **URL 验证失败**：
   - URL 无效 → 字段留空
   - 在搜索次数内继续找其他链接验证
   - 超过搜索次数 → URL 留空，推送其他数据
4. **推送失败**：本地备份，立即处理下一个医生
5. **搜索次数限制**：严格控制在5次以内
6. **遇到任何问题**：不要浪费时间修复，直接推送已找到的数据或标记未找到

---


## 七、字段提取（简化版）

**快速提取原则：页面有什么就填什么，不要过度处理**

- 姓名：从页面标题或内容提取
- 医院：提取即可，不必核对全称
- 科室：提取即可，不必核对匹配
- 职称：按页面显示填写即可
- 简介/擅长：原文复制即可，不必整理
- 其他字段：有就填，没有就 null

---

## 八、注意事项

1. **效率优先原则**：
   - **快速抓取、快速推送**
   - **不要花时间验证数据准确性（医院、科室、职称等）**
   - **不要反复核对医院、科室等信息**
   - **找到信息立即推送，不要等待完整数据**

2. **URL 验证与处理（严格执行）**：
   - **填写 URL 前必须验证，绝不填写未验证的 URL**
   - **验证步骤**：访问 URL → 搜索医生姓名 → 确认包含
   - **验证成功**：页面包含医生 → 可以填写
   - **验证失败**：页面不包含医生 → URL 无效，必须留空
   - **未找到 URL**：直接留空
   - **绝不推送无效 URL，URL 无效或为空不影响其他数据推送**
   - **在 5 次搜索限制内优先找有效 URL，找不到则留空推送**

3. **字段缺失处理**：
   - 找到什么填什么，未找到填 `null`
   - 不要编造数据，但也不必追求完整

4. **搜索限制（严格执行）**：
   - **只能使用百度搜索**
   - **搜索次数不得超过5次**
   - **禁止使用其他搜索引擎或直接访问医疗平台**
   - **不要为了找有效 URL 而超过搜索限制**

5. **数据推送原则**：
   - **只要有姓名就推送**
   - **其他信息缺失不影响推送**
   - **不要等待完整数据**

6. **source_model 固定值**：
   - **必须固定为 "J9S"，不能修改**
   - 这是唯一需要严格验证的字段

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

    name = hospital_data.get("name", "")
    hospital = hospital_data.get("hospital", "")
    department = hospital_data.get("department", "")
    cache_url = hospital_data.get("cache_url", "")
    trigger_dataid = hospital_data.get("trigger_dataid", "")
    person_count = hospital_data.get("person_count", 0)

    print(f"\n{'=' * 70}")
    print(f"🏥 [{my_consumer_id}] 开始处理医院")
    print(f"{'=' * 70}")
    print(f"  医生姓名: {name}")
    print(f"  医院:{hospital}")

    print(f"  科室: {department}")
    print(f"  缓存 URL: {cache_url}")
    print(f"  触发ID: {trigger_dataid or '无'}")
    print(f"  人员数量: {person_count}")
    print(f"  Delivery Tag: {delivery_tag}")
    print(f"{'=' * 70}\n")
    sys.stdout.flush()

    if not name:
        print("❌ 医生名称为空，跳过")
        safe_ack(channel, delivery_tag)
        return

    if copaw_client is None:
        print("❌ CoPaw 客户端未初始化")
        safe_ack(channel, delivery_tag, requeue=True)
        return

    prompt = PROMPT_TEMPLATE.replace("__hospital__", hospital)
    prompt = prompt.replace("__dept__", department)
    prompt = prompt.replace("__name__", name)

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
        description="医生信息抓取器 - 从 RabbitMQ 消费数据并抓取医生信息"
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
