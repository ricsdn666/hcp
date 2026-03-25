#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CoPaw Agent Client - 与 AI Agent 交互的 Python 客户端

功能：
- 发送消息给 Agent
- 接收 Agent 的流式响应
- 支持多轮对话
- 支持重新连接到正在运行的任务

使用示例：
    # 基本使用
    python copaw_client.py "你好，帮我写一首诗"
    
    # 交互模式
    python copaw_client.py --interactive
    
    # 作为库使用
    from copaw_client import CoPawClient
    client = CoPawClient()
    response = client.chat("你好")
"""

import argparse
import json
import sys
import uuid
from dataclasses import dataclass, field
from typing import Optional, Generator, Any

import requests
from requests.exceptions import RequestException


@dataclass
class CoPawConfig:
    """CoPaw 客户端配置"""
    base_url: str = "http://127.0.0.1:8088"
    agent_id: str = "default"
    timeout: float = 3600.0  # 5 分钟超时，适合长任务
    

@dataclass 
class ChatSession:
    """聊天会话"""
    session_id: str
    user_id: str
    chat_id: Optional[str] = None
    history: list = field(default_factory=list)


class CoPawClient:
    """CoPaw Agent 客户端"""
    
    def __init__(self, config: Optional[CoPawConfig] = None):
        self.config = config or CoPawConfig()
        self.session: Optional[ChatSession] = None
        
    def _get_api_url(self, endpoint: str) -> str:
        """获取完整 API URL"""
        return f"{self.config.base_url}/api/{endpoint}"
    
    def create_session(self, user_id: Optional[str] = None) -> ChatSession:
        """创建新的聊天会话"""
        session_id = str(uuid.uuid4())
        user_id = user_id or f"python_user_{uuid.uuid4().hex[:8]}"
        
        # 通过 API 创建聊天记录
        try:
            response = requests.post(
                self._get_api_url("chats"),
                json={
                    "session_id": session_id,
                    "user_id": user_id,
                    "channel": "console",
                    "name": "Python Client Chat"
                },
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                chat_id = data.get("id")
            else:
                chat_id = None
        except RequestException:
            chat_id = None
            
        self.session = ChatSession(
            session_id=session_id,
            user_id=user_id,
            chat_id=chat_id
        )
        return self.session
    
    def get_session(self) -> ChatSession:
        """获取当前会话，如果没有则创建"""
        if self.session is None:
            self.create_session()
        return self.session
    
    def chat(
        self, 
        message: str, 
        session_id: Optional[str] = None,
        user_id: Optional[str] = None,
        stream: bool = True,
        reconnect: bool = False
    ) -> Generator[dict, None, None]:
        """
        发送消息并获取响应
        
        Args:
            message: 要发送的消息
            session_id: 会话 ID（可选，用于继续现有对话）
            user_id: 用户 ID（可选）
            stream: 是否流式返回
            reconnect: 是否重新连接到正在运行的任务
            
        Yields:
            响应事件字典，包含 type 和 data 字段
        """
        session = self.get_session()
        
        # 使用提供的参数或会话默认值
        sid = session_id or session.session_id
        uid = user_id or session.user_id
        
        # 使用正确的 API 格式（OpenAI 风格）
        payload = {
            "input": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": message
                        }
                    ]
                }
            ],
            "session_id": sid,
            "user_id": uid,
            "channel": "console",
            "stream": True,
            "reconnect": reconnect
        }
        
        # 记录到历史
        session.history.append({"role": "user", "content": message})
        
        try:
            response = requests.post(
                self._get_api_url("console/chat"),
                json=payload,
                stream=True,
                timeout=self.config.timeout,
                headers={"Accept": "text/event-stream"}
            )
            response.raise_for_status()
            
            assistant_message = ""
            last_msg_id = None
            
            for line in response.iter_lines(decode_unicode=True):
                if not line:
                    continue
                    
                # SSE 格式: data: {...}
                if line.startswith("data: "):
                    try:
                        data = json.loads(line[6:])
                        obj_type = data.get("object", "unknown")
                        content_type = data.get("type", "unknown")
                        msg_id = data.get("msg_id")
                        
                        # 解析 OpenAI 风格的响应格式
                        if obj_type == "content" and content_type == "text":
                            # 这是文本内容增量
                            text = data.get("text", "")
                            delta = data.get("delta", False)
                            
                            # 检查是否是推理消息（reasoning 类型的消息）
                            # 如果 msg_id 以特定前缀开头，可能是推理内容
                            # 我们只输出非推理内容
                            
                            event = {
                                "type": "content_delta",
                                "data": {
                                    "delta": text,
                                    "is_delta": delta,
                                    "msg_id": msg_id
                                }
                            }
                            assistant_message += text
                            yield event
                            
                        elif obj_type == "message":
                            # 消息开始或结束
                            status = data.get("status", "unknown")
                            msg_type = data.get("type", "unknown")
                            msg_id = data.get("id")
                            
                            if msg_type == "reasoning":
                                yield {"type": "thinking_start", "data": {"msg_id": msg_id}}
                                last_msg_id = ("reasoning", msg_id)
                            else:
                                yield {"type": "message", "data": {"status": status}}
                                last_msg_id = ("text", msg_id)
                                
                        elif obj_type == "response":
                            # 响应状态
                            status = data.get("status", "unknown")
                            if status == "completed":
                                # 保存完整响应到历史
                                session.history.append({
                                    "role": "assistant", 
                                    "content": assistant_message
                                })
                            yield {"type": "response", "data": {"status": status}}
                            
                        elif content_type == "tool_call" or obj_type == "tool_call":
                            # 工具调用
                            yield {"type": "tool_call", "data": data}
                            
                        elif content_type == "tool_result" or obj_type == "tool_result":
                            # 工具结果
                            yield {"type": "tool_result", "data": data}
                            
                        else:
                            # 其他类型，原样返回
                            yield {"type": "raw", "data": data}
                            
                    except json.JSONDecodeError:
                        yield {"type": "raw", "data": line[6:]}
                else:
                    # 非 SSE 格式的行
                    yield {"type": "raw", "data": line}
                    
        except RequestException as e:
            yield {"type": "error", "data": str(e)}
    
    def chat_sync(
        self, 
        message: str, 
        session_id: Optional[str] = None,
        user_id: Optional[str] = None
    ) -> str:
        """
        同步发送消息并获取完整响应
        
        Returns:
            完整的助手回复文本
        """
        full_response = ""
        for event in self.chat(message, session_id, user_id):
            if event.get("type") == "content_delta":
                full_response += event.get("data", {}).get("delta", "")
            elif event.get("type") == "error":
                raise Exception(event.get("data"))
        return full_response
    
    def stop(self, session_id: Optional[str] = None) -> bool:
        """
        停止正在运行的任务
        
        Returns:
            是否成功停止
        """
        session = self.get_session()
        sid = session_id or session.session_id
        
        try:
            response = requests.post(
                self._get_api_url("console/chat/stop"),
                json={"session_id": sid},
                timeout=10
            )
            return response.status_code == 200
        except RequestException:
            return False
    
    def get_chat_history(self, chat_id: Optional[str] = None) -> list:
        """
        获取聊天历史
        
        Returns:
            消息历史列表
        """
        session = self.get_session()
        cid = chat_id or session.chat_id
        
        if not cid:
            return []
            
        try:
            response = requests.get(
                self._get_api_url(f"chats/{cid}"),
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                return data.get("messages", [])
        except RequestException:
            pass
        return []
    
    def list_chats(self, user_id: Optional[str] = None) -> list:
        """
        列出所有聊天
        
        Returns:
            聊天列表
        """
        params = {}
        if user_id:
            params["user_id"] = user_id
            
        try:
            response = requests.get(
                self._get_api_url("chats"),
                params=params,
                timeout=10
            )
            if response.status_code == 200:
                return response.json()
        except RequestException:
            pass
        return []
    
    def upload_file(self, file_path: str, session_id: Optional[str] = None) -> dict:
        """
        上传文件供聊天使用
        
        Returns:
            上传结果
        """
        session = self.get_session()
        sid = session_id or session.session_id
        
        try:
            with open(file_path, "rb") as f:
                response = requests.post(
                    self._get_api_url("console/upload"),
                    files={"file": f},
                    data={"session_id": sid},
                    timeout=60
                )
                response.raise_for_status()
                return response.json()
        except (RequestException, IOError) as e:
            return {"error": str(e)}
    
    def health_check(self) -> bool:
        """检查 API 是否可用"""
        try:
            response = requests.get(
                f"{self.config.base_url}/",
                timeout=5
            )
            return response.status_code == 200
        except RequestException:
            return False


def print_streaming_response(events: Generator[dict, None, None]) -> str:
    """
    打印流式响应并返回完整文本
    
    用于命令行工具
    
    默认只显示最终响应，过滤掉推理过程。
    如果需要查看推理过程，设置环境变量 COPAW_SHOW_THINKING=1
    """
    import os
    
    show_thinking = os.environ.get("COPAW_SHOW_THINKING", "0") == "1"
    
    full_response = ""
    reasoning_msg_id = None
    final_msg_id = None
    in_reasoning = False
    
    for event in events:
        event_type = event.get("type", "unknown")
        
        if event_type == "thinking_start":
            in_reasoning = True
            reasoning_msg_id = event.get("data", {}).get("msg_id")
            if show_thinking:
                print("\n💭 思考中...", file=sys.stderr)
                
        elif event_type == "content_delta":
            msg_id = event.get("data", {}).get("msg_id")
            text = event.get("data", {}).get("delta", "")
            
            # 只显示非推理内容
            if msg_id != reasoning_msg_id:
                # 新的消息 ID，说明是最终响应
                if final_msg_id is None:
                    final_msg_id = msg_id
                if msg_id == final_msg_id:
                    if text:
                        print(text, end="", flush=True)
                        full_response += text
            
        elif event_type == "tool_call":
            tool_data = event.get("data", {})
            tool_name = tool_data.get("name", "unknown")
            print(f"\n🔧 调用工具: {tool_name}", file=sys.stderr)
            
        elif event_type == "tool_result":
            print("✅ 工具完成", file=sys.stderr)
            
        elif event_type == "error":
            print(f"\n❌ 错误: {event.get('data')}", file=sys.stderr)
            
        elif event_type == "response":
            status = event.get("data", {}).get("status", "unknown")
            if status == "completed":
                print()  # 完成时换行
    
    return full_response


def interactive_mode(client: CoPawClient):
    """交互模式 - 持续对话"""
    print("🤖 CoPaw Agent 交互模式")
    print("输入消息进行对话，输入 'quit' 或 'exit' 退出")
    print("输入 'clear' 开始新会话，输入 'history' 查看历史")
    print("-" * 50)
    
    while True:
        try:
            user_input = input("\n你: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n再见！")
            break
            
        if not user_input:
            continue
            
        # 命令处理
        if user_input.lower() in ("quit", "exit"):
            print("再见！")
            break
        elif user_input.lower() == "clear":
            client.create_session()
            print("✨ 已开始新会话")
            continue
        elif user_input.lower() == "history":
            session = client.get_session()
            for msg in session.history:
                role = "🤖" if msg["role"] == "assistant" else "你"
                print(f"{role}: {msg['content'][:100]}...")
            continue
            
        # 发送消息
        print("\n🤖: ", end="", flush=True)
        events = client.chat(user_input)
        print_streaming_response(events)


def main():
    parser = argparse.ArgumentParser(
        description="CoPaw Agent 客户端 - 与 AI Agent 交互",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 发送单条消息
  python copaw_client.py "你好，帮我写一首诗"
  
  # 交互模式
  python copaw_client.py --interactive
  
  # 指定会话 ID 继续对话
  python copaw_client.py --session-id "existing-session-id" "继续我们的对话"
  
  # 列出所有聊天
  python copaw_client.py --list-chats
"""
    )
    
    parser.add_argument(
        "message",
        nargs="?",
        help="要发送的消息（不提供则进入交互模式）"
    )
    parser.add_argument(
        "-i", "--interactive",
        action="store_true",
        help="进入交互模式"
    )
    parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:8088",
        help="CoPaw API 地址"
    )
    parser.add_argument(
        "--agent-id",
        default="default",
        help="Agent ID"
    )
    parser.add_argument(
        "--session-id",
        help="会话 ID（用于继续现有对话）"
    )
    parser.add_argument(
        "--user-id",
        help="用户 ID"
    )
    parser.add_argument(
        "--list-chats",
        action="store_true",
        help="列出所有聊天"
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=3600,
        help="请求超时时间（秒）"
    )
    
    args = parser.parse_args()
    
    # 创建客户端
    config = CoPawConfig(
        base_url=args.base_url,
        agent_id=args.agent_id,
        timeout=args.timeout
    )
    client = CoPawClient(config)
    
    # 健康检查
    if not client.health_check():
        print(f"❌ 无法连接到 CoPaw API: {args.base_url}")
        print("请确保 CoPaw 服务正在运行")
        sys.exit(1)
    
    # 列出聊天
    if args.list_chats:
        chats = client.list_chats(args.user_id)
        print(f"📋 共 {len(chats)} 个聊天:")
        for chat in chats:
            print(f"  - {chat.get('name', 'N/A')} (session: {chat.get('session_id', 'N/A')})")
        return
    
    # 设置会话
    if args.session_id:
        client.session = ChatSession(
            session_id=args.session_id,
            user_id=args.user_id or f"python_user_{uuid.uuid4().hex[:8]}"
        )
    else:
        client.create_session(args.user_id)
    
    # 交互模式或单条消息
    if args.interactive or not args.message:
        interactive_mode(client)
    else:
        # 单条消息模式

        args.message = """
        对 菏泽中医院 官网进行抓取，提取该医院所有可见医生的详细信息，经过结构化字段提取和数据核验后，将结果逐条实时推送到 RabbitMQ 队列。

---

## 一、执行步骤（按顺序执行）

### 1. 探索官网结构
- 先通过搜索引擎找到官网正确地址
- 分析网站导航，找到"专家介绍"、"医生团队"、"专家团队"、"科室医生"等入口
- 识别医生列表页和详情页的URL模式
- 确认是否存在分页机制

### 2. 抓取医生列表
- 遍历所有科室/专业分类
- 处理分页，确保不遗漏任何医生
- 收集所有医生详情页URL

### 3. 抓取医生详情
- 提取医生姓名（必须准确）
- 提取职称、介绍等原始信息
- 从非结构化文本中提取结构化字段

### 4. 数据核验与推送
- 验证数据合法性
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
  "standard_department": null,
  "display_department": "展示科室名",
  "title": "职称",
  "administrative_position": "行政职务",
  "academic_title": "学术头衔",
  "education": "学历",
  "degree": "学位",
  "alma_mater": "毕业院校",
  "years_of_experience": 从业年限数字,
  "intro": "医生简介原文",
  "specialty": "擅长领域原文",
  "confidence_score": 100.0,
  "otherpropertys": "{\"confidence_name\": \"官网\", \"source_kind\": \"official\", \"seed_url\": \"官网首页URL\", \"root_domain\": \"域名\", \"url\": \"医生详情页URL\", \"match_score\": 1.0, \"page_record_time\": null, \"fetch_method\": \"playwright\"}",
  "source_model": "VW5W"
}
```

---

## 四、结构化字段提取规则

### 从 intro/specialty 字段提取参考：

| 目标字段 | 提取规则 | 示例 |
|---------|---------|------|
| **职称 (title)** | 按优先级匹配：主任医师 > 副主任医师 > 主治医师 > 住院医师 | "主任医师" |
| **行政职务 (administrative_position)** | 匹配：XX科主任、XX科副主任、科室主任、副院长等 | "眼底病科主任" |
| **学术头衔 (academic_title)** | 匹配：教授、副教授、博士生导师、硕士生导师、享受国务院津贴等，多个用顿号连接 | "教授、博士生导师" |
| **学历 (education)** | 匹配：博士研究生 > 硕士研究生 > 本科 > 大专 | "博士研究生" |
| **学位 (degree)** | 匹配：博士 > 硕士 > 学士 | "博士" |
| **毕业院校 (alma_mater)** | 提取模式：毕业于XX大学/医学院、XX大学眼科学博士等 | "山东大学" |
| **从业年限 (years_of_experience)** | 提取模式：从事XX工作N年、从医N年、从事医学工作三十余年等 | 30 |

### 示例：
1.从事眼科临床工作28年，years_of_experience为28
2.从业临床工作30余年，years_of_experience为30
3.从事口腔正畸、修复专业37余年，years_of_experience为37
4.2009年获得中药学本科学历，education为本科
5.1992年毕业于泰山医学院、医学影像系。现任临沂市中心医院影像科副主任，副主任技师。从事医学影像工作30年，alma_mater是泰山医学院，administrative_position是副主任，years_of_experience是30，title是副主任技师
6.大学专科，毕业于菏泽医专医疗专业，于北京阜外医院进修学习。education是大专，alma_mater是菏泽医专

## 五、数据核验规则

### 必须通过的核验：

1. **姓名格式**：2-4个汉字，且不包含以下词汇：
   - 一直致力、教授、硕士、博士、主任医师、副主任医师、友情链接
   - 大学名称（如：山东大学、北京医学院）

2. **医院名称**：必须包含目标医院名称

3. **URL格式**：必须是有效的医生详情页URL

### 核验失败的处理：
- 记录失败原因和URL
- 不推送到队列
- 输出警告日志

---

## 六、错误处理

1. **网络错误**：重试3次，间隔递增（1s, 2s, 4s）
2. **解析错误**：记录异常，跳过该医生，继续下一个
3. **推送失败**：本地备份，记录失败列表
4. **分页遍历**：设置最大页数限制（如50页），防止无限循环

---



## 七、注意事项

1. **只从官网获取数据**，不要查找第三方网站
2. **抓取所有可见医生**，不遗漏任何科室
3. **字段缺失时用 null 填充**，不要编造数据
4. **结构化字段提取**，提取字段要充分


---
        
        """




        print(f"🤖: ", end="", flush=True)
        events = client.chat(args.message)
        print_streaming_response(events)


if __name__ == "__main__":
    main()