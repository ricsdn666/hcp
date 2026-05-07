#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hermes Agent Client - 与 Hermes Agent 交互的 Python 客户端

功能：
- 发送消息给 Hermes Agent
- 接收 Hermes 的流式响应
- 支持 OpenAI 兼容的 API 格式

使用示例：
    # 基本使用
    python hermes_client.py "你好，帮我写一首诗"

    # 作为库使用
    from hermes_client import HermesClient, HermesConfig
    config = HermesConfig(base_url="http://127.0.0.1:8642", api_key="123456")
    client = HermesClient(config)
    for event in client.chat("你好"):
        if event.get("type") == "content_delta":
            print(event.get("data", {}).get("delta", ""), end="")
"""

import argparse
import json
import sys
import uuid
from dataclasses import dataclass, field
from typing import Optional, Generator, Any, Dict, List

import requests
from requests.exceptions import RequestException


@dataclass
class HermesConfig:
    """Hermes 客户端配置"""

    base_url: str = "http://127.0.0.1:8642"
    api_key: str = "123456"
    model: str = "default"
    timeout: float = 3600.0


@dataclass
class ChatSession:
    """聊天会话"""

    session_id: str
    user_id: str
    history: List[Dict[str, str]] = field(default_factory=list)


class HermesClient:
    """Hermes Agent 客户端"""

    def __init__(self, config: Optional[HermesConfig] = None):
        self.config = config or HermesConfig()
        self.session: Optional[ChatSession] = None

    def _get_api_url(self, endpoint: str) -> str:
        """获取完整 API URL"""
        return f"{self.config.base_url}{endpoint}"

    def _get_headers(self) -> Dict[str, str]:
        """获取请求头"""
        return {
            "Authorization": f"Bearer {self.config.api_key}",
            "Content-Type": "application/json",
            "Accept": "text/event-stream",
        }

    def create_session(self, user_id: Optional[str] = None) -> ChatSession:
        """创建新的聊天会话"""
        session_id = str(uuid.uuid4())
        user_id = user_id or f"python_user_{uuid.uuid4().hex[:8]}"

        self.session = ChatSession(session_id=session_id, user_id=user_id)
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
    ) -> Generator[Dict[str, Any], None, None]:
        """
        发送消息并获取响应

        Args:
            message: 要发送的消息
            session_id: 会话 ID（可选，用于继续现有对话）
            user_id: 用户 ID（可选）
            stream: 是否流式返回

        Yields:
            响应事件字典，包含 type 和 data 字段
        """
        session = self.get_session()

        sid = session_id or session.session_id
        uid = user_id or session.user_id

        messages = session.history + [{"role": "user", "content": message}]

        payload = {
            "model": self.config.model,
            "messages": messages,
            "stream": stream,
            "session_id": sid,
            "user_id": uid,
        }

        session.history.append({"role": "user", "content": message})

        try:
            response = requests.post(
                self._get_api_url("/v1/chat/completions"),
                json=payload,
                headers=self._get_headers(),
                stream=True,
                timeout=self.config.timeout,
            )
            response.raise_for_status()

            assistant_message = ""

            for line in response.iter_lines(decode_unicode=True):
                if not line:
                    continue

                if line.startswith("data: "):
                    data_str = line[6:]

                    if data_str == "[DONE]":
                        session.history.append(
                            {"role": "assistant", "content": assistant_message}
                        )
                        yield {"type": "response", "data": {"status": "completed"}}
                        break

                    try:
                        data = json.loads(data_str)

                        choices = data.get("choices", [])
                        if choices:
                            choice = choices[0]
                            delta = choice.get("delta", {})
                            content = delta.get("content", "")

                            if content:
                                assistant_message += content
                                yield {
                                    "type": "content_delta",
                                    "data": {
                                        "delta": content,
                                        "msg_id": data.get("id"),
                                    },
                                }

                            finish_reason = choice.get("finish_reason")
                            if finish_reason:
                                yield {
                                    "type": "response",
                                    "data": {
                                        "status": "completed",
                                        "reason": finish_reason,
                                    },
                                }

                    except json.JSONDecodeError:
                        yield {"type": "raw", "data": data_str}
                else:
                    yield {"type": "raw", "data": line}

        except RequestException as e:
            yield {"type": "error", "data": str(e)}

    def chat_sync(
        self,
        message: str,
        session_id: Optional[str] = None,
        user_id: Optional[str] = None,
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

    def list_models(self) -> List[Dict[str, Any]]:
        """列出可用模型"""
        try:
            response = requests.get(
                self._get_api_url("/v1/models"), headers=self._get_headers(), timeout=10
            )
            if response.status_code == 200:
                return response.json().get("data", [])
        except RequestException:
            pass
        return []

    def health_check(self) -> bool:
        """检查 API 是否可用"""
        try:
            response = requests.get(
                self._get_api_url("/health"),
                headers={"Authorization": f"Bearer {self.config.api_key}"},
                timeout=5,
            )
            return response.status_code == 200
        except RequestException:
            return False


def print_streaming_response(events: Generator[Dict[str, Any], None, None]) -> str:
    """打印流式响应并返回完整文本"""
    import os

    show_thinking = os.environ.get("HERMES_SHOW_THINKING", "0") == "1"

    full_response = ""

    for event in events:
        event_type = event.get("type", "unknown")

        if event_type == "content_delta":
            text = event.get("data", {}).get("delta", "")
            if text:
                print(text, end="", flush=True)
                full_response += text

        elif event_type == "error":
            print(f"\n❌ 错误: {event.get('data')}", file=sys.stderr)

        elif event_type == "response":
            status = event.get("data", {}).get("status", "unknown")
            if status == "completed":
                print()

    return full_response


def interactive_mode(client: HermesClient):
    """交互模式 - 持续对话"""
    print("🤖 Hermes Agent 交互模式")
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

        print("\n🤖: ", end="", flush=True)
        events = client.chat(user_input)
        print_streaming_response(events)


def main():
    parser = argparse.ArgumentParser(
        description="Hermes Agent 客户端 - 与 Hermes Agent 交互",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 发送单条消息
  python hermes_client.py "你好，帮我写一首诗"
  
  # 交互模式
  python hermes_client.py --interactive
  
  # 指定 API 地址和 Key
  python hermes_client.py --base-url "http://127.0.0.1:8642" --api-key "123456" "消息"
  
  # 列出可用模型
  python hermes_client.py --list-models
""",
    )

    parser.add_argument(
        "message", nargs="?", help="要发送的消息（不提供则进入交互模式）"
    )
    parser.add_argument("-i", "--interactive", action="store_true", help="进入交互模式")
    parser.add_argument(
        "--base-url", default="http://127.0.0.1:8642", help="Hermes API 地址"
    )
    parser.add_argument("--api-key", default="123456", help="Hermes API Key")
    parser.add_argument("--model", default="default", help="模型名称")
    parser.add_argument("--session-id", help="会话 ID（用于继续现有对话）")
    parser.add_argument("--user-id", help="用户 ID")
    parser.add_argument("--list-models", action="store_true", help="列出可用模型")
    parser.add_argument(
        "--timeout", type=float, default=3600, help="请求超时时间（秒）"
    )

    args = parser.parse_args()

    config = HermesConfig(
        base_url=args.base_url,
        api_key=args.api_key,
        model=args.model,
        timeout=args.timeout,
    )
    client = HermesClient(config)

    if not client.health_check():
        print(f"❌ 无法连接到 Hermes API: {args.base_url}")
        print("请确保 Hermes 服务正在运行")
        sys.exit(1)

    if args.list_models:
        models = client.list_models()
        print(f"📋 共 {len(models)} 个模型:")
        for model in models:
            print(f"  - {model.get('id', 'N/A')}")
        return

    if args.session_id:
        client.session = ChatSession(
            session_id=args.session_id,
            user_id=args.user_id or f"python_user_{uuid.uuid4().hex[:8]}",
        )
    else:
        client.create_session(args.user_id)

    if args.interactive or not args.message:
        interactive_mode(client)
    else:
        print(f"🤖: ", end="", flush=True)
        events = client.chat(args.message)
        print_streaming_response(events)


if __name__ == "__main__":
    main()
