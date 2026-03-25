# AGENTS.md - 开发指南

本项目是一个 Python 项目，包含 CoPaw Agent 客户端和医院医生信息抓取器。

## 项目结构

```
hcp/
├── copaw_client.py       # CoPaw Agent 客户端 - 与 AI Agent 交互
├── doctor_scraper.py     # 医院医生信息抓取器 - 从 RabbitMQ 消费医院数据
├── venv/                 # Python 虚拟环境
├── .ruff_cache/          # Ruff linting 缓存
└── .sisyphus/            # Sisyphus 配置目录
```

## 构建与运行命令

### 环境设置
```bash
# 创建虚拟环境
python -m venv venv

# 激活虚拟环境
source venv/bin/activate  # Linux/macOS
# 或
venv\Scripts\activate  # Windows

# 安装依赖
pip install requests pika
```

### 运行程序
```bash
# CoPaw 客户端 - 发送消息
python copaw_client.py "你的消息"

# CoPaw 客户端 - 交互模式
python copaw_client.py --interactive

# CoPaw 客户端 - 指定 API 地址
python copaw_client.py --base-url "http://localhost:8088" "消息"

# CoPaw 客户端 - 列出所有聊天
python copaw_client.py --list-chats

# 医生信息抓取器 - 消费 RabbitMQ 队列
python doctor_scraper.py
```

### 作为模块导入
```python
from copaw_client import CoPawClient, CoPawConfig

config = CoPawConfig(base_url="http://localhost:8088")
client = CoPawClient(config)

# 流式响应
for event in client.chat("你好"):
    if event.get("type") == "content_delta":
        print(event.get("data", {}).get("delta", ""), end="")

# 同步响应
response = client.chat_sync("你好")
```

### Lint 检查
```bash
# 使用 ruff 检查代码
ruff check .

# 使用 ruff 格式化代码
ruff format .

# 检查并自动修复
ruff check . --fix
```

### 测试
当前项目没有自动化测试文件。手动测试方法：
```bash
# 测试 CoPaw 客户端（需要 CoPaw 服务运行）
python copaw_client.py "你好"

# 测试医生抓取器（需要 RabbitMQ 服务运行）
python doctor_scraper.py
```

## 代码风格指南

### Python 版本
- 使用 Python 3.7+ 特性
- 使用 dataclasses 进行数据类定义
- 使用类型注解 (typing 模块)

### 导入规范
```python
# 1. 标准库导入（按字母顺序）
import argparse
import json
import os
import re
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Generator, List, Optional

# 2. 第三方库导入
import pika
import requests
from requests.exceptions import RequestException

# 3. 本地模块导入
from copaw_client import CoPawClient
```

### 命名约定
- **类名**: PascalCase (例：`CoPawClient`, `CoPawConfig`, `ChatSession`)
- **函数名**: snake_case (例：`get_rabbitmq_connection`, `process_hospital`)
- **变量名**: snake_case (例：`base_url`, `session_id`, `hospital_name`)
- **常量**: 全大写 + 下划线 (例：`RABBITMQ_URL`, `CACHEHOSP_QUEUE`)
- **私有方法**: 单下划线前缀 (例：`_get_api_url`)
- **模块级变量**: 大写 + 下划线，放在文件顶部

### 文档字符串
所有公共函数和类必须包含文档字符串：
```python
class CoPawClient:
    """CoPaw Agent 客户端"""
    
    def chat(
        self,
        message: str,
        session_id: Optional[str] = None,
        stream: bool = True
    ) -> Generator[dict, None, None]:
        """
        发送消息并获取响应
        
        Args:
            message: 要发送的消息
            session_id: 会话 ID（可选，用于继续现有对话）
            stream: 是否流式返回
            
        Yields:
            响应事件字典，包含 type 和 data 字段
        """
```

### 类型注解
- 必须为函数参数和返回值添加类型注解
- 使用 `typing` 模块处理复杂类型：
  - `Optional[str]` - 可为 None 的字符串
  - `Dict[str, Any]` - 字典类型
  - `List[Dict]` - 列表类型
  - `Generator[Type, None, None]` - 生成器类型

### 数据类定义
使用 dataclasses 定义数据类：
```python
@dataclass
class CoPawConfig:
    """CoPaw 客户端配置"""
    base_url: str = "http://127.0.0.1:8088"
    agent_id: str = "default"
    timeout: float = 3600.0


@dataclass
class ChatSession:
    """聊天会话"""
    session_id: str
    user_id: str
    chat_id: Optional[str] = None
    history: list = field(default_factory=list)
```

### 错误处理
```python
# HTTP 请求错误处理
try:
    response = requests.post(url, json=payload, timeout=10)
    response.raise_for_status()
except RequestException as e:
    print(f"❌ 请求失败：{e}")
    return {"error": str(e)}

# JSON 解析错误处理
try:
    data = json.loads(response_text)
except json.JSONDecodeError:
    continue

# RabbitMQ 错误处理
try:
    hospital_data = json.loads(body.decode("utf-8"))
except json.JSONDecodeError as e:
    print(f"❌ JSON 解析失败：{e}")
    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
```

### 代码格式化
- 遵循 PEP 8 规范
- 使用 4 个空格缩进
- 最大行长度：88 字符 (ruff 默认)
- 空行规范：
  - 文件顶部 shebang 和 encoding 声明
  - 导入块与代码之间：2 个空行
  - 类定义之间：2 个空行
  - 类内方法之间：1 个空行
  - 逻辑段落之间：1 个空行

### 日志与输出
- 使用 `print()` 进行控制台输出
- 使用 emoji 标记不同类型的输出：
  - 🚀 启动
  - 🏥 医院
  - 🤖 Agent/机器人
  - ✅ 成功
  - ❌ 错误
  - ⚠️ 警告
  - 📋 列表
  - 🔧 工具调用
  - 💭 思考中
- 使用分隔线 (`-` * 60 或 `=` * 60) 划分输出区域
- 重要输出后使用 `sys.stdout.flush()` 确保实时显示

### 最佳实践
1. **Session 管理**: 使用 `requests.Session()` 复用连接
2. **超时设置**: HTTP 请求必须设置 timeout
3. **流式处理**: 大响应使用 `stream=True` 和 `iter_lines()`
4. **URL 处理**: 使用 `.rstrip("/")` 确保 URL 格式一致
5. **类型安全**: 使用 `.get()` 方法访问字典键避免 KeyError
6. **环境变量**: 敏感配置使用环境变量 (如 `RABBITMQ_URL`)
7. **异常处理**: 捕获具体异常，不要使用裸 `except`
8. **资源清理**: 使用 `finally` 块确保资源正确释放

## 环境变量

```bash
# RabbitMQ 连接 URL
export RABBITMQ_URL="amqp://guest:password@host:port/"

# CoPaw API 地址
export COPAW_BASE_URL="http://localhost:8088"

# 显示思考过程
export COPAW_SHOW_THINKING=1
```

## 依赖项

主要依赖：
- `requests` - HTTP 请求库
- `pika` - RabbitMQ 客户端库

标准库依赖：
- `json` - JSON 处理
- `sys` - 系统参数
- `os` - 环境变量
- `re` - 正则表达式
- `time` - 时间处理
- `uuid` - UUID 生成
- `argparse` - 命令行参数解析
- `dataclasses` - 数据类
- `typing` - 类型注解