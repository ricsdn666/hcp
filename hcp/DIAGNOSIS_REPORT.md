# 消息丢失问题诊断报告

## 问题描述
从 RabbitMQ 的 CacheHosp 队列消费消息时，部分消息丢失。

## 根本原因
**CacheHosp 和 DoctorResult 队列都有其他消费者在运行**，导致消息竞争。

证据：
- 队列状态显示：消费者数量 = 1
- 发送消息后，队列消息数量立即变为 0
- 独立测试队列工作正常（无消息丢失）

## 解决方案

### 方案1：停止其他消费者（推荐）
```bash
# 停止所有消费者
pkill -f doctor_scraper
sleep 5

# 启动你的消费者
python doctor_scraper_fixed.py
```

### 方案2：使用高优先级版本
```bash
# 直接运行改进版
python doctor_scraper_fixed.py

# 改进点：
# - prefetch_count = 10 (更高优先级)
# - 唯一消费者ID追踪
# - 启动时检测其他消费者
# - 详细的处理日志
```

### 方案3：协调消费者
如果多个消费者需要同时工作：
1. 确保所有消费者使用相同的 prefetch_count
2. RabbitMQ 会轮询分配消息给每个消费者
3. 每个消费者会收到约 1/N 的消息

## 测试验证

### 测试1：检查队列状态
```bash
python check_consumers.py
```

### 测试2：发送测试消息
```bash
python -c "
import pika, json
conn = pika.BlockingConnection(pika.URLParameters('amqp://guest:147258369aB@r.pisiewang.top:45672/'))
ch = conn.channel()
ch.basic_publish('', 'CacheHosp', json.dumps({
    'hosp_name': '测试医院',
    'stand_name': '测试医院',
    'cache_url': ''
}, ensure_ascii=False))
print('✅ 发送测试消息')
conn.close()
"
```

### 测试3：监控消费者
```bash
# 运行消费者并观察日志
python doctor_scraper_fixed.py

# 你会看到：
# 📊 队列状态:
#   - 待处理消息: 0
#   - 活跃消费者: 1  <-- 其他消费者
# ⚠️  警告: 检测到 1 个其他消费者!
```

## 消息不丢失的最佳实践

1. **独占队列**：使用专用队列名，避免竞争
2. **高 prefetch_count**：让消费者获取更多消息
3. **监控消费者数量**：定期检查是否有竞争
4. **确认机制**：确保 auto_ack=False，手动确认消息
5. **错误重试**：处理失败的消息重新入队

## 文件说明

- `doctor_scraper_fixed.py` - 改进版（推荐使用）
- `check_consumers.py` - 检查队列状态工具
- `test_consumer.py` - 测试消费者脚本

## 下一步

1. 确认是否有其他消费者应该运行
2. 如果不需要，停止其他消费者
3. 使用 `doctor_scraper_fixed.py` 启动
4. 监控处理日志，确认无消息丢失