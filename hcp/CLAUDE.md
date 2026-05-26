# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python project for hospital doctor information scraping, integrating with AI Agent services (CoPaw and Hermes) to automate web crawling and structured data extraction. The system consumes hospital/doctor tasks from RabbitMQ queues, uses AI agents to scrape official hospital websites, and pushes structured doctor data to result queues.

## Core Components

### Agent Clients
- **copaw_client.py**: CoPaw Agent client with SSE streaming, session management, and health checks
- **hermes_client.py**: Hermes Agent client with OpenAI-compatible API format

### Scrapers
- **doctor_scraper.py**: Consumes `CacheHosp` queue → scrapes all doctors from a hospital website → pushes to `DoctorResult` queue
- **info_extractor.py**: Reads URLs from PostgreSQL → extracts single doctor info using CoPaw → pushes to queue
- **single_doctor_search_hermes.py**: Consumes `SearchDoctorPro` queue → searches specific doctor by name/hospital/dept → pushes results

### Prompt Templates
- **doctor_scraper_prompt.md**: Detailed instructions for hospital-wide doctor scraping (embedded in doctor_scraper.py)
- **single_doctor_search_prompt.md**: Instructions for single doctor search tasks

## Key Commands

### Running Scrapers
```bash
# Doctor scraper (needs RabbitMQ + CoPaw service)
python doctor_scraper.py --copaw-url "http://localhost:8088"

# Info extractor with sharding (needs PostgreSQL + CoPaw)
python info_extractor.py --copaw-url "http://localhost:8088" --shard-index 0 --shard-total 5

# Single doctor search with Hermes (needs RabbitMQ + Hermes)
python single_doctor_search_hermes.py --hermes-url "http://127.0.0.1:8642" --hermes-key "123456"
```

### Agent Client CLI
```bash
# CoPaw client - send message
python copaw_client.py "your message"

# CoPaw client - interactive mode
python copaw_client.py --interactive

# CoPaw client - specify API URL
python copaw_client.py --base-url "http://localhost:8088" "message"

# Hermes client - similar usage
python hermes_client.py "your message"
python hermes_client.py --interactive --base-url "http://127.0.0.1:8642" --api-key "123456"
```

### Linting
```bash
ruff check .
ruff format .
ruff check . --fix
```

## Environment Setup

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install requests pika psycopg2-binary
```

## Environment Variables

```bash
export RABBITMQ_URL="amqp://guest:password@host:port/"  # RabbitMQ connection
export COPAW_BASE_URL="http://localhost:8088"           # CoPaw API address
export COPAW_SHOW_THINKING=1                            # Show agent reasoning
export HERMES_SHOW_THINKING=1                           # Show Hermes reasoning
```

## RabbitMQ Queues

| Queue | Consumer | Purpose |
|-------|----------|---------|
| `CacheHosp` | doctor_scraper.py | Hospital scraping tasks |
| `SearchDoctorPro` | single_doctor_search_hermes.py | Single doctor search tasks |
| `DoctorResult` | (output) | Structured doctor data |

## Data Flow

```
RabbitMQ (CacheHosp) → doctor_scraper.py → CoPaw Agent → Web Scraping → DoctorResult Queue
PostgreSQL (URLs) → info_extractor.py → CoPaw Agent → Single Doctor Extraction → DoctorResult Queue
RabbitMQ (SearchDoctorPro) → single_doctor_search_hermes.py → Hermes Agent → Doctor Search → DoctorResult Queue
```

## Output Schema (DoctorResult)

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
  "otherpropertys": "{\"url\": \"...\", \"source_kind\": \"official\"}",
  "source_model": "VW5W"
}
```

## Code Style

See AGENTS.md for detailed coding guidelines. Key points:
- Python 3.7+ with dataclasses and type annotations
- Class names: PascalCase (`CoPawClient`)
- Function/variable names: snake_case (`process_hospital`)
- Constants: UPPER_SNAKE (`RABBITMQ_URL`)
- Max line length: 88 characters (ruff default)
- Use emoji markers in console output: 🚀 🏥 🤖 ✅ ❌ ⚠️ 🔧 💭
- HTTP requests must set timeout
- Use `.get()` for dictionary access to avoid KeyError

## Important Constraints

1. **Only scrape official hospital websites** - never use third-party platforms (好大夫, 微医, etc.)
2. **Push to DoctorResult queue immediately** - don't batch results
3. **Validate URLs** - ensure they are doctor detail pages, not list pages
4. **Missing fields = null** - never fabricate data
5. **Admin positions** - only extract positions at the current hospital, not academic organizations or other institutions