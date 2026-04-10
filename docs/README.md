# Documentation Index

## Getting Started

| Document | Description |
|----------|-------------|
| [Quick Start Guide](guides/QUICK_START_GUIDE.md) | Setup infrastructure, start services, test APIs |
| [API cURL Examples](api/API_CURL.md) | Copy-paste cURL commands to test all endpoints |
| [Create Strategy API](api/CREATE_STRATEGY_API.md) | Detailed strategy creation reference with examples |

## Architecture & Design

| Document | Description |
|----------|-------------|
| [Complete System Analysis](guides/COMPLETE_SYSTEM_ANALYSIS.md) | Deep dive into all 6 components |
| [Architecture Overview](guides/trading-system-architecture.md) | System design and data flow |
| [Kafka Topics Guide](guides/KAFKA_TOPICS_GUIDE.md) | Message formats, topics, and schemas |
| [Proto Definitions](guides/proto-definitions.md) | gRPC service contracts |

## Broker Integration (Indira Securities)

| Document | Description |
|----------|-------------|
| [Migration Guide](guides/INDIRA_MIGRATION_GUIDE.md) | Indira API implementation details |
| [API Quick Reference](guides/INDIRA_API_QUICK_REFERENCE.md) | Endpoint summary |
| [Multi-User Guide](guides/INDIRA_MULTI_USER_GUIDE.md) | Multi-user session and credential management |

## Knowledge Transfer (KT)

Detailed per-service documentation for onboarding:

| Document | Service |
|----------|---------|
| [KT Index](KT/README.md) | Overview and navigation |
| [API Gateway](KT/API_GATEWAY_KT.md) | REST to gRPC, CORS, WebSocket |
| [User Config](KT/USER_CONFIG_SERVICE_KT.md) | Strategy CRUD, PostgreSQL, Kafka |
| [Data Ingestion](KT/DATA_INGESTION_SERVICE_KT.md) | MongoDB change streams, Kafka publishing |
| [Rules Engine](KT/RULES_ENGINE_SERVICE_KT.md) | Matching algorithm, scoring, Redis |
| [Trade Execution](KT/TRADE_EXECUTION_SERVICE_KT.md) | Order lifecycle, broker integration |
| [Risk Management](KT/RISK_MANAGEMENT_SERVICE_KT.md) | Pre-trade checks, risk profiles |

## Business & QA

| Document | Description |
|----------|-------------|
| [Project Requirements](Project_Requirements_Document.md) | Full business requirements (13 modules) |
| [QA Testing Document](QA_Feature_Testing_Document.md) | 500+ test cases across all screens |

## Real-Time Features

| Document | Description |
|----------|-------------|
| [Order Status WebSocket](orderStatus/Walkthrough.md) | WebSocket implementation for live order updates |

---

## Services Overview

| Service | Port | Status |
|---------|------|--------|
| API Gateway | 8081 (HTTP) | REST API for frontend |
| User Config | 50051 (gRPC) | Strategy management |
| Data Ingestion | 50052 (gRPC) | MongoDB -> Kafka pipeline |
| Rules Engine | 50053 (gRPC) | Strategy matching |
| Trade Execution | 50054 (gRPC) | Order execution |
| Risk Management | 50055 (gRPC) | Risk checks |
