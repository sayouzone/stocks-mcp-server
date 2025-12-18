## Yahoo Finance API

패키지 구조

```
yahoo/
├── __init__.py          # 공개 API 정의
├── client.py            # OpenDART HTTP 클라이언트
├── models.py            # 데이터 클래스 (DTO)
├── utils.py             # 유틸리티 함수 & 상수
├── crawler.py           # 통합 인터페이스 (Facade)
├── examples.py          # 사용 예시
└── parsers/
    ├── __init__.py
    ├── disclosure.py      # 공시정보 API 파서
#    └── reports.py         # 정기보고서 주요정보 API 파서
```
