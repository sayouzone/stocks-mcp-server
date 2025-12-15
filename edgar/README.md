##

패키지 구조

```
edgar_crawler/
├── __init__.py          # 공개 API 정의
├── client.py            # SEC EDGAR HTTP 클라이언트
├── models.py            # 데이터 클래스 (DTO)
├── utils.py             # 유틸리티 함수 & 상수
├── crawler.py           # 통합 인터페이스 (Facade)
├── examples.py          # 사용 예시
└── parsers/
    ├── __init__.py
    ├── form_10k.py      # 10-K/10-Q 파서
    ├── form_8k.py       # 8-K 파서
    ├── form_13f.py      # 13F 파서
    └── def14a.py        # DEF 14A 파서
```


주요 개선 사항
| 개선 항목 | 설명 |
|---------|------|
| 모듈 분리 | 파일링 타입별로 파서를 별도 모듈로 분리 |
| Facade 패턴 | `EDGARCrawler`가 모든 파서를 통합 관리 |
| 데이터 모델 | `@dataclass`를 별도 모듈로 분리 |
| 유틸리티 | 공통 함수/상수를 `utils.py`로 추출 |
| HTTP 클라이언트 | API 호출 로직을 `EDGARClient`로 분리 |
| 레거시 호환 | 기존 메서드명 유지 (하위 호환성) |
