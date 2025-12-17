# NaverCrawler

네이버 뉴스에서 분야별 헤드라인 기사 수집
- 정치: https://news.naver.com/section/100
- 경제: https://news.naver.com/section/101
- 사회: https://news.naver.com/section/102
- 생활/문화: https://news.naver.com/section/103
- IT/과학: https://news.naver.com/section/105
- 세계: https://news.naver.com/section/104
- 뉴스검색 OpenAPI: https://openapi.naver.com/v1/search/news.json?query=xxxx&display=100


```
opendart/
├── __init__.py          # 공개 API 정의
├── client.py            # OpenDART HTTP 클라이언트
├── models.py            # 데이터 클래스 (DTO)
├── utils.py             # 유틸리티 함수 & 상수
├── crawler.py           # 통합 인터페이스 (Facade)
├── examples.py          # 사용 예시
└── parsers/
    ├── __init__.py
    ├── news.py          # 문서 API 파서
    └── market.py        # 정기보고서 주요정보 API 파서
```

```
NaverCrawler
├── NaverNews (뉴스 클래스)
│   ├── fetch (질문으로 Naver News API를 이용하여 뉴스 목록 조회)
│   └── parse (뉴스 목록으로 뉴스 상세 정보 조회)
├── NaverMarket (주식 클래스)
│   ├── fetch (기업 코드로 시작일과 종료일 사이 일별 시세를 조회)
│   ├── fetch_market_sum (현재 종목의 시가총액을 스크래핑하여 숫자로 반환)
│   ├── fetch_company_metadata (현재 종목의 시가총액을 스크래핑하여 숫자로 반환)
│   └── parse (...)
├── ...
├── check_gcp (GCP에서 Caching 정보 확인)
└── save_gcp (GCP에서 Caching 정보 저장)
```

