## DART 오픈API

패키지 구조

```
fnguide/
├── __init__.py          # 공개 API 정의
├── client.py            # OpenDART HTTP 클라이언트
├── models.py            # 데이터 클래스 (DTO)
├── utils.py             # 유틸리티 함수 & 상수
├── crawler.py           # 통합 인터페이스 (Facade)
├── examples.py          # 사용 예시
└── parsers/
    ├── __init__.py
    ├── document.py        # 문서 API 파서
    ├── document_viewer.py # 문서 뷰어 API 파서
    ├── disclosure.py      # 공시정보 API 파서
    ├── finance.py         # 정기보고서 재무정보 API 파서
    ├── material_facts.py  # 주요사항보고서 주요정보 API 파서
    ├── ownership.py       # 지분공시 종합정보 API 파서
    ├── registration.py    # 증권신고서 주요정보 API 파서
    └── reports.py         # 정기보고서 주요정보 API 파서
```

```
FnGuideCrawler
├── FnGuideMain (Snapshot 클래스)
│   ├── fetch (메인 URL으로 HTML 정보 가져오기)
│   └── parse (메인 HTML 정보 파싱)
├── FnGuideFinance (재무제표 클래스)
│   ├── fetch (재무제표 URL으로 HTML 정보 가져오기)
│   ├── parse (재무제표 HTML 정보 파싱)
│   ├── TableFinder
│   ├── HeaderExtractor
│   └── BodyExtractor
├── FnGuideCompany (기업개요 클래스)
├── FnGuideFinanceRatio (재무비율 클래스)
├── ...
├── check_gcp (GCP에서 Caching 정보 확인)
└── save_gcp (GCP에서 Caching 정보 저장)
```

## FnGuide Url 목록

- [메인](https://comp.fnguide.com/SVO2/ASP/SVD_main.asp?pGB=1&gicode=A{stock})
- [기업개요](https://comp.fnguide.com/SVO2/ASP/SVD_Corp.asp?pGB=1&gicode=A{stock})
- [재무제표](https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A{stock})
- [재무비율](https://comp.fnguide.com/SVO2/ASP/SVD_FinanceRatio.asp?pGB=1&gicode=A{stock})
- [투자지표](https://comp.fnguide.com/SVO2/ASP/SVD_Invest.asp?pGB=1&gicode=A{stock})
- [컨센서스](https://comp.fnguide.com/SVO2/ASP/SVD_Consensus.asp?pGB=1&gicode=A{stock})
- [지분분석](https://comp.fnguide.com/SVO2/ASP/SVD_shareanalysis.asp?pGB=1&gicode=A{stock})
- [업종분석](https://comp.fnguide.com/SVO2/ASP/SVD_ujanal.asp?pGB=1&gicode=A{stock})
- [경쟁사비교](https://comp.fnguide.com/SVO2/ASP/SVD_Comparison.asp?pGB=1&gicode=A{stock})
- [거래소공시](https://comp.fnguide.com/SVO2/ASP/SVD_Disclosure.asp?pGB=1&gicode=A{stock})
- [금감원공시](https://comp.fnguide.com/SVO2/ASP/SVD_Dart.asp?pGB=1&gicode=A{stock})


## 예제

- [삼성전자(A005930) | Snapshot | 기업정보 | Company Guide](https://comp.fnguide.com/SVO2/ASP/SVD_main.asp?pGB=1&gicode=A005930)
- [삼성전자(A005930) | 재무제표 | 기업정보 | Company Guide, 메뉴 포함](https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A005930&cID=&MenuYn=Y&ReportGB=&NewMenuID=103&stkGb=701)
- [삼성전자(A005930) | 재무제표 | 기업정보 | Company Guide, 메뉴 제외](https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A005930)