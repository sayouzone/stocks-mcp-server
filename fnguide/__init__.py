"""
FnGuide Crawler
===========================

FnGuide에서 주요 데이터를 추출하는 Python 패키지

Installation:
    pip install requests beautifulsoup4 lxml

Quick Start:
    >>> from edgar_crawler import EDGARCrawler
    >>> 
    >>> crawler = EDGARCrawler(user_agent="MyCompany admin@email.com")
    >>> cik = crawler.fetch_cik_by_ticker("AAPL")
    >>> 
    >>> # 10-K 재무 데이터 추출
    >>> filings = extcrawlerractor.fetch_10k_filings(cik, count=1)
    >>> data = crawler.extract_10k(cik, filings[0].accession_number)
    >>> print(f"Revenue: ${data['financial_data'].revenue:,.0f}")
    >>> 
    >>> # 13F 포트폴리오 추출
    >>> filings = crawler.fetch_13f_filings("0001067983", count=1)  # Berkshire
    >>> portfolio = crawler.extract_13f("0001067983", filings[0].accession_number)
    >>> print(f"Top Holding: {portfolio.top_holdings[0]['issuer']}")
    >>> 
    >>> # DEF 14A 임원 보상
    >>> filings = crawler.fetch_def14a_filings(cik, count=1)
    >>> proxy = crawler.extract_def14a(cik, filings[0].accession_number)
    >>> for exec in proxy.executive_compensation:
    ...     print(f"{exec.name}: ${exec.total:,.0f}")

Supported Filings:
    - 10-K: Annual Report (재무제표, 리스크 팩터, MD&A)
    - 10-Q: Quarterly Report (분기 재무제표)
    - 8-K: Current Report (중요 이벤트 공시)
    - 13F: Institutional Holdings (기관투자자 포트폴리오)
    - DEF 14A: Proxy Statement (임원 보상, 이사회, 거버넌스)

Note:
    SEC에서 User-Agent 헤더를 요구합니다. 
    회사명과 이메일을 포함한 식별 가능한 User-Agent를 사용하세요.
"""

__version__ = "0.1.0"
__author__ = "SeongJung Kim"

from .crawler import FnGuideCrawler
from .client import FnGuideClient
#from .models import (
#    # 공통
#    DartConfig,
#)
from .parsers import (
    FnGuideCompanyParser,
    FnGuideComparisonParser,
    FnGuideConsensusParser,
    FnGuideDartParser,
    FnGuideDisclosureParser,
    FnGuideFinanceRatioParser,
    FnGuideFinanceParser,
    FnGuideIndustryAnalysisParser,
    FnGuideInvestParser,
    FnGuideMainParser,
    FnGuideShareAnalysisParser,
)

__all__ = [
    # 메인 클래스
    "FnGuideCrawler",
    "FnGuideClient",
    
    # 데이터 모델
    #"DartConfig",
    
    # 파서
    "FnGuideCompanyParser",
    "FnGuideComparisonParser",
    "FnGuideConsensusParser",
    "FnGuideDartParser",
    "FnGuideDisclosureParser",
    "FnGuideFinanceRatioParser",
    "FnGuideFinanceParser",
    "FnGuideIndustryAnalysisParser",
    "FnGuideInvestParser",
    "FnGuideMainParser",
    "FnGuideShareAnalysisParser",
]
