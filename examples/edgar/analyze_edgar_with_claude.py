#!/usr/bin/env python3
"""
SEC EDGAR 문서 분석기 (완전판)
- 공식 SEC API 사용 (무료, API 키 불필요)
- 다양한 분석 방법 제공
- 10-K, 10-Q, 8-K 등 모든 Filing 지원

공식 문서: https://www.sec.gov/search-filings/edgar-application-programming-interfaces
"""

import requests
import json
import pandas as pd
from typing import Dict, List, Optional
from datetime import datetime
import time
import re
from pathlib import Path

# =============================================================================
# SEC EDGAR API 클라이언트
# =============================================================================

class SECEdgarClient:
    """
    SEC EDGAR 공식 API 클라이언트
    
    API 키 불필요하지만 User-Agent는 필수
    SEC 정책: https://www.sec.gov/os/accessing-edgar-data
    """
    
    def __init__(self, company_name: str, email: str):
        """
        Args:
            company_name: 회사명
            email: 연락처 이메일
        """
        # SEC 요구사항: User-Agent 헤더 필수
        self.headers = {
            'User-Agent': f'{company_name} {email}',
            'Accept-Encoding': 'gzip, deflate'
        }
        
        self.base_url = "https://data.sec.gov"
        self.edgar_url = "https://www.sec.gov/cgi-bin/browse-edgar"
        
        # Rate limiting (초당 10개 요청 제한)
        self.rate_limit_delay = 0.1
    
    def _rate_limit(self):
        """Rate limiting 준수"""
        time.sleep(self.rate_limit_delay)
    
    def get_company_cik(self, ticker: str) -> Optional[str]:
        """
        티커 심볼로 CIK 번호 조회
        
        Args:
            ticker: 주식 티커 (예: 'AAPL')
        
        Returns:
            CIK 번호 (10자리)
        """
        # CIK 매핑 파일 다운로드
        # 회사이름, 티커, CIK
        url = "https://www.sec.gov/files/company_tickers.json"
        
        try:
            self._rate_limit()
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            data = response.json()
            
            # 티커로 CIK 찾기
            for key, company in data.items():
                if company['ticker'].upper() == ticker.upper():
                    # CIK를 10자리로 패딩
                    cik = str(company['cik_str']).zfill(10)
                    print(f"✓ Found: {company['title']} (CIK: {cik})")
                    return cik
            
            print(f"✗ Ticker '{ticker}' not found")
            return None
        
        except Exception as e:
            print(f"Error getting CIK: {e}")
            return None
    
    def get_submissions(self, cik: str) -> Dict:
        """
        회사의 모든 제출 문서 조회
        
        Args:
            cik: CIK 번호
        
        Returns:
            제출 문서 정보 딕셔너리
        """
        # CIK를 10자리로 패딩
        cik = str(cik).zfill(10)
        
        url = f"{self.base_url}/submissions/CIK{cik}.json"
        
        try:
            self._rate_limit()
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            return response.json()
        
        except Exception as e:
            print(f"Error getting submissions: {e}")
            return {}
    
    def get_company_facts(self, cik: str) -> Dict:
        """
        회사의 재무 팩트 조회 (XBRL 데이터)
        
        Args:
            cik: CIK 번호
        
        Returns:
            재무 팩트 딕셔너리
        """
        cik = str(cik).zfill(10)
        
        url = f"{self.base_url}/api/xbrl/companyfacts/CIK{cik}.json"
        
        try:
            self._rate_limit()
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            return response.json()
        
        except Exception as e:
            print(f"Error getting company facts: {e}")
            return {}
    
    def get_company_concept(
        self,
        cik: str,
        taxonomy: str = "us-gaap",
        concept: str = "AccountsPayableCurrent"
    ) -> Dict:
        """
        특정 개념의 데이터 조회
        
        Args:
            cik: CIK 번호
            taxonomy: XBRL taxonomy (us-gaap, ifrs-full, dei, srt)
            concept: 개념 이름
        
        Returns:
            개념 데이터 딕셔너리
        """
        cik = str(cik).zfill(10)
        
        url = f"{self.base_url}/api/xbrl/companyconcept/CIK{cik}/{taxonomy}/{concept}.json"
        
        try:
            self._rate_limit()
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            return response.json()
        
        except Exception as e:
            print(f"Error getting company concept: {e}")
            return {}
    
    def download_filing(
        self,
        accession_number: str,
        cik: str,
        primary_document: str
    ) -> Optional[str]:
        """
        Filing 문서 다운로드
        
        Args:
            accession_number: Accession number (하이픈 없이)
            cik: CIK 번호
            primary_document: 주 문서 파일명
        
        Returns:
            문서 HTML 내용
        """
        cik = str(cik).zfill(10)
        
        # Accession number 포맷팅 (하이픈 제거)
        acc_no = accession_number.replace("-", "")
        
        url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_no}/{primary_document}"
        
        try:
            self._rate_limit()
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            return response.text
        
        except Exception as e:
            print(f"Error downloading filing: {e}")
            return None


# =============================================================================
# SEC Filing 분석기
# =============================================================================

class SECFilingAnalyzer:
    """SEC Filing 문서 분석기"""
    
    def __init__(self, client: SECEdgarClient):
        self.client = client
    
    def get_recent_filings(
        self,
        cik: str,
        form_type: Optional[str] = None,
        limit: int = 10
    ) -> pd.DataFrame:
        """
        최근 Filing 목록 조회
        
        Args:
            cik: CIK 번호
            form_type: Filing 타입 (10-K, 10-Q, 8-K 등)
            limit: 반환할 최대 개수
        
        Returns:
            Filing 정보 DataFrame
        """
        submissions = self.client.get_submissions(cik)
        
        if not submissions:
            return pd.DataFrame()
        
        recent = submissions.get('filings', {}).get('recent', {})
        
        # DataFrame 생성
        df = pd.DataFrame({
            'accessionNumber': recent.get('accessionNumber', []),
            'filingDate': recent.get('filingDate', []),
            'reportDate': recent.get('reportDate', []),
            'form': recent.get('form', []),
            'primaryDocument': recent.get('primaryDocument', []),
            'primaryDocDescription': recent.get('primaryDocDescription', []),
        })
        
        # Form type 필터링
        if form_type:
            df = df[df['form'] == form_type]
        
        # 날짜순 정렬
        df = df.sort_values('filingDate', ascending=False)
        
        return df.head(limit)
    
    def extract_financial_metrics(self, cik: str) -> pd.DataFrame:
        """
        주요 재무 지표 추출
        
        Args:
            cik: CIK 번호
        
        Returns:
            재무 지표 DataFrame
        """
        facts = self.client.get_company_facts(cik)
        
        if not facts:
            return pd.DataFrame()
        
        metrics = []
        
        # US-GAAP 데이터 추출
        us_gaap = facts.get('facts', {}).get('us-gaap', {})
        
        # 주요 지표 목록
        important_metrics = [
            'Assets',
            'Liabilities',
            'StockholdersEquity',
            'Revenues',
            'NetIncomeLoss',
            'EarningsPerShareBasic',
            'CashAndCashEquivalentsAtCarryingValue'
        ]
        
        for metric in important_metrics:
            if metric in us_gaap:
                data = us_gaap[metric]
                label = data.get('label', metric)
                description = data.get('description', '')
                
                # USD 단위 데이터 추출
                units = data.get('units', {}).get('USD', [])
                
                if units:
                    # 가장 최근 값
                    latest = sorted(units, key=lambda x: x.get('end', ''), reverse=True)[0]
                    
                    metrics.append({
                        'Metric': label,
                        'Value': latest.get('val', 'N/A'),
                        'Date': latest.get('end', 'N/A'),
                        'Form': latest.get('form', 'N/A'),
                        'Description': description[:100]
                    })
        
        return pd.DataFrame(metrics)
    
    def analyze_revenue_trend(self, cik: str) -> pd.DataFrame:
        """
        매출 추이 분석
        
        Args:
            cik: CIK 번호
        
        Returns:
            매출 추이 DataFrame
        """
        revenue_data = self.client.get_company_concept(
            cik=cik,
            taxonomy="us-gaap",
            concept="Revenues"
        )
        
        if not revenue_data:
            return pd.DataFrame()
        
        # USD 단위 데이터 추출
        units = revenue_data.get('units', {}).get('USD', [])
        
        if not units:
            return pd.DataFrame()
        
        # DataFrame 생성
        df = pd.DataFrame(units)
        
        # 필요한 컬럼만 선택
        cols = ['end', 'val', 'fy', 'fp', 'form', 'filed']
        df = df[[col for col in cols if col in df.columns]]
        
        # 날짜순 정렬
        if 'end' in df.columns:
            df = df.sort_values('end', ascending=False)
        
        return df.head(20)
    
    def extract_text_from_filing(self, html_content: str) -> str:
        """
        HTML Filing에서 텍스트 추출
        
        Args:
            html_content: HTML 내용
        
        Returns:
            추출된 텍스트
        """
        # 간단한 HTML 태그 제거
        text = re.sub(r'<[^>]+>', ' ', html_content)
        
        # 여러 공백을 하나로
        text = re.sub(r'\s+', ' ', text)
        
        # XBRL 태그 제거
        text = re.sub(r'</?ix:[^>]+>', '', text)
        
        return text.strip()


# =============================================================================
# 사용 예제
# =============================================================================

def example_basic_usage(ticker: str):
    """
    기본 사용 예제
    
    Args:
        ticker: 티커
    """
    print("\n" + "=" * 70)
    print("예제 1: 기본 사용법")
    print("=" * 70)
    
    # 클라이언트 생성
    client = SECEdgarClient(
        company_name="Sayouzone",
        email="sjkim@sayouzone.com"
    )
    
    # 티커로 CIK 조회
    cik = client.get_company_cik(ticker)
    
    if cik:
        # 제출 문서 조회
        submissions = client.get_submissions(cik)
        
        print(f"\n회사명: {submissions.get('name')}")
        print(f"CIK: {submissions.get('cik')}")
        print(f"SIC: {submissions.get('sic')} - {submissions.get('sicDescription')}")
        print(f"티커: {', '.join(submissions.get('tickers', []))}")
        print(f"거래소: {', '.join(submissions.get('exchanges', []))}")
    
    print()


def example_recent_filings(ticker: str):
    """
    최근 Filing 조회 예제
    
    Args:
        ticker: 티커
    """
    print("\n" + "=" * 70)
    print("예제 2: 최근 Filing 조회")
    print("=" * 70)
    
    client = SECEdgarClient("Sayouzone", "sjkim@sayouzone.com")
    analyzer = SECFilingAnalyzer(client)
    
    cik = client.get_company_cik(ticker)
    
    if cik:
        # 최근 10-K 문서
        filings = analyzer.get_recent_filings(cik, form_type="10-K", limit=5)
        
        print(f"\n최근 10-K Filings ({len(filings)} 개):")
        print(filings[['filingDate', 'form', 'primaryDocDescription']].to_string(index=False))
    
    print()


def example_financial_metrics(ticker: str):
    """
    재무 지표 분석 예제
    
    Args:
        ticker: 티커
    """
    print("\n" + "=" * 70)
    print("예제 3: 재무 지표 분석")
    print("=" * 70)
    
    client = SECEdgarClient("Sayouzone", "sjkim@sayouzone.com")
    analyzer = SECFilingAnalyzer(client)
    
    cik = client.get_company_cik(ticker)
    
    if cik:
        # 재무 지표 추출
        metrics = analyzer.extract_financial_metrics(cik)
        
        if not metrics.empty:
            print(f"\n주요 재무 지표:")
            print(metrics.to_string(index=False))
    
    print()


def example_revenue_trend(ticker: str):
    """
    매출 추이 분석 예제
    
    Args:
         ticker: 티커
    """
    print("\n" + "=" * 70)
    print("예제 4: 매출 추이 분석")
    print("=" * 70)
    
    client = SECEdgarClient("Sayouzone", "sjkim@sayouzone.com")
    analyzer = SECFilingAnalyzer(client)
    
    cik = client.get_company_cik(ticker)
    
    if cik:
        # 매출 추이
        revenue = analyzer.analyze_revenue_trend(cik)
        
        if not revenue.empty:
            print(f"\n매출 추이 (최근 10개):")
            print(revenue.head(10).to_string(index=False))
    
    print()


def example_download_filing(ticker: str):
    """
    Filing 문서 다운로드 예제
    
    Args:
        ticker: 티커
    """
    print("\n" + "=" * 70)
    print("예제 5: Filing 문서 다운로드")
    print("=" * 70)
    
    client = SECEdgarClient("Sayouzone", "sjkim@sayouzone.com")
    analyzer = SECFilingAnalyzer(client)
    
    cik = client.get_company_cik(ticker)
    
    if cik:
        # 최근 10-K 가져오기
        filings = analyzer.get_recent_filings(cik, form_type="10-K", limit=1)
        
        if not filings.empty:
            filing = filings.iloc[0]
            
            print(f"\n다운로드 중: {filing['form']} ({filing['filingDate']})")
            
            # 문서 다운로드
            html_content = client.download_filing(
                accession_number=filing['accessionNumber'],
                cik=cik,
                primary_document=filing['primaryDocument']
            )
            
            if html_content:
                # 텍스트 추출
                text = analyzer.extract_text_from_filing(html_content)
                
                print(f"문서 크기: {len(html_content)} 문자")
                print(f"추출된 텍스트: {len(text)} 문자")
                print(f"\n내용 미리보기:\n{text[:500]}...")
    
    print()


def example_compare_companies(tickers: list[str]):
    """
    회사 간 비교 예제
    
    Args:
        tickers: 티커 리스트
    """
    print("\n" + "=" * 70)
    print("예제 6: 회사 간 비교")
    print("=" * 70)
    
    client = SECEdgarClient("Sayouzone", "sjkim@sayouzone.com")
    analyzer = SECFilingAnalyzer(client)
    
    companies = tickers
    results = []
    
    for ticker in companies:
        cik = client.get_company_cik(ticker)
        
        if cik:
            submissions = client.get_submissions(cik)
            metrics = analyzer.extract_financial_metrics(cik)
            
            # 자산 정보 찾기
            assets_row = metrics[metrics['Metric'].str.contains('Assets', na=False)]
            
            results.append({
                'Ticker': ticker,
                'Company': submissions.get('name'),
                'Assets': assets_row.iloc[0]['Value'] if not assets_row.empty else 'N/A',
                'Assets Date': assets_row.iloc[0]['Date'] if not assets_row.empty else 'N/A'
            })
    
    df = pd.DataFrame(results)
    print(f"\n회사 비교:")
    print(df.to_string(index=False))
    
    print()


def example_save_to_file(ticker: str):
    """
    파일 저장 예제
    
    Args:
        ticker: 티커
    """
    print("\n" + "=" * 70)
    print("예제 7: 데이터 파일 저장")
    print("=" * 70)
    
    client = SECEdgarClient("Sayouzone", "sjkim@sayouzone.com")
    analyzer = SECFilingAnalyzer(client)
    
    cik = client.get_company_cik(ticker)
    
    if cik:
        # 최근 Filing 조회
        filings = analyzer.get_recent_filings(cik, limit=20)
        
        # CSV로 저장
        output_file = "sec_filings_NVDA.csv"
        filings.to_csv(output_file, index=False)
        
        print(f"\n✓ Saved to {output_file}")
        print(f"  {len(filings)} filings saved")
        
        # 재무 지표 저장
        metrics = analyzer.extract_financial_metrics(cik)
        
        if not metrics.empty:
            metrics_file = "sec_metrics_NVDA.csv"
            metrics.to_csv(metrics_file, index=False)
            
            print(f"✓ Saved to {metrics_file}")
            print(f"  {len(metrics)} metrics saved")
    
    print()


# =============================================================================
# 실전 클래스
# =============================================================================

class SECAnalysisPipeline:
    """완전한 SEC 분석 파이프라인"""
    
    def __init__(self, company_name: str, email: str):
        self.client = SECEdgarClient(company_name, email)
        self.analyzer = SECFilingAnalyzer(self.client)
    
    def analyze_company(self, ticker: str) -> Dict:
        """회사 종합 분석"""
        print(f"\n분석 중: {ticker}")
        print("-" * 50)
        
        # CIK 조회
        cik = self.client.get_company_cik(ticker)
        
        if not cik:
            return {"error": "Ticker not found"}
        
        # 기본 정보
        submissions = self.client.get_submissions(cik)
        
        # 최근 Filing
        recent_filings = self.analyzer.get_recent_filings(cik, limit=10)
        
        # 재무 지표
        metrics = self.analyzer.extract_financial_metrics(cik)
        
        # 매출 추이
        revenue = self.analyzer.analyze_revenue_trend(cik)
        
        return {
            "ticker": ticker,
            "cik": cik,
            "company_info": submissions,
            "recent_filings": recent_filings,
            "financial_metrics": metrics,
            "revenue_trend": revenue
        }


def example_full_pipeline(ticker: str):
    """
    완전한 파이프라인 예제
    
    Args:
        ticker: 티커
    """
    print("\n" + "=" * 70)
    print("예제 8: 완전한 분석 파이프라인")
    print("=" * 70)
    
    pipeline = SECAnalysisPipeline("Sayouzone", "sjkim@sayouzone.com")
    
    # 회사 분석
    result = pipeline.analyze_company(ticker)
    
    if "error" not in result:
        info = result["company_info"]
        print(f"\n회사: {info.get('name')}")
        print(f"티커: {result['ticker']}")
        print(f"CIK: {result['cik']}")
        
        print(f"\n최근 Filing: {len(result['recent_filings'])} 개")
        print(result['recent_filings'][['filingDate', 'form']].head())
        
        print(f"\n재무 지표: {len(result['financial_metrics'])} 개")
        print(result['financial_metrics'].head())
    
    print()


# =============================================================================
# 메인 실행
# =============================================================================

def main():
    """모든 예제 실행"""
    
    print("\n")
    print("🚀 SEC EDGAR 문서 분석기")
    print("=" * 70)
    print()
    
    print("⚠️  주의사항:")
    print("- SEC는 User-Agent 헤더를 요구합니다")
    print("- 초당 10개 요청 제한이 있습니다")
    print("- 회사명과 이메일을 실제 정보로 변경하세요")
    print()
    
    try:
        example_basic_usage("AAPL")
        example_recent_filings("TSLA")
        example_financial_metrics("MSFT")
        example_revenue_trend("GOOGL")
        example_download_filing("AAPL")
        example_compare_companies(["AAPL", "MSFT", "GOOGL"])
        example_save_to_file("NVDA")
        example_full_pipeline("AAPL")
        
        print("=" * 70)
        print("✅ 모든 예제 실행 완료!")
        print("=" * 70)
        
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()


"""
Claude Sonnet 4.5에게 "sec.gov edgar 문서를 분석하는 python 소스를 제공해 줘"라고 요청


🚀 SEC EDGAR 문서 분석기
======================================================================

⚠️  주의사항:
- SEC는 User-Agent 헤더를 요구합니다
- 초당 10개 요청 제한이 있습니다
- 회사명과 이메일을 실제 정보로 변경하세요


======================================================================
예제 1: 기본 사용법
======================================================================
✓ Found: Apple Inc. (CIK: 0000320193)

회사명: Apple Inc.
CIK: 0000320193
SIC: 3571 - Electronic Computers
티커: AAPL
거래소: Nasdaq


======================================================================
예제 2: 최근 Filing 조회
======================================================================
✓ Found: Tesla, Inc. (CIK: 0001318605)

최근 10-K Filings (5 개):
filingDate form primaryDocDescription
2025-01-30 10-K                  10-K
2024-01-29 10-K                  10-K
2023-01-31 10-K                  10-K
2022-02-07 10-K                  10-K
2021-02-08 10-K                  10-K


======================================================================
예제 3: 재무 지표 분석
======================================================================
✓ Found: MICROSOFT CORP (CIK: 0000789019)

주요 재무 지표:
                                      Metric        Value       Date Form                                                                                          Description
                                      Assets 636351000000 2025-09-30 10-Q Sum of the carrying amounts as of the balance sheet date of all assets that are recognized. Assets a
                                 Liabilities 273275000000 2025-09-30 10-Q Sum of the carrying amounts as of the balance sheet date of all liabilities that are recognized. Lia
 Stockholders' Equity Attributable to Parent 363076000000 2025-09-30 10-Q Total of all stockholders' equity (deficit) items, net of receivables from officers, directors, owne
                                    Revenues  36148000000 2010-12-31 10-Q Amount of revenue recognized from goods sold, services rendered, insurance premiums, or other activi
    Net Income (Loss) Attributable to Parent  27747000000 2025-09-30 10-Q The portion of profit or loss for the period, net of income taxes, which is attributable to the pare
Cash and Cash Equivalents, at Carrying Value  28849000000 2025-09-30 10-Q Amount of currency on hand as well as demand deposits with banks or financial institutions. Includes


======================================================================
예제 4: 매출 추이 분석
======================================================================
✓ Found: Alphabet Inc. (CIK: 0001652044)

매출 추이 (최근 10개):
       end          val   fy fp form      filed
2025-09-30 102346000000 2025 Q3 10-Q 2025-10-30
2025-09-30 289007000000 2025 Q3 10-Q 2025-10-30
2025-06-30  96428000000 2025 Q2 10-Q 2025-07-24
2025-06-30 186662000000 2025 Q2 10-Q 2025-07-24
2024-09-30  88268000000 2025 Q3 10-Q 2025-10-30
2024-09-30 253549000000 2025 Q3 10-Q 2025-10-30
2024-06-30  84742000000 2025 Q2 10-Q 2025-07-24
2024-06-30 165281000000 2025 Q2 10-Q 2025-07-24
2021-12-31 257637000000 2021 FY 10-K 2022-02-02
2021-06-30  61880000000 2021 Q2 10-Q 2021-07-28


======================================================================
예제 5: Filing 문서 다운로드
======================================================================
✓ Found: Apple Inc. (CIK: 0000320193)

다운로드 중: 10-K (2025-10-31)
문서 크기: 1520208 문자
추출된 텍스트: 233148 문자

내용 미리보기:
aapl-20250927 false 2025 FY 0000320193 P1Y P1Y P1Y P1Y http://fasb.org/us-gaap/2025#LongTermDebtNoncurrent http://fasb.org/us-gaap/2025#LongTermDebtNoncurrent http://fasb.org/us-gaap/2025#OtherAssetsNoncurrent http://fasb.org/us-gaap/2025#OtherAssetsNoncurrent http://fasb.org/us-gaap/2025#PropertyPlantAndEquipmentNet http://fasb.org/us-gaap/2025#PropertyPlantAndEquipmentNet http://fasb.org/us-gaap/2025#OtherLiabilitiesCurrent http://fasb.org/us-gaap/2025#OtherLiabilitiesCurrent http://fasb.org/u...


======================================================================
예제 6: 회사 간 비교
======================================================================
✓ Found: Apple Inc. (CIK: 0000320193)
✓ Found: MICROSOFT CORP (CIK: 0000789019)
✓ Found: Alphabet Inc. (CIK: 0001652044)

회사 비교:
Ticker        Company       Assets Assets Date
  AAPL     Apple Inc. 359241000000  2025-09-27
  MSFT MICROSOFT CORP 636351000000  2025-09-30
 GOOGL  Alphabet Inc. 536469000000  2025-09-30


======================================================================
예제 7: 데이터 파일 저장
======================================================================
✓ Found: NVIDIA CORP (CIK: 0001045810)

✓ Saved to sec_filings_NVDA.csv
  20 filings saved
✓ Saved to sec_metrics_NVDA.csv
  6 metrics saved


======================================================================
예제 8: 완전한 분석 파이프라인
======================================================================

분석 중: AAPL
--------------------------------------------------
✓ Found: Apple Inc. (CIK: 0000320193)

회사: Apple Inc.
티커: AAPL
CIK: 0000320193

최근 Filing: 10 개
   filingDate    form
0  2025-11-14       4
1  2025-11-14  25-NSE
2  2025-11-12       4
3  2025-10-31    10-K
4  2025-10-30     8-K

재무 지표: 6 개
                                        Metric         Value        Date  Form                                        Description
0                                       Assets  359241000000  2025-09-27  10-K  Sum of the carrying amounts as of the balance ...
1                                  Liabilities  285508000000  2025-09-27  10-K  Sum of the carrying amounts as of the balance ...
2  Stockholders' Equity Attributable to Parent   73733000000  2025-09-27  10-K  Total of all stockholders' equity (deficit) it...
3                                     Revenues  265595000000  2018-09-29  10-K  Amount of revenue recognized from goods sold, ...
4     Net Income (Loss) Attributable to Parent  112010000000  2025-09-27  10-K  The portion of profit or loss for the period, ...

======================================================================
✅ 모든 예제 실행 완료!
======================================================================
"""

# =============================================================================
# 📚 참고 자료
# =============================================================================

"""
✅ 공식 SEC EDGAR API:
- 메인: https://www.sec.gov/search-filings/edgar-application-programming-interfaces
- API 키 불필요 (무료)
- User-Agent 헤더 필수
- Rate limit: 초당 10개 요청

📦 추가 라이브러리 (선택사항):
pip install sec-edgar-api  # 래퍼 라이브러리
pip install edgartools      # AI 통합 라이브러리

🎯 주요 API 엔드포인트:

1. Submissions (제출 문서):
   https://data.sec.gov/submissions/CIK##########.json

2. Company Facts (재무 데이터):
   https://data.sec.gov/api/xbrl/companyfacts/CIK##########.json

3. Company Concept (특정 개념):
   https://data.sec.gov/api/xbrl/companyconcept/CIK##########/us-gaap/Revenues.json

4. Frames (집계 데이터):
   https://data.sec.gov/api/xbrl/frames/us-gaap/Revenues/USD/CY2023Q4I.json

5. Filing 다운로드:
   https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{filename}

💡 주요 Form Types:
- 10-K: 연차 보고서
- 10-Q: 분기 보고서
- 8-K: 중요 사건 보고
- DEF 14A: Proxy Statement (주주총회)
- S-1: IPO 등록
- 4: 내부자 거래

🔍 CIK 조회:
- 티커 매핑: https://www.sec.gov/files/company_tickers.json
- 수동 검색: https://www.sec.gov/edgar/searchedgar/companysearch

⚠️ 중요 사항:
1. User-Agent 헤더 필수 (회사명 + 이메일)
2. Rate limiting 준수 (초당 10개)
3. 적절한 에러 처리
4. 대용량 데이터 주의
"""
