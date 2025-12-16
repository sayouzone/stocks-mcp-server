#!/usr/bin/env python3
"""
SEC EDGAR Crawler 사용 예시
"""

import pandas as pd
import sys
from datetime import datetime
from pathlib import Path

# 상위 디렉토리를 path에 추가
sys.path.insert(0, str(Path(__file__).parent.parent))

from opendart import OpenDartCrawler

def demo_10k(crawler: OpenDartCrawler, cik: str, ticker: str):
    """10-K 파일링 데모"""
    print(f"\n{'='*60}")
    print(f"10-K Annual Report - {ticker}")
    print('='*60)
    
    #filings = crawler.fetch_10k_filings(cik, count=1)
    filings = crawler.fetch_filings(cik, doc_type="10-K", count=1)
    if not filings:
        print("No 10-K filings found")
        return
    
    print(f"Latest filing: {filings[0].filing_date}")
    
    #print(filings)
    data = crawler.extract_10k(cik, filings[0].document_url, filings[0].accession_number)
    #for filing in filings:
    #    print(filing)
    #    data = crawler.extract_10k(cik, filing.document_url, filing.accession_number)
    
    #print(data)
    print(f"\nSections extracted: {list(data['sections'].keys())}")
    
    if data["financial_data"]:
        fd = data["financial_data"]
        print("\nFinancial Data:")
        if fd.revenue:
            print(f"  Revenue: ${fd.revenue:,.0f}")
        if fd.net_income:
            print(f"  Net Income: ${fd.net_income:,.0f}")
        if fd.total_assets:
            print(f"  Total Assets: ${fd.total_assets:,.0f}")
        if fd.eps_diluted:
            print(f"  Diluted EPS: ${fd.eps_diluted:.2f}")
    
    if data["risk_factors"]:
        print(f"\nRisk Factors: {len(data['risk_factors'])} identified")
        print(f"  First: {data['risk_factors'][0][:100]}...")


def demo_10q(crawler: OpenDartCrawler, cik: str, ticker: str):
    """10-Q 파일링 데모"""
    print(f"\n{'='*60}")
    print(f"10-Q Quarterly Report - {ticker}")
    print('='*60)
    
    #filings = crawler.fetch_10q_filings(cik, count=1)
    filings = crawler.fetch_filings(cik, doc_type="10-Q", count=1)
    if not filings:
        print("No 10-Q filings found")
        return
    
    print(f"Latest filing: {filings[0].filing_date}")
    
    #print(filings)
    data = crawler.extract_10q(cik, filings[0].document_url, filings[0].accession_number)
    #for filing in filings:
    #    print(filing)
    #    data = crawler.extract_10k(cik, filing.document_url, filing.accession_number)
    
    #print(data)
    print(f"\nSections extracted: {list(data['sections'].keys())}")
    
    if data["financial_data"]:
        fd = data["financial_data"]
        print("\nFinancial Data:")
        if fd.revenue:
            print(f"  Revenue: ${fd.revenue:,.0f}")
        if fd.net_income:
            print(f"  Net Income: ${fd.net_income:,.0f}")
        if fd.total_assets:
            print(f"  Total Assets: ${fd.total_assets:,.0f}")
        if fd.eps_diluted:
            print(f"  Diluted EPS: ${fd.eps_diluted:.2f}")
    
    #if data["risk_factors"]:
    #    print(f"\nRisk Factors: {len(data['risk_factors'])} identified")
    #    print(f"  First: {data['risk_factors'][0][:100]}...")


def demo_8k(crawler: OpenDartCrawler, cik: str, ticker: str):
    """8-K 파일링 데모"""
    print(f"\n{'='*60}")
    print(f"8-K Current Reports - {ticker}")
    print('='*60)
    
    #filings = crawler.fetch_8k_filings(cik, count=3)
    filings = crawler.fetch_filings(cik, doc_type="8-K", count=3)
    
    for filing in filings:
        print(f"\n{filing.filing_date}:")
        data = crawler.extract_8k(cik, filing.document_url, filing.accession_number)
        
        print(f"  Items: {data.items}")
        for event in data.event_descriptions[:2]:
            print(f"  - {event['item']}: {event['description']}")


def demo_13f(crawler: OpenDartCrawler, cik: str, ticker: str):
    """13F 파일링 데모"""
    print(f"\n{'='*60}")
    #print("13F Institutional Holdings - Berkshire Hathaway")
    print(f"13F Institutional Holdings - {ticker}")
    print('='*60)
    
    #cik = "0001067983"
    #filings = crawler.fetch_13f_filings(cik, count=2)
    filings = crawler.fetch_filings(cik, doc_type="13F", count=2)
    
    if not filings:
        print("No 13F filings found")
        return
    
    # 최신 포트폴리오
    data = crawler.extract_13f(cik, filings[0].document_url, filings[0].accession_number)
    
    print(f"\nReport Period: {data.report_period}")
    print(f"Total AUM: ${data.total_value / 1000:,.2f}M")
    print(f"Holdings Count: {data.holdings_count}")
    
    print("\nTop 5 Holdings:")
    for i, h in enumerate(data.top_holdings[:5], 1):
        print(f"  {i}. {h['issuer']}: ${h['value_millions']:,.2f}M ({h['percentage']:.1f}%)")
    
    # 분기 비교
    if len(filings) >= 2:
        print("\nQuarter-over-Quarter Changes:")
        comparison = crawler.compare_13f(
            cik,
            filings[0].document_url,
            filings[1].document_url,
            filings[0].accession_number,
            filings[1].accession_number
        )
        
        if comparison["new_positions"]:
            print(f"\n  New Positions: {len(comparison['new_positions'])}")
            for p in comparison["new_positions"][:3]:
                print(f"    + {p['issuer']}: ${p['value_thousands']/1000:,.2f}M")
        
        if comparison["closed_positions"]:
            print(f"\n  Closed Positions: {len(comparison['closed_positions'])}")
            for p in comparison["closed_positions"][:3]:
                print(f"    - {p['issuer']}")


def demo_def14a(crawler: OpenDartCrawler, cik: str, ticker: str):
    """DEF 14A 파일링 데모"""
    print(f"\n{'='*60}")
    print(f"DEF 14A Proxy Statement - {ticker}")
    print('='*60)
    
    #filings = crawler.fetch_def14a_filings(cik, count=1)
    filings = crawler.fetch_filings(cik, doc_type="DEF 14A", count=1)

    if not filings:
        print("No DEF 14A filings found")
        return
    
    data = crawler.extract_def14a(cik, filings[0].document_url, filings[0].accession_number)
    
    print(f"\nMeeting: {data.meeting_type} ({data.meeting_date})")
    
    # 임원 보상
    if data.executive_compensation:
        print("\nExecutive Compensation:")
        for exec_comp in data.executive_compensation[:5]:
            print(f"  {exec_comp.name}")
            print(f"    Salary: ${exec_comp.salary:,.0f}")
            print(f"    Stock Awards: ${exec_comp.stock_awards:,.0f}")
            print(f"    Total: ${exec_comp.total:,.0f}")
    
    # CEO Pay Ratio
    if data.ceo_pay_ratio:
        print(f"\nCEO Pay Ratio: {data.ceo_pay_ratio:.0f}:1")
    
    # 이사회
    print(f"\nBoard: {data.board_size} directors ({data.independent_directors} independent)")
    
    # 투표 안건
    if data.proposals:
        print("\nProposals:")
        for prop in data.proposals[:5]:
            rec = f" [{prop.board_recommendation}]" if prop.board_recommendation else ""
            print(f"  {prop.proposal_number}. {prop.title[:50]}{rec}")
    
    # 거버넌스
    if data.governance_highlights:
        print("\nGovernance Highlights:")
        for item in data.governance_highlights[:5]:
            print(f"  ✓ {item}")

def demo_corp_code(crawler: OpenDartCrawler, code: str):
    """DART의 기업코드을 조회 데모"""
    print(f"\n{'='*60}")
    print(f"회사명 또는 종목코드로 DART의 기업코드을 조회 - {code}")
    print('='*60)

    #corp_code = crawler.fetch_corp_code("삼성전자")
    #print(corp_code)
    corp_code = crawler.fetch_corp_code(code)

    print(f"기업코드: {corp_code}")

def demo_base_documents(crawler: OpenDartCrawler, code: str):
    """기업의 기본 공시 문서 조회 데모"""
    print(f"\n{'='*60}")
    print(f"회사명 또는 종목코드로 DART의 기업코드을 조회 - {code}")
    print('='*60)

    corp_code = crawler.fetch_corp_code(code)
    data = crawler.company(corp_code) # 삼성전자, 005930, 00126380
    #ata = crawler.company(code) # 조회 안됨
    print(data, type(data))

    df = crawler.fetch(code)
    print(df)
    #print(df.to_string())
    #with pd.option_context('display.max_rows', None, 'display.max_columns', None):
    #    print(df)

def demo_finance(crawler: OpenDartCrawler, corp_code: str):
    """정기보고서 재무정보 데모"""
    print(f"\n{'='*60}")
    print(f"정기보고서 재무정보 조회 - {code}")
    print('='*60)

    rcept_no = None

    # 지난해 (2024년) 정기보고서 재무정보 조회
    now = datetime.now()
    last_year = str(now.year - 1)

    corp_name = crawler.fetch_corp_name(corp_code)

    api_type = "단일회사 주요계정"
    data = crawler.finance(corp_code, last_year, api_type=api_type)
    #print(data)
    status = data.get("status", "")
    list = data.get("list", [])
    if status == "000" and len(list) > 0:
        print(f"\n{api_type} {last_year}년 ({corp_name}, {corp_code})")
        df = pd.DataFrame(list)
        rcept_no = df.get("rcept_no")
        print(df)

    api_type = "다중회사 주요계정"
    data = crawler.finance(corp_code, last_year, api_type=api_type)
    #print(data)
    status = data.get("status", "")
    list = data.get("list", [])
    if status == "000" and len(list) > 0:
        print(f"\n{api_type} {last_year}년 ({corp_name}, {corp_code})")
        df = pd.DataFrame(list)
        rcept_no = df.get("rcept_no")
        print(df)

    api_type = "단일회사 전체 재무제표"
    data = crawler.finance(corp_code, last_year, api_type=api_type)
    #print(data)
    status = data.get("status", "")
    list = data.get("list", [])
    if status == "000" and len(list) > 0:
        print(f"\n{api_type} {last_year}년 ({corp_name}, {corp_code})")
        df = pd.DataFrame(list)
        rcept_no = df.get("rcept_no")
        print(df)

    #api_type = "XBRL택사노미재무제표양식"
    api_type = "단일회사 주요 재무지표"
    data = crawler.finance(corp_code, last_year, api_type=api_type)
    #print(data)
    status = data.get("status", "")
    list = data.get("list", [])
    if status == "000" and len(list) > 0:
        print(f"\n{api_type} {last_year}년 ({corp_name}, {corp_code})")
        df = pd.DataFrame(list)
        print(df)

    api_type = "다중회사 주요 재무지표"
    data = crawler.finance(corp_code, last_year, api_type=api_type)
    #print(data)
    status = data.get("status", "")
    list = data.get("list", [])
    if status == "000" and len(list) > 0:
        print(f"\n{api_type} {last_year}년 ({corp_name}, {corp_code})")
        df = pd.DataFrame(list)
        print(df)

    # 올해 (2025년) 정기보고서 재무정보 조회
    current_year = str(now.year)
    quarter = (now.month - 1) // 3

    api_type = "단일회사 주요계정"
    data = crawler.finance(corp_code, current_year, quarter=quarter, api_type=api_type)
    #print(data)
    status = data.get("status", "")
    list = data.get("list", [])
    if status == "000" and len(list) > 0:
        print(f"\n{api_type} {current_year}년 {quarter}분기 ({corp_name}, {corp_code})")
        df = pd.DataFrame(list)
        rcept_no = df.get("rcept_no")
        print(df)

    api_type = "다중회사 주요계정"
    data = crawler.finance(corp_code, current_year, quarter=quarter, api_type=api_type)
    #print(data)
    status = data.get("status", "")
    list = data.get("list", [])
    if status == "000" and len(list) > 0:
        print(f"\n{api_type} {current_year}년 {quarter}분기 ({corp_name}, {corp_code})")
        df = pd.DataFrame(list)
        rcept_no = df.get("rcept_no")
        print(df)

    api_type = "단일회사 전체 재무제표"
    data = crawler.finance(corp_code, current_year, quarter=quarter, api_type=api_type)
    #print(data)
    status = data.get("status", "")
    list = data.get("list", [])
    if status == "000" and len(list) > 0:
        print(f"\n{api_type} {current_year}년 {quarter}분기 ({corp_name}, {corp_code})")
        df = pd.DataFrame(list)
        rcept_no = df.get("rcept_no")
        print(df)
    
    # 수익성지표 : M210000 안정성지표 : M220000 성장성지표 : M230000 활동성지표 : M240000
    indicator_code = "M210000"

    #api_type = "XBRL택사노미재무제표양식"
    api_type = "단일회사 주요 재무지표"
    data = crawler.finance(corp_code, current_year, quarter=quarter, api_type=api_type)
    #print(data)
    status = data.get("status", "")
    list = data.get("list", [])
    if status == "000" and len(list) > 0:
        idx_cl_nm = list[0].get("idx_cl_nm")
        print(f"\n{api_type} {current_year}년 {quarter}분기 {idx_cl_nm} ({corp_name}, {corp_code})")
        df = pd.DataFrame(list)
        print(df)

    api_type = "다중회사 주요 재무지표"
    data = crawler.finance(corp_code, current_year, quarter=quarter, api_type=api_type)
    #print(data)
    status = data.get("status", "")
    list = data.get("list", [])
    if status == "000" and len(list) > 0:
        idx_cl_nm = list[0].get("idx_cl_nm")
        print(f"\n{api_type} {current_year}년 {quarter}분기 {idx_cl_nm} ({corp_name}, {corp_code})")
        df = pd.DataFrame(list)
        print(df)

    indicator_code = "M220000"

    #api_type = "XBRL택사노미재무제표양식"
    api_type = "단일회사 주요 재무지표"
    data = crawler.finance(corp_code, current_year, quarter=quarter, api_type=api_type, indicator_code=indicator_code)
    #print(data)
    status = data.get("status", "")
    list = data.get("list", [])
    if status == "000" and len(list) > 0:
        idx_cl_nm = list[0].get("idx_cl_nm")
        print(f"\n{api_type} {current_year}년 {quarter}분기 {idx_cl_nm} ({corp_name}, {corp_code})")
        df = pd.DataFrame(list)
        print(df)

    api_type = "다중회사 주요 재무지표"
    data = crawler.finance(corp_code, current_year, quarter=quarter, api_type=api_type, indicator_code=indicator_code)
    #print(data)
    status = data.get("status", "")
    list = data.get("list", [])
    if status == "000" and len(list) > 0:
        idx_cl_nm = list[0].get("idx_cl_nm")
        print(f"\n{api_type} {current_year}년 {quarter}분기 {idx_cl_nm} ({corp_name}, {corp_code})")
        df = pd.DataFrame(list)
        print(df)

    indicator_code = "M230000"

    #api_type = "XBRL택사노미재무제표양식"
    api_type = "단일회사 주요 재무지표"
    data = crawler.finance(corp_code, current_year, quarter=quarter, api_type=api_type, indicator_code=indicator_code)
    #print(data)
    status = data.get("status", "")
    list = data.get("list", [])
    if status == "000" and len(list) > 0:
        idx_cl_nm = list[0].get("idx_cl_nm")
        print(f"\n{api_type} {current_year}년 {quarter}분기 {idx_cl_nm} ({corp_name}, {corp_code})")
        df = pd.DataFrame(list)
        print(df)

    api_type = "다중회사 주요 재무지표"
    data = crawler.finance(corp_code, current_year, quarter=quarter, api_type=api_type, indicator_code=indicator_code)
    #print(data)
    status = data.get("status", "")
    list = data.get("list", [])
    if status == "000" and len(list) > 0:
        idx_cl_nm = list[0].get("idx_cl_nm")
        print(f"\n{api_type} {current_year}년 {quarter}분기 {idx_cl_nm} ({corp_name}, {corp_code})")
        df = pd.DataFrame(list)
        print(df)

    indicator_code = "M240000"

    #api_type = "XBRL택사노미재무제표양식"
    api_type = "단일회사 주요 재무지표"
    data = crawler.finance(corp_code, current_year, quarter=quarter, api_type=api_type, indicator_code=indicator_code)
    #print(data)
    status = data.get("status", "")
    list = data.get("list", [])
    if status == "000" and len(list) > 0:
        idx_cl_nm = list[0].get("idx_cl_nm")
        print(f"\n{api_type} {current_year}년 {quarter}분기 {idx_cl_nm} ({corp_name}, {corp_code})")
        df = pd.DataFrame(list)
        print(df)

    api_type = "다중회사 주요 재무지표"
    data = crawler.finance(corp_code, current_year, quarter=quarter, api_type=api_type, indicator_code=indicator_code)
    #print(data)
    status = data.get("status", "")
    list = data.get("list", [])
    if status == "000" and len(list) > 0:
        idx_cl_nm = list[0].get("idx_cl_nm")
        print(f"\n{api_type} {current_year}년 {quarter}분기 {idx_cl_nm} ({corp_name}, {corp_code})")
        df = pd.DataFrame(list)
        print(df)

    # 올해 마지막 재무제표 접수번호
    print(f"\n{current_year}년 {quarter}분기 접수번호: {rcept_no.iloc[0]}")
    
    return rcept_no.iloc[0]

def demo_download_xbrl(crawler: OpenDartCrawler, rcept_no: str = None):
    # OpenDart 정기보고서 재무정보 - 재무제표 원본파일(XBRL). 다운로드
    print(f"\n{'='*60}")
    print(f"회사명 또는 종목코드로 DART의 기업코드을 조회 - {code}")
    print('='*60)
    #rcept_no = "20190401004781"
    rcept_no = "20250814003156" if not rcept_no else rcept_no
    save_path = crawler.finance_file(rcept_no, quarter = 4)
    
    if not save_path:
        print(f"파일이 존재하지 않습니다. {rcept_no}")
    else:
        print(f"저장 경로: {save_path}")

def main(code: str):
    """메인 데모 실행"""
    DART_API_KEY="fd664865257f1a3073b654f9185de11a708f726c"

    # SEC에서 요구하는 User-Agent 설정
    crawler = OpenDartCrawler(api_key=DART_API_KEY)

    # 회사이름으로 corp_code 검색
    #company_name = "삼성전자"
    #corp_code = crawler.fetch_corp_code(company_name)
    corp_code = crawler.fetch_corp_code(code)
    if not corp_code:
        print(f"Could not find corp_code for {code}")
        return

    print(f"\n{code} corp_code: {corp_code}")

    # 각 파일링 타입 데모
    #demo_corp_code(crawler, code)
    #demo_base_documents(crawler, code)
    rcept_no = demo_finance(crawler, corp_code)
    # 00126380      삼성전자     005930        Y                  반기보고서 (2025.06)  20250814003156              삼성전자  20250814  
    #rcept_no="20251114002447"
    demo_download_xbrl(crawler, rcept_no=rcept_no)
    
    
    print("\n" + "="*60)
    print("Demo completed!")
    print("="*60)


if __name__ == "__main__":
    # 삼성전자, 하이닉스, 네이버 예시
    code = "005930" # 삼성전자
    main(code)
