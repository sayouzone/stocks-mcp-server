#!/usr/bin/env python3
"""
Yahoo Crawler 사용 예시
"""

import os
import pandas as pd
import sys

from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

# 상위 디렉토리를 path에 추가
sys.path.insert(0, str(Path(__file__).parent.parent))

from yahoo import YahooCrawler

def demo_info(crawler: YahooCrawler, ticker: str):
    """Yahoo 주요 시세 조회 데모"""
    print(f"\n{'='*60}")
    print(f"Yahoo 주요 시세 조회 - {ticker}")
    print('='*60)

    # 정보 데모
    print(f"\nYahoo 정보 조회 ({ticker})")
    data = crawler.info(ticker)
    print(data)

def demo_market(crawler: YahooCrawler, ticker: str):
    """Yahoo 주요 시세 조회 데모"""
    print(f"\n{'='*60}")
    print(f"Yahoo 주요 시세 조회 - {ticker}")
    print('='*60)

    # 일별 시세 데모
    start_date='2025-12-01'
    end_date='2025-12-31'
    print(f"\nYahoo 일별 시세 조회 ({ticker}) {start_date} ~ {end_date}")

    data = crawler.market(ticker, start_date=start_date, end_date=end_date)
    print(data)

def demo_news(crawler: YahooCrawler, ticker: str):
    """Yahoo 뉴스 조회 데모"""
    print(f"\n{'='*60}")
    print(f"Yahoo 뉴스 조회 - {ticker}")
    print('='*60)

    # Yahoo 뉴스 검색
    query=ticker
    print(f"\nYahoo 뉴스 검색: {query}")

    data = crawler.news(query=query, max_articles=10)
    print(data)

    #for item in data:
    #    print(item)

def main(ticker: str):
    """메인 데모 실행"""
    
    load_dotenv()

    # Yahoo에서 요구하는 User-Agent 설정
    crawler = YahooCrawler()

    # 각 파일링 타입 데모
    #demo_info(crawler, ticker)
    demo_market(crawler, ticker)
    #demo_news(crawler, ticker)
    
    print("\n" + "="*60)
    print("Demo completed!")
    print("="*60)


if __name__ == "__main__":
    # 삼성전자, 하이닉스, 네이버 예시
    ticker = "TSLA" # 삼성전자
    main(ticker)
