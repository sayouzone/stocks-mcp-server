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

def demo_market(crawler: YahooCrawler, code: str):
    """Yahoo 주요 시세 조회 데모"""
    print(f"\n{'='*60}")
    print(f"Yahoo 주요 시세 조회 - {code}")
    print('='*60)

    # 일별 시세 데모
    start_date='2025-01-01'
    end_date='2025-12-31'
    print(f"\nYahoo 일별 시세 조회 ({code}) {start_date} ~ {end_date}")

    data = crawler.market(code, start_date=start_date, end_date=end_date)
    print(data)

    # 주요 시세 데모
    print("\nYahoo 주요 시세 조회")

    df_main_prices = crawler.main_prices(code)
    print(df_main_prices)

    # 주요 시세 데모
    metadata = crawler.company_metadata(code)
    print(metadata)

def demo_news(crawler: YahooCrawler, code: str):
    """Yahoo 뉴스 조회 데모"""
    print(f"\n{'='*60}")
    print(f"Yahoo 뉴스 조회 - {code}")
    print('='*60)

    # Yahoo 뉴스 카테고리별 검색
    category_news = crawler.category_news()
    print(category_news)

    # Yahoo 뉴스 검색
    query="삼성전자"
    print(f"\nYahoo 뉴스 검색: {query}")

    data = crawler.news(query=query, max_articles=10)
    #print(data)

    for item in data:
        print(item)

def main(stock: str):
    """메인 데모 실행"""
    
    load_dotenv()

    # Yahoo에서 요구하는 User-Agent 설정
    crawler = YahooCrawler()

    # 각 파일링 타입 데모
    #demo_market(crawler, stock)
    demo_news(crawler, stock)
    
    print("\n" + "="*60)
    print("Demo completed!")
    print("="*60)


if __name__ == "__main__":
    # 삼성전자, 하이닉스, 네이버 예시
    code = "005930" # 삼성전자
    main(code)
