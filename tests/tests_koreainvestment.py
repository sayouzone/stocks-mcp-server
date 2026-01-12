#!/usr/bin/env python3
"""
Koreainvestment Crawler 사용 예시
"""

import os
import pandas as pd
import sys

from datetime import datetime, timedelta
from dotenv import load_dotenv
from pathlib import Path

# 상위 디렉토리를 path에 추가
sys.path.insert(0, str(Path(__file__).parent.parent))

from koreainvestment import KoreainvestmentCrawler

def demo_domestic(crawler: KoreainvestmentCrawler):
    """한국투자증권 국내 조회 데모"""
    print(f"\n{'='*60}")
    print(f"한국투자증권 국내 조회")
    print('='*60)

    # 국내주식 잔고 조회
    print(f"\n국내주식 잔고 조회")

    data = crawler.inquire_balance()
    print(data.summary.to_korean())
    for balance in data.balances:
        print(balance.to_korean())

    print(f"\n상품기본조회[v1_국내주식-029]")
    data = crawler.search_info("005930")
    print(data)

    print(f"\n주식기본조회[v1_국내주식-067]")
    data = crawler.search_stock_info("005930")
    print(data)

    print(f"\n국내주식 대차대조표[v1_국내주식-078]")
    data = crawler.balance_sheet("005930")
    print(data)

def demo_overseas(crawler: KoreainvestmentCrawler):
    """한국투자증권 해외 조회 데모"""
    print(f"\n{'='*60}")
    print(f"한국투자증권 해외 조회")
    print('='*60)

    # 해외주식 잔고 조회
    print(f"\n해외주식 잔고 조회")

    data = crawler.overseas_inquire_balance()
    print(data.summary.to_korean())
    for balance in data.balances:
        print(balance.to_korean())

def main():
    """메인 데모 실행"""
    
    load_dotenv()

    app_key = os.getenv('KIS_APP_KEY')
    app_secret = os.getenv('KIS_APP_SECRET')

    # 한국신용평가에서 요구하는 User-Agent 설정
    crawler = KoreainvestmentCrawler(app_key, app_secret)

    # 각 파일링 타입 데모
    demo_domestic(crawler)
    demo_overseas(crawler)
    
    print("\n" + "="*60)
    print("Demo completed!")
    print("="*60)


if __name__ == "__main__":
    main()
