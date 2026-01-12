# Copyright (c) 2025, Sayouzone
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import pandas as pd

from dataclasses import dataclass, field, asdict
from datetime import datetime
from decimal import Decimal
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Tuple

"""
Koreainvestment (KIS) Data Models
"""

@dataclass
class BalanceQueryParam:
    CANO: str               # 종합계좌번호
    ACNT_PRDT_CD: str       # 계좌상품코드
    AFHR_FLPR_YN: str       # 시간외단일가, 거래소여부
    INQR_DVSN: str          # 조회구분
    UNPR_DVSN: str          # 단가구분
    FUND_STTL_ICLD_YN: str  # 펀드결제분포함여부
    FNCG_AMT_AUTO_RDPT_YN: str    # 융자금액자동상환여부
    PRCS_DVSN: str          # 처리구분

    OFL_YN: Optional[str] = None            # 오프라인여부
    CTX_AREA_FK100: Optional[str] = None    # 연속조회검색조건100
    CTX_AREA_NK100: Optional[str] = None    # 연속조회키100

    def to_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items() if v is not None}


@dataclass
class DomesticStockBalance:
    """국내주식 개별 종목 잔고 정보 (output1)."""
    
    # API 응답에 포함될 수 있는 필드들
    # 현재 데이터에는 빈 배열이지만, 일반적인 국내주식 잔고 필드 구조
    pdno: str = ""                        # 상품번호 (종목코드)
    prdt_name: str = ""                   # 상품명
    hldg_qty: int = 0                     # 보유수량
    ord_psbl_qty: int = 0                 # 주문가능수량
    pchs_avg_pric: Decimal = Decimal("0") # 매입평균가격
    pchs_amt: Decimal = Decimal("0")      # 매입금액
    prpr: Decimal = Decimal("0")          # 현재가
    evlu_amt: Decimal = Decimal("0")      # 평가금액
    evlu_pfls_amt: Decimal = Decimal("0") # 평가손익금액
    evlu_pfls_rt: Decimal = Decimal("0")  # 평가손익률

    FIELD_NAMES_KO = {
        "pdno": "종목코드",
        "prdt_name": "상품명",
        "hldg_qty": "보유수량",
        "ord_psbl_qty": "주문가능수량",
        "pchs_avg_pric": "매입평균가격",
        "pchs_amt": "매입금액",
        "prpr": "현재가",
        "evlu_amt": "평가금액",
        "evlu_pfls_amt": "평가손익금액",
        "evlu_pfls_rt": "평가손익률",
    }

    @classmethod
    def from_dict(cls, data: dict) -> "DomesticStockBalance":
        """딕셔너리에서 DomesticStockBalance 객체 생성."""
        return cls(
            pdno=data.get("pdno", ""),
            prdt_name=data.get("prdt_name", ""),
            hldg_qty=int(data.get("hldg_qty", 0)),
            ord_psbl_qty=int(data.get("ord_psbl_qty", 0)),
            pchs_avg_pric=Decimal(data.get("pchs_avg_pric", "0")),
            pchs_amt=Decimal(data.get("pchs_amt", "0")),
            prpr=Decimal(data.get("prpr", "0")),
            evlu_amt=Decimal(data.get("evlu_amt", "0")),
            evlu_pfls_amt=Decimal(data.get("evlu_pfls_amt", "0")),
            evlu_pfls_rt=Decimal(data.get("evlu_pfls_rt", "0")),
        )

    def to_korean(self) -> dict:
        """필드명을 한글로 변환한 딕셔너리 반환."""
        def format_value(v):
            if isinstance(v, Decimal):
                # 불필요한 소수점 이하 0 제거 후 천단위 구분자 적용
                normalized = v.normalize()
                if normalized == normalized.to_integral_value():
                    return f"{int(normalized):,}"
                return f"{normalized:,f}"
            return v
        
        return {
            self.FIELD_NAMES_KO.get(k, k): format_value(v)
            for k, v in self.__dict__.items()
            if v is not None
        }


@dataclass
class DomesticAccountSummary:
    """국내주식 계좌 요약 정보 (output2)."""
    
    dnca_tot_amt: Decimal       # 예수금총금액
    nxdy_excc_amt: Decimal      # 익일정산금액
    prvs_rcdl_excc_amt: Decimal # 가수도정산금액
    cma_evlu_amt: Decimal       # CMA평가금액
    bfdy_buy_amt: Decimal       # 전일매수금액
    thdt_buy_amt: Decimal       # 금일매수금액
    nxdy_auto_rdpt_amt: Decimal # 익일자동상환금액
    bfdy_sll_amt: Decimal       # 전일매도금액
    thdt_sll_amt: Decimal       # 금일매도금액
    d2_auto_rdpt_amt: Decimal   # D+2자동상환금액
    bfdy_tlex_amt: Decimal      # 전일제비용금액
    thdt_tlex_amt: Decimal      # 금일제비용금액
    tot_loan_amt: Decimal       # 총대출금액
    scts_evlu_amt: Decimal      # 유가증권평가금액
    tot_evlu_amt: Decimal       # 총평가금액
    nass_amt: Decimal           # 순자산금액
    fncg_gld_auto_rdpt_yn: str  # 융자금자동상환여부
    pchs_amt_smtl_amt: Decimal  # 매입금액합계금액
    evlu_amt_smtl_amt: Decimal  # 평가금액합계금액
    evlu_pfls_smtl_amt: Decimal # 평가손익합계금액
    tot_stln_slng_chgs: Decimal # 총대주매각대금
    bfdy_tot_asst_evlu_amt: Decimal # 전일총자산평가금액
    asst_icdc_amt: Decimal      # 자산증감금액
    asst_icdc_erng_rt: Decimal  # 자산증감수익률

    FIELD_NAMES_KO = {
        "dnca_tot_amt": "예수금총금액",
        "nxdy_excc_amt": "익일정산금액",
        "prvs_rcdl_excc_amt": "가수도정산금액",
        "cma_evlu_amt": "CMA평가금액",
        "bfdy_buy_amt": "전일매수금액",
        "thdt_buy_amt": "금일매수금액",
        "nxdy_auto_rdpt_amt": "익일자동상환금액",
        "bfdy_sll_amt": "전일매도금액",
        "thdt_sll_amt": "금일매도금액",
        "d2_auto_rdpt_amt": "D+2자동상환금액",
        "bfdy_tlex_amt": "전일제비용금액",
        "thdt_tlex_amt": "금일제비용금액",
        "tot_loan_amt": "총대출금액",
        "scts_evlu_amt": "유가증권평가금액",
        "tot_evlu_amt": "총평가금액",
        "nass_amt": "순자산금액",
        "fncg_gld_auto_rdpt_yn": "융자금자동상환여부",
        "pchs_amt_smtl_amt": "매입금액합계금액",
        "evlu_amt_smtl_amt": "평가금액합계금액",
        "evlu_pfls_smtl_amt": "평가손익합계금액",
        "tot_stln_slng_chgs": "총대주매각대금",
        "bfdy_tot_asst_evlu_amt": "전일총자산평가금액",
        "asst_icdc_amt": "자산증감금액",
        "asst_icdc_erng_rt": "자산증감수익률",
    }

    @classmethod
    def from_dict(cls, data: dict) -> "DomesticAccountSummary":
        """딕셔너리에서 DomesticAccountSummary 객체 생성."""
        return cls(
            dnca_tot_amt=Decimal(data["dnca_tot_amt"]),
            nxdy_excc_amt=Decimal(data["nxdy_excc_amt"]),
            prvs_rcdl_excc_amt=Decimal(data["prvs_rcdl_excc_amt"]),
            cma_evlu_amt=Decimal(data["cma_evlu_amt"]),
            bfdy_buy_amt=Decimal(data["bfdy_buy_amt"]),
            thdt_buy_amt=Decimal(data["thdt_buy_amt"]),
            nxdy_auto_rdpt_amt=Decimal(data["nxdy_auto_rdpt_amt"]),
            bfdy_sll_amt=Decimal(data["bfdy_sll_amt"]),
            thdt_sll_amt=Decimal(data["thdt_sll_amt"]),
            d2_auto_rdpt_amt=Decimal(data["d2_auto_rdpt_amt"]),
            bfdy_tlex_amt=Decimal(data["bfdy_tlex_amt"]),
            thdt_tlex_amt=Decimal(data["thdt_tlex_amt"]),
            tot_loan_amt=Decimal(data["tot_loan_amt"]),
            scts_evlu_amt=Decimal(data["scts_evlu_amt"]),
            tot_evlu_amt=Decimal(data["tot_evlu_amt"]),
            nass_amt=Decimal(data["nass_amt"]),
            fncg_gld_auto_rdpt_yn=data.get("fncg_gld_auto_rdpt_yn", ""),
            pchs_amt_smtl_amt=Decimal(data["pchs_amt_smtl_amt"]),
            evlu_amt_smtl_amt=Decimal(data["evlu_amt_smtl_amt"]),
            evlu_pfls_smtl_amt=Decimal(data["evlu_pfls_smtl_amt"]),
            tot_stln_slng_chgs=Decimal(data["tot_stln_slng_chgs"]),
            bfdy_tot_asst_evlu_amt=Decimal(data["bfdy_tot_asst_evlu_amt"]),
            asst_icdc_amt=Decimal(data["asst_icdc_amt"]),
            asst_icdc_erng_rt=Decimal(data["asst_icdc_erng_rt"]),
        )

    def to_korean(self) -> dict:
        """필드명을 한글로 변환한 딕셔너리 반환."""
        def format_value(v):
            if isinstance(v, Decimal):
                # 불필요한 소수점 이하 0 제거 후 천단위 구분자 적용
                normalized = v.normalize()
                if normalized == normalized.to_integral_value():
                    return f"{int(normalized):,}"
                return f"{normalized:,f}"
            return v
        
        return {
            self.FIELD_NAMES_KO.get(k, k): format_value(v)
            for k, v in self.__dict__.items()
            if v is not None
        }


@dataclass
class DomesticBalanceResponse:
    """국내주식 잔고 조회 API 응답."""
    
    rt_cd: str           # 응답코드 (0: 성공)
    msg_cd: str          # 메시지코드
    msg1: str            # 응답메시지
    ctx_area_fk100: str  # 연속조회검색조건100
    ctx_area_nk100: str  # 연속조회키100
    balances: list[DomesticStockBalance] # 개별 종목 잔고 목록 (output1)
    summary: DomesticAccountSummary      # 계좌 요약 (output2)

    @property
    def is_success(self) -> bool:
        """API 호출 성공 여부."""
        return self.rt_cd == "0"

    @property
    def has_holdings(self) -> bool:
        """보유 종목 존재 여부."""
        return len(self.balances) > 0

    @classmethod
    def from_response(cls, data: dict) -> "DomesticBalanceResponse":
        """딕셔너리에서 DomesticBalanceResponse 객체 생성."""
        # output2는 리스트로 오지만 단일 요약 정보
        output2_data = data.get("output2", [])
        summary_data = output2_data[0] if output2_data else {}
        
        return cls(
            rt_cd=data["rt_cd"],
            msg_cd=data["msg_cd"],
            msg1=data["msg1"].strip(),
            ctx_area_fk100=data.get("ctx_area_fk100", "").strip(),
            ctx_area_nk100=data.get("ctx_area_nk100", "").strip(),
            balances=[DomesticStockBalance.from_dict(item) for item in data.get("output1", [])],
            summary=DomesticAccountSummary.from_dict(summary_data) if summary_data else None,
        )
