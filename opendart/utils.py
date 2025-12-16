"""
OpenDart 유틸리티 함수 및 상수
"""

# ============================================================
# 상수 정의
# ============================================================

API_URL = "https://opendart.fss.or.kr/api"
MAIN_URL = "https://dart.fss.or.kr/dsaf001/main.do"
PDF_URL = "https://dart.fss.or.kr/pdf/download/pdf.do?rcp_no={rcp_no}&dcm_no={dcm_no}"
PDF_MAIN_URL = "http://dart.fss.or.kr/pdf/download/main.do"
VIEWER_URL = "https://dart.fss.or.kr/report/viewer.do"


def decode_euc_kr(text):
    """깨진 한글 인코딩 복원"""
    # EUC-KR로 인코딩된 문자열이 Latin-1(ISO-8859-1)로 잘못 해석된 경우
    try:
        enc_text = text.encode('latin-1').decode('euc-kr')
        return enc_text
    except:
        pass
    
    # CP949로 시도
    try:
        enc_text = text.encode('latin-1').decode('cp949')
        return enc_text
    except:
        pass
    
    return text

