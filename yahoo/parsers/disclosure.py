class YahooDisclosureParser:
    """
    Yahoo Finance API 파싱 클래스
    
    공시정보: Public Disclosure, https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS001
    """

    def __init__(self, client: YahooClient):
        self.client = client