import io
import zipfile
import re
import pandas as pd
from datetime import datetime
from urllib.parse import unquote

from ..client import OpenDartClient

from ..utils import (
    decode_euc_kr,
    API_URL,
    MAIN_URL,
    PDF_URL,
    PDF_MAIN_URL,
    VIEWER_URL
)

class DartAPIParser:
    """
    OpenDART API 파싱 클래스
    
    공시정보: Public Disclosure, https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS001
    정기보고서 주요정보: Key Information in Periodic Reports, https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS002
    정기보고서 재무정보: Financial Information in Periodic Reports, https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS003
    지분공시 종합정보: Comprehensive Share Ownership Information, https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS004
    주요사항보고서 주요정보: Key Information in Reports on Material Facts, https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS005
    증권신고서 주요정보: Key Information in Registration Statements, https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS006
    """

    list_url = f"{API_URL}/list.json"
    company_url = f"{API_URL}/company.json"
    document_url = f"{API_URL}/document.xml"
    corp_code_url = f"{API_URL}/corpCode.xml"   

    finance_urls = {
        "단일회사 주요계정": f"{API_URL}/fnlttSinglAcnt.json",
        "다중회사 주요계정": f"{API_URL}/fnlttMultiAcnt.json",
        "재무제표 원본파일(XBRL)": f"{API_URL}/fnlttXbrl.xml",
        "단일회사 전체 재무제표": f"{API_URL}/fnlttSinglAcntAll.json",
        "XBRL택사노미재무제표양식": f"{API_URL}/xbrlTaxonomy.json",
        "단일회사 주요 재무지표": f"{API_URL}/fnlttSinglIndx.json",
        "다중회사 주요 재무지표": f"{API_URL}/fnlttCmpnyIndx.json"
    }

    def __init__(self, client: OpenDartClient):
        self.client = client

    def list(self, code, start: str | None = None, end: str | None = None):
        """
        OpenDart 공시정보 - 공시검색 

        Args:
            code (str): 기업 고유번호 (주식 코드(stock_code) 및 DART 고유번호 모두 가능(corp_code))
            start (str): 시작일 (YYYY-MM-DD, YYYY/MM/DD, YYYY.MM.DD, YYYYMMDD)
            end (end): 종료일 (YYYY-MM-DD, YYYY/MM/DD, YYYY.MM.DD, YYYYMMDD)
        Returns:
            pd.DataFrame: 공시 목록
        """
        start = datetime.now().strftime("%Y%m%d") if not start else self._dateformat(start)
        end = datetime.now().strftime("%Y%m%d") if not end else self._dateformat(end)
        
        params = {
            "crtfc_key": self.client.api_key,
            "corp_code": code,
            "bgn_de": start,
            "end_de": end,
            "corp_cls": "Y",
            "page_no": 1,
            "page_count": 100
        }

        all_data = []  # 전체 데이터 저장
        page = 1
        
        while True:
            params['page_no'] = page
            
            response = self.client._get(self.list_url, params=params)
            json_data = response.json()
            
            status = json_data.get("status")

            # 에러 체크
            if status != "000":
                print(f"Error: {json_data.get('message')}")
                break

            # 데이터 추가
            data_list = json_data.get("list", [])
            all_data.extend(data_list)
            
            # 페이지 정보
            page_no = json_data.get("page_no", 1)
            total_page = json_data.get("total_page", 1)
            total_count = json_data.get("total_count", 0)

            print(f"페이지 {page}/{total_page} 완료 (총 {total_count}건)")

            # 마지막 페이지면 종료
            if page >= total_page:
                break

            page += 1

        # DataFrame 변환
        df = pd.DataFrame(all_data)
        return df

    def company(self, code):
        """
        OpenDart 공시정보 - 기업개황 (기업 정보 조회)
        corp_code, stock_code으로 조회가 가능하지만 기업명으로는 조회되지 않는다.

        Args:
            code (str): 기업 고유번호  (주식 코드(stock_code) 및 DART 고유번호 모두 가능(corp_code))
        Returns:
            Dict: 기업개황
        """
        
        params = {
            "crtfc_key": self.client.api_key,
            "corp_code": code
        }

        print(self.company_url, params)
        response = self.client._get(self.company_url, params=params)
        
        json_data = response.json()
            
        status = json_data.get("status")

        # 에러 체크
        if status != "000":
            print(f"Error: {json_data.get('message')}")
            return {}

        self.corp_code = json_data.get("corp_code")
        self.corp_name = json_data.get("corp_name")
        self.stock_code = json_data.get("stock_code")

        return json_data

    def document(self, rcept_no, save_path: str | None = None):
        """
        OpenDart 공시정보 - 공시서류원본파일
        https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019003

        Args:
            rcept_no (str): 접수번호
            save_path (str): 파일 저장 경로
        Returns:
            pd.DataFrame: 기업개황
        """
        
        params = {
            "crtfc_key": self.client.api_key,
            "rcept_no": rcept_no
        }

        response = self.client._get(self.document_url, params=params)

        response_headers = response.headers

        if response.status_code != 200:
            print(f"다운로드 실패: {response.status_code}")
            return None

        content_type = response_headers.get("Content-Type")
        if "application/xml" in content_type:
            text_data = response.text
            print(text_data)
            return None

        # 바이너리 데이터 
        binary_data = response.content

        if save_path is None:
            save_path = f"dart_{rcept_no}"
            save_path = self.__save_zip_path(response_headers, save_path)
        
        # ZIP 파일 저장
        #self.__save_zip(binary_data, save_path)
        # ZIP 파일 압축해제 및 폴더에 저장
        #self.__save_unzip(binary_data, save_path)
        #return save_path

        result = {
            'rcept_no': rcept_no,
        }

        _result = self.__parse_unzip_xml(binary_data, save_path)
        result = result | _result
        
        """
        # ZIP 압축 해제 (메모리에서)
        with zipfile.ZipFile(io.BytesIO(binary_data)) as zf:
            file_list = zf.namelist()
            print(f"압축 파일 내 {len(file_list)}개 파일:")
            
            for fname in file_list:
                # 파일명 인코딩 수정
                enc_name = decode_euc_kr(fname)
                
                print(f"  - {enc_name}")
                result['files'].append(enc_name)
                
                # XML 파일만 파싱
                if fname.endswith('.xml'):
                    content = zf.read(fname)
                    parsed = self._parse_xml(content, enc_name)
                    result['xml_data'].append(parsed)
        """
        
        print(f"\n총 {len(result['xml_data'])}개 XML 파일 파싱 완료")
        return result

    def corp_code(self, save_path: str | None = None):
        """
        OpenDart 공시정보 - 고유번호
        https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019018

        Args:
            save_path (str): 파일 저장 경로
        Returns:
            Dict: 기업 고유번호 목록
        """
        
        params = {
            "crtfc_key": self.client.api_key
        }

        print(self.corp_code_url, params)
        response = self.client._get(self.corp_code_url, params=params)

        response_headers = response.headers

        if response.status_code != 200:
            print(f"다운로드 실패: {response.status_code}")
            return None

        content_type = response_headers.get("Content-Type")
        if "application/xml" in content_type:
            text_data = response.text
            print(text_data)
            return None

        # 바이너리 데이터 
        binary_data = response.content

        if save_path is None:
            save_path = f"dart_corp_code"
            save_path = self.__save_zip_path(response_headers, save_path)

        # ZIP 파일 저장
        #self.__save_zip(binary_data, save_path)
        # ZIP 파일 압축해제 및 폴더에 저장
        #self.__save_unzip(binary_data, save_path)
        #return save_path

        # ZIP 파일 압축해제 및 XML 파싱 
        result = self.__parse_unzip_xml(binary_data, save_path)
        
        print(f"\n총 {len(result['xml_data'])}개 XML 파일 파싱 완료")
        return result

    def finance(self, corp_code: str, year: int, quarter: int = 4, api_type: str = "단일회사 전체 재무제표"):
        """
        OpenDart 정기보고서 재무정보
        corp_code으로만 조회가 가능, stock_code 및 기업명으로는 조회되지 않는다.
        https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS003

        단일회사 주요계정
        상장법인(유가증권, 코스닥) 및 주요 비상장법인(사업보고서 제출대상 & IFRS 적용)이 제출한 정기보고서 내에 XBRL재무제표의 주요계정과목(재무상태표, 손익계산서)을 제공합니다.
        https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS003&apiId=2019016

        다중회사 주요계정
        상장법인(유가증권, 코스닥) 및 주요 비상장법인(사업보고서 제출대상 & IFRS 적용)이 제출한 정기보고서 내에 XBRL재무제표의 주요계정과목(재무상태표, 손익계산서)을 제공합니다. (대상법인 복수조회 복수조회 가능)
        https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS003&apiId=2019017

        단일회사 전체 재무제표
        상장법인(유가증권, 코스닥) 및 주요 비상장법인(사업보고서 제출대상 & IFRS 적용)이 제출한 정기보고서 내에 XBRL재무제표의 모든계정과목을 제공합니다.
        https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS003&apiId=2019020

        XBRL택사노미재무제표양식
        금융감독원 회계포탈에서 제공하는 IFRS 기반 XBRL 재무제표 공시용 표준계정과목체계(계정과목) 을 제공합니다.
        https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS003&apiId=2020001

        단일회사 주요 재무지표
        상장법인(유가증권, 코스닥) 및 주요 비상장법인(사업보고서 제출대상 & IFRS 적용)이 제출한 정기보고서 내에 XBRL재무제표의 주요 재무지표를 제공합니다.
        https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS003&apiId=2022001

        다중회사 주요 재무지표
        상장법인(유가증권, 코스닥) 및 주요 비상장법인(사업보고서 제출대상 & IFRS 적용)이 제출한 정기보고서 내에 XBRL재무제표의 주요 재무지표를 제공합니다.(대상법인 복수조회 가능)
        https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS003&apiId=2022002

        보고서 코드 (reprt_code) : 단일회사 주요계정, 다중회사 주요계정, 단일회사 전체 재무제표, 단일회사 주요 재무지표, 다중회사 주요 재무지표
        1분기보고서 : 11013
        반기보고서 : 11012
        3분기보고서 : 11014
        사업보고서 : 11011

        지표분류코드 (idx_cl_code): 단일회사 주요 재무지표, 다중회사 주요 재무지표
        수익성지표 : M210000
        안정성지표 : M220000
        성장성지표 : M230000
        활동성지표 : M240000

        재무제표구분 (sj_div): XBRL택사노미재무제표양식
        ※재무제표구분 참조

        개별/연결구분 (fs_div): 단일회사 전체 재무제표
        OFS:재무제표
        CFS:연결재무제표

        Args:
            code (str): 기업 고유번호  (주식 코드(stock_code) 및 DART 고유번호 모두 가능(corp_code))
            year (int): 사업연도
            quarter (int): 분기, 사업보고서
        Returns:
            Dict: 기업개황
        """

        quarters = {
            "1": "11013", # 1분기보고서
            "2": "11012", # 반기보고서
            "3": "11014", # 3분기보고서
            "4": "11011"  # 사업보고서
        }

        #corp_code,bsns_year,stacnt_code,idx_cl_code
        report_code = quarters.get(str(quarter), "4") 

        params = {
            "crtfc_key": self.client.api_key,
            "corp_code": corp_code,
            "bsns_year": year,
            "reprt_code": report_code,
        }

        if api_type == "단일회사 전체 재무제표":
            params["fs_div"] = "OFS" # OFS:재무제표, CFS:연결재무제표
        elif api_type == "XBRL택사노미재무제표양식":
            params["sj_div"] = "BS1" # ※재무제표구분 참조
        elif api_type == "단일회사 주요 재무지표" or \
             api_type == "다중회사 주요 재무지표":
            params["idx_cl_code"] = "M210000" # 수익성지표 : M210000 안정성지표 : M220000 성장성지표 : M230000 활동성지표 : M240000

        # 기능 선택 방식에 대해서 고민 중
        url = self.finance_urls.get(api_type, "")

        print(f"URL: {url}, params: {params}")
        response = self.client._get(url, params=params)
        
        json_data = response.json()
        #print(json_data)
            
        status = json_data.get("status")

        # 에러 체크
        if status != "000":
            print(f"Error: {json_data.get('message')}")
            return {}

        self.corp_code = json_data.get("corp_code")
        self.corp_name = json_data.get("corp_name")
        self.stock_code = json_data.get("stock_code")

        return json_data

    def finance_file(self, rcept_no, quarter: int = 4, save_path: str | None = None):
        """
        OpenDart 정기보고서 재무정보 - 재무제표 원본파일(XBRL)

        상장법인(유가증권, 코스닥) 및 주요 비상장법인(사업보고서 제출대상 & IFRS 적용)이 제출한 정기보고서 내에 XBRL재무제표의 원본파일(XBRL)을 제공합니다.
        https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS003&apiId=2019019

        Args:
            rcept_no (str): 접수번호
            quarter (int): 분기, 사업보고서
            save_path (str): 파일 저장 경로
        Returns:
            pd.DataFrame: 기업개황
        """

        url = self.finance_urls.get("재무제표 원본파일(XBRL)", "")

        quarters = {
            "1": "11013", # 1분기보고서
            "2": "11012", # 반기보고서
            "3": "11014", # 3분기보고서
            "4": "11011"  # 사업보고서
        }
        report_code = quarters.get(str(quarter), "4")
        
        params = {
            "crtfc_key": self.client.api_key,
            "rcept_no": rcept_no,
            "reprt_code": report_code
        }

        response = self.client._get(url, params=params)
        response_headers = response.headers
        print(response_headers)

        if response.status_code != 200:
            print(f"다운로드 실패: {response.status_code}")
            return None

        content_type = response_headers.get("Content-Type")
        if "application/xml" in content_type:
            text_data = response.text
            print(text_data)
            return None

        # 바이너리 데이터 
        binary_data = response.content
        #print(binary_data)
        
        if save_path is None:
            save_path = f"dart_{rcept_no}"
            save_path = self.__save_zip_path(response_headers, save_path)

        # ZIP 파일 저장
        #self.__save_zip(binary_data, save_path)
        # ZIP 파일 압축해제 및 폴더에 저장
        self.__save_unzip(binary_data, save_path)
        return save_path
    
        """
        result = {
            'rcept_no': rcept_no,
        }                

        _result = self.__parse_unzip_xml(binary_data, save_path)
        result = result | _result
        
        print(f"\n총 {len(result['xml_data'])}개 XML 파일 파싱 완료")
        return result
        """

    def _parse_xml(self, xml_content, filename=None):
        """
        XML 내용 파싱
        Args:
            xml_content (bytes): XML 바이너리 내용
            filename (str): 파일명
        Returns:
            dict: 파싱된 데이터
        """
        from bs4 import BeautifulSoup
        
        # bytes를 str로 변환
        xml_str = None
        for encoding in ['utf-8', 'euc-kr', 'cp949']:
            try:
                xml_str = xml_content.decode(encoding)
                break
            except:
                continue
        
        if xml_str is None:
            return {'filename': filename, 'error': '인코딩 실패'}
        
        soup = BeautifulSoup(xml_str, 'lxml-xml')
        
        result = {
            'filename': filename,
            'title': None,
            'tables': [],
            'text_content': None,
            'list': []
        }
        
        # 제목 추출
        title_tag = soup.find('TITLE') or soup.find('title')
        if title_tag:
            result['title'] = title_tag.get_text(strip=True)
        
        # 테이블 추출
        tables = soup.find_all('TABLE') or soup.find_all('table')
        for idx, table in enumerate(tables):
            rows = []
            for tr in table.find_all(['TR', 'tr']):
                row = []
                for cell in tr.find_all(['TD', 'TH', 'td', 'th']):
                    text = cell.get_text(separator=' ', strip=True)
                    text = re.sub(r'\s+', ' ', text)
                    row.append(text)
                if row:
                    rows.append(row)
            
            if rows:
                # DataFrame으로 변환
                df = self._rows_to_dataframe(rows)
                
                result['tables'].append({
                    'index': idx,
                    'data': rows,
                    'dataframe': df
                })
        
        # 텍스트 내용 추출
        body = soup.find('BODY') or soup.find('body')
        if body:
            result['text_content'] = body.get_text(separator='\n', strip=True)

        # list 태그들 추출
        result_list = soup.find('result')
        if result_list:
            data = []
            for item in result_list.find_all('list'):
                # XML을 JSON 리스트로 변환
                row = {child.name: child.get_text(strip=True) for child in item.children if child.name}
                data.append(row)
            result['list'] = data
        
        return result

    def _rows_to_dataframe(self, rows):
        """
        2D 리스트를 DataFrame으로 안전하게 변환
        """
        if not rows:
            return pd.DataFrame()
        
        if len(rows) == 1:
            return pd.DataFrame(rows)
        
        # 최대 컬럼 수 계산
        max_cols = max(len(row) for row in rows)
        
        # 모든 행의 길이를 최대 컬럼 수에 맞춤
        normalized = []
        for row in rows:
            if len(row) < max_cols:
                row = row + [''] * (max_cols - len(row))
            normalized.append(row)
        
        # 첫 행을 헤더로 사용
        header = normalized[0]
        data = normalized[1:]
        
        # 빈 헤더 처리
        header = [f"col_{i}" if not h else h for i, h in enumerate(header)]
        
        # 헤더 중복 처리
        seen = {}
        unique_header = []
        for col in header:
            if col in seen:
                seen[col] += 1
                unique_header.append(f"{col}_{seen[col]}")
            else:
                seen[col] = 0
                unique_header.append(col)
        
        return pd.DataFrame(data, columns=unique_header)

    def __save_zip_path(self, headers, save_path):
        save_path = None
        
        content_disposition = headers.get("Content-Disposition", "")
        # filename="..." 또는 filename*=UTF-8''... 패턴 찾기
        match = re.search(r"filename\*?=['\"]?(?:UTF-8'')?([^'\";\n]+)", content_disposition)
        if match:
            filename = match.group(1)

            # URL 인코딩된 경우
            if '%' in filename:
                filename = unquote(filename)
            
            # 깨진 인코딩 복원
            save_path = decode_euc_kr(filename)
                
            # .zip 확장자 제거하여 폴더명으로 사용
            save_path = filename.replace('.zip', '')

        return save_path

    def __save_zip(self, binary_data, save_path):
        # 파일 저장
        with open(save_path, 'wb') as f:
            f.write(binary_data)
            print(f"저장 완료: {save_path}")

    def __save_unzip(self, binary_data, save_path):
        # ZIP 압축 해제
        with zipfile.ZipFile(io.BytesIO(binary_data)) as zf:
            # 파일 목록 출력
            file_list = zf.namelist()
            print(f"압축 파일 내 {len(file_list)}개 파일:")
            for fname in file_list:
                print(f"  - {fname}")
            
            # 전체 압축 해제
            zf.extractall(save_path)
        
        print(f"압축 해제 완료: {save_path}")

    def __parse_unzip_xml(self, binary_data, save_path):
        result = {
            'files': [],
            'xml_data': []
        }
        
        # ZIP 압축 해제 (메모리에서)
        with zipfile.ZipFile(io.BytesIO(binary_data)) as zf:
            file_list = zf.namelist()
            print(f"압축 파일 내 {len(file_list)}개 파일:")
            
            for fname in file_list:
                # 파일명 인코딩 수정
                enc_name = decode_euc_kr(fname)
                
                print(f"  - {enc_name}")
                result['files'].append(enc_name)
                
                # XML 파일만 파싱
                if fname.endswith('.xml'):
                    content = zf.read(fname)
                    parsed = self._parse_xml(content, enc_name)
                    result['xml_data'].append(parsed)
        
        return result

    def _dateformat(self, date_str):
        """다양한 날짜 형식을 YYYYMMDD로 변환"""
        # 구분자(-, /, .) 제거
        return re.sub(r'[-/.]', '', date_str)