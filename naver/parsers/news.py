class NaverNewsParser:
    """
    뉴스 검색 클래스
    
    Independent Naver news pipeline (no NaverCrawler dependency).
    """

    news_categories = {
        '정치': 'https://news.naver.com/section/100',
        '경제': 'https://news.naver.com/section/101',
        '사회': 'https://news.naver.com/section/102',
        '생활/문화': 'https://news.naver.com/section/103',
        'IT/과학': 'https://news.naver.com/section/105',
        '세계': 'https://news.naver.com/section/104'
    }

    max_per_category = 10
    news_openapi_url = "https://openapi.naver.com/v1/search/news.json?query={query}&display={display}"

    client_id = 'EOof636e7yvLvMe3t1jg'
    client_secret = 'lb4v_qXkRI'

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
    }
    
    def __init__(self):
        self.client = httpx.AsyncClient(headers=self.headers, follow_redirects=True)

    async def fetch(
        self,
        category: str = "조회",
        query: str | None = None,
        url: str | None = None,
        max_articles: int = 100
    ) -> List[Dict]:
        """
        질문으로 Naver News API를 뉴스 목록을 조회한다.
        카테고리에 대한 뉴스 목록을 조회한다.
        
        Args:
            category (str): 조회, 카테고리 (정치, 경제, 사회, 생활/문화, IT/과학, 세계). 기본값: 조회
            query (str): 기사 검색 문구
            url (str): 뉴스 검색 URL
            max_articles (int): 최대 뉴스 갯수

        Returns:
            뉴스 Dictionary 목록
        """

        if category != "조회":
            return self._fetch_category_news()
        
        url = self.news_openapi_url if not url else url
        enc_text = parse.quote(query)
        api_url = url.format(query=enc_text, display=max_articles)
        
        print(f"News 목록 URL: {api_url}")

        api_headers = {
            "X-Naver-Client-Id": self.client_id,
            "X-Naver-Client-Secret": self.client_secret
        }

        articles = []
        try:
            response = await self.client.get(api_url, headers=api_headers)
            response.raise_for_status()
            search_result = response.json()

            # JSON에서 link 정보 추출
            news_list = search_result.get('items', [])
            #print(f"Items: {news_list}")
            for news_item in news_list:
                article = {
                    'query': query,
                    'url': news_item.get("link")
                }
                articles.append(article)
            
            return articles
        except Exception as e:
            print(f"Exception: {e}")
        
        return articles

    async def parse(
        self,
        articles: List[Dict]
    ) -> List[Dict]:
        """
        뉴스 목록으로 뉴스 상세 정보를 가져온다.
        제목, 내용, 언론사, 입력일, 기자 목록
        카테고리 X

        Args:
            news_list (List): 신문 기사 URL 목록

        Returns:
            뉴스 상세 Dictionary 목록
        """

        for idx, article in enumerate(articles):
            news_url = article.get('url')
            
            if news_url and 'news.naver.com' in news_url:
                detail = NaverNewsDetail()
                article = await detail.fetch(article)
            
            await asyncio.sleep(random.uniform(0.1, 0.3))

        return articles

    def _fetch_category_news(self):
        """
        카테고리별 뉴스 목록을 가져온다.
        """
        
        articles = []
        for category_name, category_url in categories.items():
            print(f"News 목록 URL: {category_url}")
            article_links = get_article_links(driver, category_url, self.max_per_category)

            for url in article_links:
                article = {
                    'query': category_name,
                    'url': url
                }
                articles.append(article)

        return articles

class NaverNewsDetail:
    
    # 제목
    title_selectors = [
        '#title_area span',
        '#ct .media_end_head_headline',
        '.media_end_head_headline',
        'h2#title_area',
        '.news_end_title'
    ]

    # 본문
    content_selectors = [
        '#dic_area',
        'article#dic_area',
        '.go_trans._article_content',
        '._article_body_contents'
    ]
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
    }
    
    def __init__(
        self, 
        url: str | None = None
    ):
        self.client = httpx.AsyncClient(headers=self.headers, follow_redirects=True)
        self.url = url

    async def fetch(
        self,
        article: Dict | None = None
    ):
        news_url = article.get('url')
        
        try:
            print(f"News Detail URL: {news_url}")
            response = await self.client.get(news_url)
            response.raise_for_status()

            # 뉴스 상세 정보 파싱
            _article = self._parse_news(response.text)
            
            article.update(_article)


        except Exception as e:
            print(f"Error scraping {news_url}: {e}")

        finally:
            return article

    def _parse_news(
        self,
        html_text: str
    ) -> Dict:
        """
        HTML에서 뉴스 상세 정보를 파싱한다.
        제목, 본문 내용, 언론사, 입력일, 기자 목록

        Args:
            html_text (str): HTML 텍스트

        Returns:
            뉴스 상세 Dictionary
        """
        soup = BeautifulSoup(html_text, 'html.parser')

        # 제목
        title = self._parse_title(soup)
        # 본문 내용
        content = self._parse_content(soup)
        # 언론사
        press = self._parse_press(soup)
        # 입력일
        published_date = self._parse_published_date(soup)
        # 기자 목록
        authors = self._parse_authors(soup)
        # 카테고리
        category = self._parse_category(soup)

        article = {
            'title': title,
            'content': content,
            'press': press,
            'authors': (", ").join(authors),
            'category': category,
            'published_date': published_date,
            'crawled_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        #print(f"Article: {article}")
        return article

    def _parse_title(
        self,
        soup
    ) -> str:
        """
        HTML에서 제목을 파싱한다.

        Args:
            soup:

        Returns:
            제목 문자열
        """
        title = "제목 없음"
        
        for selector in self.title_selectors:
            try:
                title_element = soup.select_one(selector)
                title = title_element.get_text(strip=True)
                break
            except:
                continue

        return title

    def _parse_content(
        self,
        soup
    ) -> str:
        """
        HTML에서 본문 내용을 파싱한다.

        Args:
            soup:

        Returns:
            본문 내용 문자열
        """
        content = "본문 없음"
        
        for selector in self.content_selectors:
            try:
                content_element = soup.select_one(selector)
                content = content_element.get_text(strip=True)
                break
            except:
                continue

        return content

    def _parse_press(
        self,
        soup
    ) -> str:
        """
        HTML에서 언론사를 파싱한다.

        Args:
            soup:

        Returns:
            언론사 문자열
        """
        press = "언론사 불명"
        
        try:
            press_element = soup.select_one('a.media_end_head_top_logo img')
            press = press_element.get('alt')
        except:
            try:
                press_element = soup.select_one('.media_end_head_top_logo_text')
                press = press_element['alt']
            except:
                pass

        return press

    def _parse_published_date(
        self,
        soup
    ) -> str:
        """
        HTML에서 뉴스 입력일을 파싱한다.

        Args:
            soup:

        Returns:
            뉴스 입력일 문자열
        """
        published_date = "뉴스 입력일 불명"
        
        try:
            date_element = soup.select_one('span.media_end_head_info_datestamp_time')
            published_date = date_element.get('data-date-time')
        except:
            published_date = datetime.now().strftime('%Y-%m-%d %H:%M')

        return published_date

    def _parse_authors(
        self,
        soup
    ) -> List:
        """
        HTML에서 기자 목록을 파싱한다.

        Args:
            soup:

        Returns:
            기자 목록
        """
        authors = []
        
        try:
            #author_elements = soup.select('em.media_end_head_journalist_name')
            #author = author_element.get_text(strip=True)
            
            author_elements = soup.select('span.byline_s')
            for author_element in author_elements:
                author = author_element.get_text(strip=True)
                authors.append(author)
        except Exception as e:
            print(f"Exception: {e}")

        return authors
    
    def _parse_category(
        self,
        soup
    ) -> str:
        """
        HTML에서 카테고리를 파싱한다.

        Args:
            soup:

        Returns:
            카테고리
        """
        authors = []
        
        try:
            category_element = soup.select_one('em.media_end_categorize_item')
            category = category_element.get_text(strip=True)
        except Exception as e:
            print(f"Exception: {e}")

        return category