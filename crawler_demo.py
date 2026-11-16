# -*- coding: utf-8 -*-
"""示例：抓 quotes.toscrape.com 的作者页。

换成自己的站改两处：config.py 的 START_URLS / ALLOWED_URL_PATTERNS /
CONTENT_URL_PATTERNS，以及下面 parse_content_page 里要哪些字段。
"""

import time

from bs4 import BeautifulSoup

from common_crawler import CommonCrawler


class QuotesCrawler(CommonCrawler):
    """输出作者姓名、生卒、简介。"""

    def parse_content_page(self, html: str, url: str) -> dict:
        soup = BeautifulSoup(html, "html.parser")

        title = soup.title.get_text(strip=True) if soup.title else ""
        author_tag = soup.select_one("h3.author-title")
        born_tag = soup.select_one("span.author-born-date")
        born_place = soup.select_one("span.author-born-location")

        # 取详情块，取不到就退回整个 body 的纯文本
        block = soup.select_one("div.author-details") or soup.body
        text = block.get_text(" ", strip=True)[:1000] if block else ""

        return {
            "task_name": self.task_name,
            "url": url,
            "title": title,
            "author": author_tag.get_text(strip=True) if author_tag else "",
            "born_date": born_tag.get_text(strip=True) if born_tag else "",
            "born_place": born_place.get_text(strip=True) if born_place else "",
            "text": text,
            "crawl_time": time.strftime("%Y-%m-%d %H:%M:%S"),
        }


if __name__ == "__main__":
    QuotesCrawler().start()
