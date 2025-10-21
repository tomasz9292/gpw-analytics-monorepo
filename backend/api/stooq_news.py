"""Utilities for downloading GPW-related news items from Stooq."""

from __future__ import annotations

import random
import time
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urljoin

from .company_ingestion import SimpleHttpSession
from .symbols import to_stooq_symbol


STOOQ_NEWS_URL = "https://stooq.pl/q/n/"


def _clean_text(value: str) -> str:
    return " ".join(value.split()).strip()


@dataclass
class NewsItem:
    symbol: str
    title: str
    url: str
    published_at: Optional[str] = None


class _StooqNewsParser(HTMLParser):
    """A forgiving HTML parser that extracts news links from Stooq pages."""

    def __init__(self) -> None:
        super().__init__()
        self.items: List[Dict[str, Any]] = []
        self._capture_title = False
        self._capture_date = False
        self._current_href: Optional[str] = None
        self._current_title_parts: List[str] = []
        self._current_date_parts: List[str] = []

    def handle_starttag(self, tag: str, attrs: Iterable[tuple[str, Optional[str]]]) -> None:
        attrs_dict = {key: value for key, value in attrs}
        if tag == "a":
            href = attrs_dict.get("href")
            if href and href.startswith("/n/?i="):
                self._capture_title = True
                self._current_href = href
                self._current_title_parts = []
        elif tag == "span":
            css_class = attrs_dict.get("class", "")
            if "f11" in css_class or "f12" in css_class:
                self._capture_date = True
                self._current_date_parts = []

    def handle_endtag(self, tag: str) -> None:
        if tag == "a" and self._capture_title:
            title = _clean_text("".join(self._current_title_parts))
            if title and self._current_href:
                self.items.append({"title": title, "href": self._current_href})
            self._capture_title = False
            self._current_href = None
            self._current_title_parts = []
        elif tag == "span" and self._capture_date:
            date_text = _clean_text("".join(self._current_date_parts))
            if date_text and self.items:
                self.items[-1]["published_at"] = date_text
            self._capture_date = False
            self._current_date_parts = []

    def handle_data(self, data: str) -> None:
        if self._capture_title:
            self._current_title_parts.append(data)
        elif self._capture_date:
            self._current_date_parts.append(data)


class StooqCompanyNewsHarvester:
    """Fetches recent company news snippets from Stooq."""

    def __init__(
        self,
        session: Optional[Any] = None,
        *,
        base_url: str = STOOQ_NEWS_URL,
        min_delay: float = 0.8,
        max_delay: float = 2.5,
    ) -> None:
        if session is None:
            session = SimpleHttpSession(
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0 Safari/537.36"
                    ),
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Referer": "https://stooq.pl/",
                }
            )
        self.session = session
        self.base_url = base_url.rstrip("/") + "/"
        self.min_delay = max(0.0, float(min_delay))
        self.max_delay = max(self.min_delay, float(max_delay))

    def _throttle(self) -> None:
        wait = random.uniform(self.min_delay, self.max_delay)
        time.sleep(wait)

    def fetch_news(self, symbol: str, *, limit: int = 20) -> List[NewsItem]:
        normalized = to_stooq_symbol(symbol).lower()
        url = f"{self.base_url}?s={normalized}"
        response = self.session.get(url)
        raise_for_status = getattr(response, "raise_for_status", None)
        if callable(raise_for_status):
            raise_for_status()
        parser = _StooqNewsParser()
        text_getter = getattr(response, "text", None)
        document: str
        if callable(text_getter):
            document = text_getter()  # type: ignore[call-arg]
        else:
            content = getattr(response, "content", None)
            if isinstance(content, (bytes, bytearray)):
                document = content.decode("utf-8", errors="replace")
            else:
                document = str(content)
        parser.feed(document)
        news_items: List[NewsItem] = []
        for item in parser.items:
            title = _clean_text(str(item.get("title", "")))
            href = str(item.get("href", ""))
            if not title or not href:
                continue
            full_url = urljoin(self.base_url, href)
            news_items.append(
                NewsItem(
                    symbol=symbol,
                    title=title,
                    url=full_url,
                    published_at=item.get("published_at"),
                )
            )
            if len(news_items) >= limit:
                break
        self._throttle()
        return news_items


__all__ = [
    "NewsItem",
    "StooqCompanyNewsHarvester",
]

