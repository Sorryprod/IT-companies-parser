import time
import random
import logging
from typing import Optional, Dict, Any

import requests
from fake_useragent import UserAgent

import config

logger = logging.getLogger(__name__)


class HttpClient:
    """HTTP-клиент с поддержкой ретраев и случайных задержек."""
    
    def __init__(self):
        self.session = requests.Session()
        try:
            self.ua = UserAgent()
        except Exception:
            self.ua = None
        
        self._setup_session()
    
    def _setup_session(self):
        """Настраивает сессию с базовыми заголовками."""
        self.session.headers.update({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        })
    
    def _get_user_agent(self) -> str:
        """Возвращает случайный User-Agent."""
        if self.ua:
            try:
                return self.ua.random
            except Exception:
                pass
        return 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    
    def _delay(self):
        """Случайная задержка между запросами."""
        delay = config.REQUEST_DELAY * (0.5 + random.random())
        time.sleep(delay)
    
    def get(self, url: str, params: Dict = None, headers: Dict = None,
            json_response: bool = False) -> Optional[Any]:
        """
        GET-запрос с автоматическими ретраями.
        
        Args:
            url: URL для запроса
            params: GET-параметры
            headers: дополнительные заголовки
            json_response: ожидаем JSON ответ
        
        Returns:
            Response объект или распарсенный JSON
        """
        request_headers = {'User-Agent': self._get_user_agent()}
        if headers:
            request_headers.update(headers)
        
        for attempt in range(config.MAX_RETRIES):
            try:
                self._delay()
                
                response = self.session.get(
                    url,
                    params=params,
                    headers=request_headers,
                    timeout=config.REQUEST_TIMEOUT
                )
                
                if response.status_code == 200:
                    if json_response:
                        return response.json()
                    return response
                
                elif response.status_code == 429:
                    wait = config.REQUEST_DELAY * (attempt + 2) * 2
                    logger.warning(f'Rate limit на {url}, ждём {wait:.1f}с')
                    time.sleep(wait)
                    
                elif response.status_code == 404:
                    logger.debug(f'Не найдено: {url}')
                    return None
                    
                else:
                    logger.warning(f'HTTP {response.status_code}: {url}')
                    
            except requests.exceptions.Timeout:
                logger.warning(f'Таймаут: {url} (попытка {attempt + 1})')
            except requests.exceptions.RequestException as e:
                logger.error(f'Ошибка запроса {url}: {e}')
                time.sleep(config.REQUEST_DELAY * 2)
        
        return None
    
    def get_json(self, url: str, params: Dict = None, headers: Dict = None) -> Optional[Dict]:
        """Удобный метод для JSON API."""
        return self.get(url, params, headers, json_response=True)