import logging
from typing import Optional, Dict, List

import config
from src.utils import HttpClient

logger = logging.getLogger(__name__)


class DaDataClient:
    """
    Клиент для DaData API.
    
    Бесплатный лимит: 10000 запросов в день.
    Документация: https://dadata.ru/api/
    """
    
    BASE_URL = 'https://suggestions.dadata.ru/suggestions/api/4_1/rs'
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or config.DADATA_API_KEY
        self.client = HttpClient()
        self.enabled = bool(self.api_key)
        
        if not self.enabled:
            logger.warning('DaData API ключ не указан. Функции обогащения через DaData недоступны.')
    
    def find_company(self, query: str) -> Optional[Dict]:
        """
        Ищет компанию по названию или ИНН.
        
        Args:
            query: название компании или ИНН
        
        Returns:
            Данные о компании
        """
        if not self.enabled:
            return None
        
        import requests
        
        url = f'{self.BASE_URL}/suggest/party'
        headers = {
            'Authorization': f'Token {self.api_key}',
            'Content-Type': 'application/json',
        }
        data = {
            'query': query,
            'count': 1,
        }
        
        try:
            response = requests.post(url, headers=headers, json=data, timeout=10)
            
            if response.status_code != 200:
                logger.warning(f'DaData вернул {response.status_code}')
                return None
            
            result = response.json()
            suggestions = result.get('suggestions', [])
            
            if not suggestions:
                return None
            
            return self._parse_suggestion(suggestions[0])
            
        except Exception as e:
            logger.error(f'Ошибка DaData: {e}')
            return None
    
    def _parse_suggestion(self, suggestion: dict) -> Dict:
        """Парсит ответ DaData."""
        data = suggestion.get('data', {})
        
        result = {
            'inn': data.get('inn'),
            'ogrn': data.get('ogrn'),
            'name': suggestion.get('value', ''),
            'full_name': data.get('name', {}).get('full_with_opf', ''),
            'okved_main': data.get('okved'),
        }
        
        # Адрес
        address = data.get('address', {})
        if address:
            result['address'] = address.get('value', '')
            result['region'] = address.get('data', {}).get('region_with_type', '')
        
        # Статус
        state = data.get('state', {})
        result['status'] = state.get('status')
        
        # Руководитель
        management = data.get('management', {})
        if management:
            result['ceo_name'] = management.get('name')
            result['ceo_post'] = management.get('post')
        
        return result
    
    def enrich_companies(self, companies: List[Dict]) -> List[Dict]:
        """Обогащает список компаний данными из DaData."""
        if not self.enabled:
            return companies
        
        from tqdm import tqdm
        
        logger.info(f'Обогащение через DaData для {len(companies)} компаний')
        
        enriched = []
        for company in tqdm(companies, desc='DaData'):
            # Ищем по ИНН если есть, иначе по названию
            query = company.get('inn') or company.get('name', '')
            if not query:
                enriched.append(company)
                continue
            
            dadata_info = self.find_company(query)
            if dadata_info:
                merged = company.copy()
                for key, value in dadata_info.items():
                    if value and (key not in merged or not merged[key]):
                        merged[key] = value
                enriched.append(merged)
            else:
                enriched.append(company)
        
        return enriched