import logging
from typing import List, Dict, Optional, Set
from tqdm import tqdm

import config
from src.utils import HttpClient

logger = logging.getLogger(__name__)


class HHParser:
    """
    Парсер работодателей с HeadHunter API.
    
    Использует публичный бесплатный API HH.ru:
    https://api.hh.ru/openapi/redoc
    """
    
    BASE_URL = 'https://api.hh.ru'
    
    def __init__(self):
        self.client = HttpClient()
        self.collected_ids: Set[str] = set()
    
    def parse(self, max_companies: int = 500) -> List[Dict]:
        """
        Собирает список ИТ-компаний с HH.ru.
        
        Args:
            max_companies: максимальное количество компаний
        
        Returns:
            Список компаний с базовой информацией
        """
        logger.info('Начинаем парсинг HH.ru...')
        
        companies = []
        
        # Шаг 1: Собираем работодателей из ИТ-индустрий
        logger.info('Шаг 1: Сбор по ИТ-отраслям')
        for industry_id in config.HH_IT_INDUSTRIES:
            industry_companies = self._collect_by_industry(industry_id)
            companies.extend(industry_companies)
            
            if len(companies) >= max_companies:
                break
        
        logger.info(f'Собрано после отраслей: {len(companies)}')
        
        # Шаг 2: Поиск по ключевым словам
        logger.info('Шаг 2: Поиск по ключевым словам')
        search_queries = [
            'разработка программного обеспечения',
            'IT компания',
            'software development',
            'системная интеграция',
            'облачные технологии',
            'информационные технологии',
            'fintech',
            'SaaS',
        ]
        
        for query in search_queries:
            if len(companies) >= max_companies:
                break
            
            query_companies = self._search_employers(query)
            companies.extend(query_companies)
        
        logger.info(f'Всего уникальных работодателей: {len(self.collected_ids)}')
        
        # Шаг 3: Получаем детальную информацию
        logger.info('Шаг 3: Получение детальной информации')
        detailed = self._enrich_companies(companies[:max_companies])
        
        return detailed
    
    def _collect_by_industry(self, industry_id: str, max_pages: int = 20) -> List[Dict]:
        """Собирает работодателей из конкретной отрасли."""
        companies = []
        
        for page in range(max_pages):
            data = self.client.get_json(
                f'{self.BASE_URL}/employers',
                params={
                    'industry': industry_id,
                    'area': '113',  # Россия
                    'only_with_vacancies': 'true',
                    'per_page': 100,
                    'page': page,
                }
            )
            
            if not data or 'items' not in data:
                break
            
            items = data['items']
            if not items:
                break
            
            for item in items:
                employer_id = str(item.get('id', ''))
                if employer_id and employer_id not in self.collected_ids:
                    self.collected_ids.add(employer_id)
                    companies.append(self._parse_short_info(item))
            
            # Проверяем пагинацию
            if page >= data.get('pages', 0) - 1:
                break
        
        return companies
    
    def _search_employers(self, query: str) -> List[Dict]:
        """Поиск работодателей по текстовому запросу."""
        companies = []
        
        data = self.client.get_json(
            f'{self.BASE_URL}/employers',
            params={
                'text': query,
                'area': '113',
                'only_with_vacancies': 'true',
                'per_page': 100,
            }
        )
        
        if not data or 'items' not in data:
            return companies
        
        for item in data['items']:
            employer_id = str(item.get('id', ''))
            if employer_id and employer_id not in self.collected_ids:
                self.collected_ids.add(employer_id)
                companies.append(self._parse_short_info(item))
        
        return companies
    
    def _parse_short_info(self, item: dict) -> Dict:
        """Парсит краткую информацию о работодателе."""
        return {
            'hh_id': str(item.get('id', '')),
            'name': item.get('name', ''),
            'url': item.get('alternate_url', ''),
            'vacancies_url': item.get('vacancies_url', ''),
            'source': 'hh.ru',
        }
    
    def _enrich_companies(self, companies: List[Dict]) -> List[Dict]:
        """Обогащает данные о компаниях детальной информацией."""
        enriched = []
        
        for company in tqdm(companies, desc='Загрузка деталей с HH'):
            hh_id = company.get('hh_id')
            if not hh_id:
                continue
            
            details = self._get_employer_details(hh_id)
            if details:
                company.update(details)
            
            enriched.append(company)
        
        return enriched
    
    def _get_employer_details(self, employer_id: str) -> Optional[Dict]:
        """Получает детальную информацию о работодателе."""
        data = self.client.get_json(f'{self.BASE_URL}/employers/{employer_id}')
        
        if not data:
            return None
        
        # Парсим описание
        description = data.get('description', '') or ''
        description = self._clean_html(description)
        
        # Получаем регион
        area = data.get('area', {})
        region = area.get('name', '') if area else ''
        
        # Получаем отрасли
        industries = data.get('industries', [])
        industry_names = [ind.get('name', '') for ind in industries if ind.get('name')]
        
        # Сайт
        site = data.get('site_url', '')
        
        # Пробуем извлечь количество сотрудников из описания
        employees = self._extract_employees_from_text(description)
        
        return {
            'name': data.get('name', ''),
            'site': site,
            'description': description[:500] if description else '',
            'region': region,
            'industries': '; '.join(industry_names[:3]),
            'employees_hh': employees,
        }
    
    def _extract_employees_from_text(self, text: str) -> Optional[int]:
        """Пытается извлечь количество сотрудников из текста."""
        import re
        
        if not text:
            return None
        
        text_lower = text.lower()
        
        # Паттерны для поиска
        patterns = [
            r'(\d[\d\s]*)\s*(?:сотрудник|человек|специалист)',
            r'штат[:\s]+(\d[\d\s]*)',
            r'команд[ае][:\s]+(?:более\s+)?(\d[\d\s]*)',
            r'(\d[\d\s]*)\+?\s*(?:профессионал|эксперт)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text_lower)
            if match:
                num_str = match.group(1).replace(' ', '')
                try:
                    return int(num_str)
                except ValueError:
                    continue
        
        return None
    
    def _clean_html(self, html: str) -> str:
        """Очищает HTML от тегов."""
        import re
        if not html:
            return ''
        text = re.sub(r'<[^>]+>', ' ', html)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()