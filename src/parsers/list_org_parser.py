import re
import logging
from typing import List, Dict, Optional
from urllib.parse import quote

from bs4 import BeautifulSoup
from tqdm import tqdm

import config
from src.utils import HttpClient

logger = logging.getLogger(__name__)


class ListOrgParser:
    """
    Парсер данных с list-org.com.
    
    Позволяет искать компании и получать данные из ЕГРЮЛ:
    - ИНН
    - ОКВЭД
    - Количество сотрудников
    - Выручка
    """
    
    BASE_URL = 'https://www.list-org.com'
    
    def __init__(self):
        self.client = HttpClient()
    
    def search_company(self, company_name: str) -> Optional[Dict]:
        """
        Ищет компанию по названию и возвращает данные.
        
        Args:
            company_name: название компании
        
        Returns:
            Словарь с данными компании или None
        """
        # Очищаем название для поиска
        search_name = self._prepare_search_query(company_name)
        
        # Поиск на list-org
        search_url = f'{self.BASE_URL}/search'
        params = {
            'type': 'all',
            'val': search_name,
        }
        
        response = self.client.get(search_url, params=params)
        if not response:
            return None
        
        # Парсим результаты поиска
        soup = BeautifulSoup(response.content, 'lxml')
        
        # Ищем ссылку на первый результат
        results = soup.select('div.org_list p.org a')
        if not results:
            return None
        
        # Берём первый результат
        company_url = results[0].get('href')
        if not company_url:
            return None
        
        # Загружаем страницу компании
        if not company_url.startswith('http'):
            company_url = f'{self.BASE_URL}{company_url}'
        
        return self._parse_company_page(company_url)
    
    def enrich_companies(self, companies: List[Dict]) -> List[Dict]:
        """
        Обогащает список компаний данными с list-org.
        
        Args:
            companies: список компаний с полем 'name'
        
        Returns:
            Обогащённый список
        """
        logger.info(f'Обогащение данных с list-org.com для {len(companies)} компаний')
        
        enriched = []
        found_count = 0
        
        for company in tqdm(companies, desc='Поиск на list-org'):
            name = company.get('name', '')
            if not name:
                enriched.append(company)
                continue
            
            # Ищем данные на list-org
            list_org_data = self.search_company(name)
            
            if list_org_data:
                # Объединяем данные
                merged = company.copy()
                for key, value in list_org_data.items():
                    if value and (key not in merged or not merged[key]):
                        merged[key] = value
                enriched.append(merged)
                found_count += 1
            else:
                enriched.append(company)
        
        logger.info(f'Найдено на list-org: {found_count} из {len(companies)}')
        return enriched
    
    def _prepare_search_query(self, name: str) -> str:
        """Подготавливает название для поиска."""
        # Убираем кавычки и лишние символы
        name = re.sub(r'[«»"\'()]', '', name)
        # Убираем организационно-правовую форму
        name = re.sub(r'\b(ООО|ОАО|ЗАО|ПАО|АО)\b', '', name, flags=re.IGNORECASE)
        return name.strip()
    
    def _parse_company_page(self, url: str) -> Optional[Dict]:
        """Парсит страницу компании на list-org."""
        response = self.client.get(url)
        if not response:
            return None
        
        soup = BeautifulSoup(response.content, 'lxml')
        
        result = {'source_url': url}
        
        # Парсим таблицу с данными
        info_table = soup.select_one('table.tt')
        if not info_table:
            return None
        
        for row in info_table.select('tr'):
            cells = row.select('td')
            if len(cells) >= 2:
                label = cells[0].get_text(strip=True).lower()
                value = cells[1].get_text(strip=True)
                
                if 'инн' in label:
                    inn = re.search(r'\d{10,12}', value)
                    if inn:
                        result['inn'] = inn.group()
                
                elif 'огрн' in label:
                    ogrn = re.search(r'\d{13,15}', value)
                    if ogrn:
                        result['ogrn'] = ogrn.group()
                
                elif 'оквэд' in label and 'основной' in label:
                    okved_match = re.search(r'(\d+\.[\d.]+)', value)
                    if okved_match:
                        result['okved_main'] = okved_match.group(1)
                
                elif 'сотрудник' in label or 'численность' in label:
                    emp_match = re.search(r'(\d+)', value.replace(' ', ''))
                    if emp_match:
                        result['employees'] = int(emp_match.group(1))
                
                elif 'выручка' in label:
                    revenue = self._parse_revenue(value)
                    if revenue:
                        result['revenue'] = revenue
                
                elif 'адрес' in label and 'юридический' in label:
                    result['address'] = value
        
        # Пробуем получить название
        title = soup.select_one('h1')
        if title:
            result['full_name'] = title.get_text(strip=True)
        
        return result if result.get('inn') else None
    
    def _parse_revenue(self, text: str) -> Optional[int]:
        """Парсит выручку из текста."""
        if not text:
            return None
        
        text = text.lower().replace(' ', '').replace('\xa0', '')
        
        # Ищем число
        match = re.search(r'([\d,]+)', text)
        if not match:
            return None
        
        try:
            value = float(match.group(1).replace(',', '.'))
            
            # Определяем множитель
            if 'млрд' in text:
                value *= 1_000_000_000
            elif 'млн' in text:
                value *= 1_000_000
            elif 'тыс' in text:
                value *= 1_000
            
            return int(value)
        except ValueError:
            return None
    
    def collect_by_okved(self, okved_prefix: str, max_pages: int = 10) -> List[Dict]:
        """
        Собирает компании по коду ОКВЭД.
        
        Args:
            okved_prefix: префикс ОКВЭД (например, '62.01')
            max_pages: максимум страниц для парсинга
        
        Returns:
            Список компаний
        """
        logger.info(f'Сбор компаний по ОКВЭД {okved_prefix}')
        
        companies = []
        
        for page in range(1, max_pages + 1):
            url = f'{self.BASE_URL}/okved/{okved_prefix}'
            if page > 1:
                url = f'{url}/{page}'
            
            response = self.client.get(url)
            if not response:
                break
            
            soup = BeautifulSoup(response.content, 'lxml')
            
            # Ищем ссылки на компании
            org_links = soup.select('div.org_list p.org a')
            if not org_links:
                break
            
            for link in org_links:
                href = link.get('href', '')
                name = link.get_text(strip=True)
                
                if href and name:
                    companies.append({
                        'name': name,
                        'list_org_url': f'{self.BASE_URL}{href}' if not href.startswith('http') else href,
                    })
            
            # Проверяем, есть ли следующая страница
            pagination = soup.select('div.pagination a')
            has_next = any('>' in a.get_text() or str(page + 1) == a.get_text(strip=True) 
                          for a in pagination)
            if not has_next:
                break
        
        logger.info(f'Найдено {len(companies)} компаний по ОКВЭД {okved_prefix}')
        return companies