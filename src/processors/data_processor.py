import re
import logging
from typing import List, Dict, Optional, Set

import config

logger = logging.getLogger(__name__)


class DataProcessor:
    """Обработка и очистка данных о компаниях."""
    
    def __init__(self):
        self.seen_inns: Set[str] = set()
        self.seen_names: Set[str] = set()
    
    def process(self, companies: List[Dict]) -> List[Dict]:
        """
        Полный цикл обработки данных.
        
        1. Очистка полей
        2. Фильтрация по ИТ-профилю
        3. Фильтрация по количеству сотрудников
        4. Дедупликация
        5. Сортировка
        """
        logger.info(f'Начало обработки {len(companies)} записей')
        
        # Очистка
        cleaned = [self._clean_record(c) for c in companies]
        cleaned = [c for c in cleaned if c]  # убираем None
        logger.info(f'После очистки: {len(cleaned)}')
        
        # Фильтрация по ИТ
        it_only = [c for c in cleaned if self._is_it_company(c)]
        logger.info(f'После фильтра ИТ: {len(it_only)}')
        
        # Фильтрация по сотрудникам
        with_employees = [c for c in it_only if self._has_enough_employees(c)]
        logger.info(f'После фильтра сотрудников (>={config.MIN_EMPLOYEES}): {len(with_employees)}')
        
        # Дедупликация
        unique = self._deduplicate(with_employees)
        logger.info(f'После дедупликации: {len(unique)}')
        
        # Сортировка по количеству сотрудников
        unique.sort(key=lambda x: x.get('employees', 0), reverse=True)
        
        return unique
    
    def _clean_record(self, record: Dict) -> Optional[Dict]:
        """Очищает одну запись."""
        result = {}
        
        # ИНН
        inn = self._normalize_inn(record.get('inn'))
        if inn:
            result['inn'] = inn
        
        # Название
        name = self._clean_name(record.get('name') or record.get('full_name', ''))
        if not name:
            return None
        result['name'] = name
        
        # Количество сотрудников
        employees = self._normalize_employees(record)
        if employees:
            result['employees'] = employees
        
        # ОКВЭД
        okved = self._normalize_okved(record.get('okved_main'))
        if okved:
            result['okved_main'] = okved
        
        # Сайт
        site = self._normalize_url(record.get('site'))
        if site:
            result['site'] = site
        
        # Описание
        desc = record.get('description', '')
        if desc:
            result['description'] = self._clean_text(desc)[:500]
        
        # Регион
        region = record.get('region', '')
        if region:
            result['region'] = region.strip()
        
        # Источник
        sources = set()
        if record.get('source'):
            sources.add(record['source'])
        if record.get('source_url'):
            sources.add('list-org')
        result['source'] = ', '.join(sorted(sources)) if sources else 'unknown'
        
        # Выручка
        if record.get('revenue'):
            result['revenue'] = record['revenue']
            result['revenue_year'] = record.get('revenue_year', 2023)
        
        # Контакты
        if record.get('address'):
            result['contacts'] = record['address'][:200]
        
        return result
    
    def _normalize_inn(self, inn_raw) -> Optional[str]:
        """Нормализует ИНН."""
        if not inn_raw:
            return None
        
        inn_str = re.sub(r'\D', '', str(inn_raw))
        
        if len(inn_str) == 10 or len(inn_str) == 12:
            return inn_str
        
        if len(inn_str) == 9:
            return '0' + inn_str
        
        return None
    
    def _clean_name(self, name: str) -> str:
        """Очищает название компании."""
        if not name:
            return ''
        
        name = name.strip()
        name = re.sub(r'\s+', ' ', name)
        name = re.sub(r'^[«"\']+|[»"\']+$', '', name)
        
        return name.strip()
    
    def _normalize_employees(self, record: Dict) -> Optional[int]:
        """Извлекает количество сотрудников."""
        # Пробуем разные поля
        for field in ['employees', 'employees_hh', 'staff_count']:
            value = record.get(field)
            if value is None:
                continue
            
            if isinstance(value, (int, float)):
                return int(value)
            
            # Парсим строку
            parsed = self._parse_employees_str(str(value))
            if parsed:
                return parsed
        
        return None
    
    def _parse_employees_str(self, text: str) -> Optional[int]:
        """Парсит количество сотрудников из строки."""
        if not text:
            return None
        
        text = text.lower().strip()
        text = re.sub(r'\s+', '', text)
        
        numbers = [int(n) for n in re.findall(r'\d+', text)]
        if not numbers:
            return None
        
        # Для диапазонов берём нижнюю границу
        if 'от' in text or 'более' in text or 'свыше' in text or '+' in text:
            return max(numbers)
        
        if len(numbers) >= 2:
            return min(numbers)
        
        return numbers[0]
    
    def _normalize_okved(self, okved: str) -> str:
        """Нормализует ОКВЭД."""
        if not okved:
            return ''
        
        match = re.search(r'(\d+\.[\d.]*)', str(okved))
        return match.group(1) if match else ''
    
    def _normalize_url(self, url: str) -> str:
        """Нормализует URL."""
        if not url:
            return ''
        
        url = url.strip().lower()
        url = re.sub(r'^https?://', '', url)
        url = re.sub(r'^www\.', '', url)
        url = url.rstrip('/')
        
        return url
    
    def _clean_text(self, text: str) -> str:
        """Очищает текст."""
        if not text:
            return ''
        
        text = re.sub(r'<[^>]+>', ' ', text)
        text = re.sub(r'\s+', ' ', text)
        
        return text.strip()
    
    def _is_it_company(self, company: Dict) -> bool:
        """Проверяет, является ли компания ИТ-компанией."""
        # Проверяем ОКВЭД
        okved = company.get('okved_main', '')
        for prefix in config.IT_OKVED_PREFIXES:
            if okved.startswith(prefix):
                return True
        
        # Проверяем ключевые слова
        text = ' '.join([
            company.get('name', ''),
            company.get('description', ''),
            company.get('industries', ''),
        ]).lower()
        
        for keyword in config.IT_KEYWORDS:
            if keyword in text:
                return True
        
        return False
    
    def _has_enough_employees(self, company: Dict) -> bool:
        """Проверяет, достаточно ли сотрудников."""
        employees = company.get('employees')
        if employees is None:
            return False
        return employees >= config.MIN_EMPLOYEES
    
    def _deduplicate(self, companies: List[Dict]) -> List[Dict]:
        """Удаляет дубликаты."""
        unique = []
        
        for company in companies:
            inn = company.get('inn', '')
            name = company.get('name', '').lower()
            
            # Проверяем по ИНН
            if inn:
                if inn in self.seen_inns:
                    continue
                self.seen_inns.add(inn)
            else:
                # Если нет ИНН, проверяем по названию
                if name in self.seen_names:
                    continue
                self.seen_names.add(name)
            
            unique.append(company)
        
        return unique