#!/usr/bin/env python3
"""
Парсер российских ИТ-компаний.

Собирает данные из:
- HH.ru API (работодатели в ИТ-сфере)
- List-org.com (данные ЕГРЮЛ: ИНН, ОКВЭД, сотрудники)
- DaData API (опционально, для обогащения)

Использование:
    python main.py

Результат сохраняется в data/companies.csv
"""

import os
import sys
import logging
from datetime import datetime

import pandas as pd

import config
from src.parsers import HHParser, ListOrgParser, DaDataClient
from src.processors import DataProcessor


def setup_logging():
    """Настройка логирования."""
    log_format = '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('parser.log', encoding='utf-8'),
        ]
    )
    
    # Уменьшаем verbosity сторонних библиотек
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)


def ensure_dirs():
    """Создаёт необходимые директории."""
    output_dir = os.path.dirname(config.OUTPUT_PATH)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)


def main():
    """Главная функция."""
    setup_logging()
    logger = logging.getLogger('main')
    
    logger.info('=' * 60)
    logger.info('Запуск парсера российских ИТ-компаний')
    logger.info(f'Время: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    logger.info('=' * 60)
    
    ensure_dirs()
    
    all_companies = []
    
    # === Этап 1: Сбор данных с HH.ru ===
    logger.info('\n' + '=' * 40)
    logger.info('ЭТАП 1: Сбор данных с HH.ru')
    logger.info('=' * 40)
    
    try:
        hh_parser = HHParser()
        hh_companies = hh_parser.parse(max_companies=400)
        logger.info(f'Собрано с HH.ru: {len(hh_companies)} работодателей')
        all_companies.extend(hh_companies)
    except Exception as e:
        logger.error(f'Ошибка при парсинге HH.ru: {e}')
    
    # === Этап 2: Обогащение через list-org ===
    logger.info('\n' + '=' * 40)
    logger.info('ЭТАП 2: Обогащение данных через list-org.com')
    logger.info('=' * 40)
    
    try:
        list_org_parser = ListOrgParser()
        all_companies = list_org_parser.enrich_companies(all_companies)
    except Exception as e:
        logger.error(f'Ошибка при обогащении через list-org: {e}')
    
    # === Этап 3: Дополнительный сбор по ОКВЭД (если мало данных) ===
    if len(all_companies) < 150:
        logger.info('\n' + '=' * 40)
        logger.info('ЭТАП 3: Дополнительный сбор по ОКВЭД')
        logger.info('=' * 40)
        
        try:
            for okved in ['62.01', '62.02', '63.11']:
                okved_companies = list_org_parser.collect_by_okved(okved, max_pages=5)
                
                # Получаем детали для каждой компании
                for company in okved_companies[:50]:
                    if company.get('list_org_url'):
                        details = list_org_parser._parse_company_page(company['list_org_url'])
                        if details:
                            company.update(details)
                            company['source'] = 'list-org'
                
                all_companies.extend(okved_companies)
        except Exception as e:
            logger.error(f'Ошибка при сборе по ОКВЭД: {e}')
    
    # === Этап 4: Обогащение через DaData (если есть ключ) ===
    if config.DADATA_API_KEY:
        logger.info('\n' + '=' * 40)
        logger.info('ЭТАП 4: Обогащение через DaData API')
        logger.info('=' * 40)
        
        try:
            dadata = DaDataClient()
            all_companies = dadata.enrich_companies(all_companies)
        except Exception as e:
            logger.error(f'Ошибка при обогащении через DaData: {e}')
    
    # === Этап 5: Обработка и фильтрация ===
    logger.info('\n' + '=' * 40)
    logger.info('ЭТАП 5: Обработка и фильтрация данных')
    logger.info('=' * 40)
    
    processor = DataProcessor()
    final_companies = processor.process(all_companies)
    
    if not final_companies:
        logger.error('Не удалось собрать данные о компаниях!')
        logger.info('Попробуйте:')
        logger.info('  1. Проверить интернет-соединение')
        logger.info('  2. Увеличить REQUEST_DELAY в .env')
        logger.info('  3. Добавить DADATA_API_KEY для обогащения')
        return 1
    
    # === Этап 6: Сохранение результата ===
    logger.info('\n' + '=' * 40)
    logger.info('ЭТАП 6: Сохранение результата')
    logger.info('=' * 40)
    
    # Порядок колонок
    columns = [
        'inn', 'name', 'employees', 'okved_main', 'source',
        'revenue_year', 'revenue', 'site', 'description', 
        'region', 'contacts'
    ]
    
    df = pd.DataFrame(final_companies)
    
    # Оставляем только существующие колонки
    existing_cols = [c for c in columns if c in df.columns]
    df = df[existing_cols]
    
    # Сохраняем
    df.to_csv(config.OUTPUT_PATH, index=False, encoding='utf-8-sig')
    
    # === Итоги ===
    logger.info('\n' + '=' * 60)
    logger.info('РЕЗУЛЬТАТЫ')
    logger.info('=' * 60)
    logger.info(f'Всего компаний: {len(df)}')
    logger.info(f'Файл сохранён: {config.OUTPUT_PATH}')
    
    if len(df) > 0:
        logger.info(f'\nСтатистика по сотрудникам:')
        logger.info(f'  Минимум: {df["employees"].min()}')
        logger.info(f'  Максимум: {df["employees"].max()}')
        logger.info(f'  Среднее: {df["employees"].mean():.0f}')
        logger.info(f'  Медиана: {df["employees"].median():.0f}')
        
        if 'source' in df.columns:
            logger.info(f'\nИсточники:')
            for source, count in df['source'].value_counts().items():
                logger.info(f'  {source}: {count}')
        
        if 'region' in df.columns:
            logger.info(f'\nТоп-5 регионов:')
            for region, count in df['region'].value_counts().head().items():
                if region:
                    logger.info(f'  {region}: {count}')
        
        # Показываем примеры
        logger.info(f'\nПримеры компаний (топ-5 по штату):')
        for _, row in df.head().iterrows():
            logger.info(f'  {row["name"]}: {row["employees"]} чел.')
    
    logger.info('\n' + '=' * 60)
    logger.info('Готово!')
    logger.info('=' * 60)
    
    return 0


if __name__ == '__main__':
    sys.exit(main())