import os
from dotenv import load_dotenv

load_dotenv()

# HTTP настройки
REQUEST_DELAY = float(os.getenv('REQUEST_DELAY', '1.0'))
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3

# Фильтры
MIN_EMPLOYEES = int(os.getenv('MIN_EMPLOYEES', '100'))

# API ключи
DADATA_API_KEY = os.getenv('DADATA_API_KEY', '')

# Пути
OUTPUT_PATH = os.getenv('OUTPUT_PATH', 'data/companies.csv')

# ОКВЭД коды для ИТ
IT_OKVED_PREFIXES = [
    '62.0',   # Разработка ПО и консультирование
    '63.1',   # Обработка данных, хостинг, порталы
    '58.2',   # Издание ПО
]

# HH.ru - отрасли ИТ
HH_IT_INDUSTRIES = [
    '7',      # Информационные технологии
]

# Ключевые слова ИТ
IT_KEYWORDS = [
    'разработ', 'software', 'програм', 'it ', 'ит-',
    'digital', 'диджитал', 'tech', 'тех', 'интегратор',
    'saas', 'cloud', 'облач', 'devops', 'data',
    'автоматизац', 'цифров', 'систем', 'веб', 'web',
    'mobile', 'мобил', 'app', 'приложен',
]