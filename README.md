# Парсер российских ИТ-компаний

Автоматический сбор базы российских ИТ-компаний со штатом от 100 сотрудников.

## Источники данных

1. **HH.ru API** (бесплатный) — список работодателей в ИТ-сфере
2. **List-org.com** (парсинг) — данные ЕГРЮЛ: ИНН, ОКВЭД, численность
3. **DaData API** (опционально) — обогащение и валидация данных

## Быстрый старт

```bash
# Клонирование
git clone <repo-url>
cd it-companies-parser

# Виртуальное окружение
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

# Зависимости
pip install -r requirements.txt

# Конфигурация
cp .env.example .env
# Отредактируйте .env при необходимости

# Запуск
python main.py