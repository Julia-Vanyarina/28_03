#!/bin/bash

# Проверяем существование исходной папки с изображениями
if [ ! -d "./data/images" ]; then
  echo "Исходная папка ./data/images не существует!"
  exit 1
fi

# Проверяем существование файла со статистикой
if [ ! -f "./data/connection_stats.csv" ]; then
  echo "Файл статистики ./data/connection_stats.csv не найден!"
  # Не выходим с ошибкой, так как это может быть первый запуск
fi

# Создаем целевую папку, если ее нет
mkdir -p /home/mgpu/st_105

# Копируем изображения (с проверкой успешности)
echo "Копирование изображений..."
if cp -r ./data/images/* /home/mgpu/st_105/; then
  echo "Файлы изображений успешно скопированы из ./data/images в /home/mgpu/st_105"
else
  echo "Ошибка при копировании файлов изображений!"
  exit 1
fi

# Копируем файл статистики (если существует)
if [ -f "./data/connection_stats.csv" ]; then
  echo "Копирование файла статистики..."
  if cp ./data/connection_stats.csv /home/mgpu/st_105/; then
    echo "Файл статистики connection_stats.csv успешно скопирован"
    
    # Добавляем запись в лог статистики
    echo "$(date) - Скопирован файл статистики" >> /home/mgpu/st_105/transfer_log.txt
  else
    echo "Ошибка при копировании файла статистики!"
    exit 1
  fi
fi

# Проверяем количество перенесенных файлов
image_count=$(ls -1 /home/mgpu/st_105 | grep -v connection_stats.csv | grep -v transfer_log.txt | wc -l)
echo "Перенесено $image_count файлов изображений"

# Проверяем наличие файла статистики в целевой папке
if [ -f "/home/mgpu/st_105/connection_stats.csv" ]; then
  stats_lines=$(wc -l < /home/mgpu/st_105/connection_stats.csv)
  echo "Файл статистики содержит $((stats_lines-1)) записей (первая строка - заголовок)"
fi