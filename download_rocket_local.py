import csv
import json
import pathlib
from datetime import datetime, timedelta
import time
from statistics import mean
 
import airflow.utils.dates
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
 
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}
 
dag = DAG(
    dag_id="rocket_images_weekly_download_v2",
    description="Еженедельное скачивание изображений ракет с анализом подключения и обработкой ошибок",
    schedule_interval="@weekly",
    default_args=default_args,
    catchup=False,
    tags=['rocket', 'images', 'download', 'monitoring'],
)
 
def save_connection_stats_to_csv(stats_data):
    csv_file = "/opt/airflow/data/connection_stats.csv"
    file_exists = pathlib.Path(csv_file).exists()
    
    fieldnames = [
        'timestamp',
        'attempts',
        'successful_attempts',
        'avg_response_time',
        'max_response_time',
        'min_response_time',
        'status'
    ]
    
    with open(csv_file, mode='a' if file_exists else 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        if not file_exists:
            writer.writeheader()
        
        writer.writerow(stats_data)
 
# 1. Задача для скачивания JSON с данными о запусках
download_launches = BashOperator(
    task_id="download_launches",
    bash_command="curl -o /opt/airflow/data/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    dag=dag,
)
 
# 2. Улучшенная задача для анализа подключения к серверу с записью в CSV
def analyze_and_log_connection():
    test_url = "https://ll.thespacedevs.com/2.0.0/launch/upcoming"
    attempts = 3
    timeouts = []
    successful_attempts = 0
    stats = {
        'timestamp': datetime.now().isoformat(),
        'attempts': attempts,
        'successful_attempts': 0,
        'avg_response_time': 0,
        'max_response_time': 0,
        'min_response_time': 0,
        'status': 'FAILED'
    }
    
    for i in range(attempts):
        try:
            start_time = time.time()
            response = requests.get(
                test_url,
                timeout=(5, 10),
                headers={'User-Agent': 'Airflow DAG tester/1.0'}
            )
            response_time = time.time() - start_time
            timeouts.append(response_time)
            
            if response.status_code == 200:
                successful_attempts += 1
                print(f"Попытка {i+1}: Успешно. Время ответа: {response_time:.2f}с")
            else:
                print(f"Попытка {i+1}: Ошибка. Код статуса: {response.status_code}")
                
        except requests.exceptions.RequestException as e:
            print(f"Попытка {i+1}: Ошибка подключения - {str(e)}")
            timeouts.append(10)  # Максимальный таймаут
    
    if successful_attempts > 0:
        stats.update({
            'successful_attempts': successful_attempts,
            'avg_response_time': round(mean(timeouts), 2),
            'max_response_time': round(max(timeouts), 2),
            'min_response_time': round(min(timeouts), 2),
            'status': 'SUCCESS' if successful_attempts == attempts else 'PARTIAL'
        })
        
        print(f"Статистика подключения: {stats}")
        
        # Сохраняем статистику в CSV
        save_connection_stats_to_csv(stats)
        
        avg_time = stats['avg_response_time']
        if avg_time > 5:
            print(f"Предупреждение: Высокое среднее время ответа ({avg_time:.2f}с)")
        else:
            print(f"Подключение стабильное. Среднее время ответа: {avg_time:.2f}с")
    else:
        save_connection_stats_to_csv(stats)
        raise AirflowSkipException("Все попытки подключения провалились, пропускаем загрузку изображений")
 
connection_analysis = PythonOperator(
    task_id="connection_analysis",
    python_callable=analyze_and_log_connection,
    dag=dag,
)
 
# 3. Задача для загрузки изображений с обработкой ошибок (без изменений)
def download_images_with_error_handling():
    images_dir = "/opt/airflow/data/images"
    pathlib.Path(images_dir).mkdir(parents=True, exist_ok=True)
    
    try:
        with open("/opt/airflow/data/launches.json") as f:
            launches = json.load(f)
            
            if not isinstance(launches.get("results"), list):
                raise AirflowSkipException("Некорректный формат JSON: отсутствует список results")
            
            if not launches["results"]:
                raise AirflowSkipException("Нет данных о запусках в JSON")
            
            image_urls = []
            for launch in launches["results"]:
                if isinstance(launch, dict) and launch.get("image"):
                    image_urls.append(launch["image"])
            
            if not image_urls:
                raise AirflowSkipException("В данных о запусках отсутствуют URL изображений")
            
            success_count = 0
            for idx, url in enumerate(image_urls, 1):
                try:
                    if not url.startswith(('http://', 'https://')):
                        print(f"Некорректный URL: {url}")
                        continue
                    
                    response = requests.get(
                        url,
                        timeout=(10, 30),
                        stream=True,
                        headers={'User-Agent': 'Airflow Rocket Image Downloader'}
                    )
                    response.raise_for_status()
                    
                    content_type = response.headers.get('content-type', '')
                    if not content_type.startswith('image/'):
                        print(f"URL {url} возвращает не изображение: {content_type}")
                        continue
                    
                    filename = f"{images_dir}/{url.split('/')[-1].split('?')[0]}"
                    if not filename.lower().endswith(('.jpg', '.jpeg', '.png')):
                        filename += '.jpg'
                    
                    with open(filename, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                    
                    success_count += 1
                    print(f"Успешно загружено изображение {idx}/{len(image_urls)}: {filename}")
                
                except requests_exceptions.SSLError:
                    print(f"Ошибка SSL для {url}")
                except requests_exceptions.ConnectionError:
                    print(f"Ошибка подключения для {url}")
                except requests_exceptions.Timeout:
                    print(f"Таймаут при загрузке {url}")
                except requests_exceptions.HTTPError as e:
                    print(f"HTTP ошибка {e.response.status_code} для {url}")
                except Exception as e:
                    print(f"Неожиданная ошибка при загрузке {url}: {str(e)}")
            
            print(f"Итого загружено: {success_count}/{len(image_urls)} изображений")
            
            if success_count == 0:
                raise AirflowSkipException("Не удалось загрузить ни одного изображения")
 
    except json.JSONDecodeError:
        raise Exception("Ошибка: Некорректный JSON в файле launches.json")
    except Exception as e:
        raise Exception(f"Критическая ошибка: {str(e)}")
 
download_images = PythonOperator(
    task_id="download_images",
    python_callable=download_images_with_error_handling,
    dag=dag,
)
 
# 4. Задача для уведомления о результате
notify = BashOperator(
    task_id="notify",
    bash_command='echo "Еженедельное обновление: загружено $(ls /opt/airflow/data/images/ | wc -l) новых изображений ракет."',
    dag=dag,
)
 
# Определяем порядок выполнения задач
download_launches >> connection_analysis >> download_images >> notify