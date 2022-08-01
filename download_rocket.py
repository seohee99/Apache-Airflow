import json
import pathlib
 
import airflow.utils.dates
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
 
 
dag = DAG( #객체의 인스턴스 생성(구체화) - 모든 워크플로의 시작점 , DAG class는 두 개의 인수를 필요로 함
 
    dag_id="download_rocket_launches",   # DAG이름(Airflow UI에 표시)
    description="Download rocket pictures of recently launched rockets.",
    start_date=airflow.utils.dates.days_ago(14),   # DAG 처음 실행 시작 날짜/시간
    schedule_interval="@daily",   # DAG 실행 간격, None인 경우 DAG가 자동으로 실행되지 않음을 의미
 
)
 
download_launches = BashOperator(   # BashOperator를 이용해 curl로 URL 결괏값 다운로드
    task_id="download_launches",   #task 이름
    bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",  # noqa: E501, 실행할 bash command
    dag=dag,   # DAG변수에 대한 참조
)
 
 
def _get_pictures(): #python 함수는 결괏값을 parsing하고 모든 로켓 사진을 다운로드
 
    # 경로 존재하는 지 확인, 경로 없으면 directory 생성
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)
 
    # launches.json에 있는 모든 그림 파일 다운로드
    with open("/tmp/launches.json") as f:  # 이전 단계의 task 결과 확인, 로켓 발사 JSON 파일 열기
        launches = json.load(f)  # 데이터를 섞을 수 있도록 dictionary로 읽기
        image_urls = [launch["image"] for launch in launches["results"]]  # 모든 발사에 대한 'image'의 URL값 읽기
 
        for image_url in image_urls:  # 모든 이미지 URL을 얻기 위한 루프
            try:
                response = requests.get(image_url)   # 각각의 이미지 다운로드
                image_filename = image_url.split("/")[-1]  # 마지막 파일 이름만 가져옴
                target_file = f"/tmp/images/{image_filename}"  # 타겟 파일 저장 경로 구성
                with open(target_file, "wb") as f:  # 타켓 파일 핸들 열기
                    f.write(response.content)   # 각각의 이미지 저장, 파일 경로에 이미지 쓰기
                print(f"Downloaded {image_url} to {target_file}")  # Airflow 로그에 저장하기 위해 stdout으로 결과 출력
            except requests_exceptions.MissingSchema:  # 잠재적 에러 포착 및 처리
                print(f"{image_url} appears to be an invalid URL.")
            except requests_exceptions.ConnectionError:
                print(f"Could not connect to {image_url}.")
 
 
get_pictures = PythonOperator(  # python 함수 호출을 위해 PythonOperator 구체화
    task_id="get_pictures",
    python_callable=_get_pictures, #DAG에서 PythonOperator 사용해 python 함수 호출, 실행할 python 함수 지정
    dag=dag
)
 
 
notify = BashOperator(
    task_id="notify",
    bash_command='echo "There are now $(ls /tmp/images/ | wc -l) images."',
    dag=dag,
)
 
 
download_launches >> get_pictures >> notify # task 실행 순서 설정(의존성 설정)