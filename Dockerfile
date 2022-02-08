FROM apache/airflow:2.0.1


COPY requirements.txt .

RUN pip install -r requirements.txt


RUN pip install requests
RUN pip uninstall  --yes azure-storage && pip install -U azure-storage-blob apache-airflow-providers-microsoft-azure==1.1.0

