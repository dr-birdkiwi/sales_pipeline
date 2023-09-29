# sales_pipeline

## Setup

### Prerequisites
 - Install git from https://github.com/git-guides/install-git
 - Install Docker from https://docs.docker.com/engine/install/ for your platform
 - [dev-only] Install Python from https://www.python.org/downloads/
 - [dev-only] `pip install -r requirements.txt`

### Steps
 - Launch Docker
 - Open a terminal, go to your prefered directory, and run `git clone https://github.com/dr-birdkiwi/sales_pipeline.git`
 - Run `docker-compose up`

## Usage
### Navigate Airflow UI
 - Visit the Airflow UI via `http://localhost:8080/`
    - username: `airflow`
    - password: `airflow`
 - Enable the DAG named `sales_pipleine`
![Alt text](pics/airflow_1.png)
 - Enter the DAG named `sales_pipeline` and trigger it by clicking the triangle on the right hand side
![Alt text](pics/airflow_2.png)

### Navigate PostgresDB
 - Use your preferred tool to connect to the PostgresDB
    - host: localhost
    - port: 5432
    - db_name: postgres
    - username: airflow
    - password: airflow
 - All the tables are located under the `public` schema
 ![Alt text](pics/posgres_1.png)

