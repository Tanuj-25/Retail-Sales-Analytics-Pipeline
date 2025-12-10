# Retail-Sales-Analytics-Pipeline
This project implements an end-to-end ETL pipeline that ingests ~15 GB/day of retail transactional data from AWS S3, validates schema and data quality, and applies business-driven transformations using Apache Spark/PySpark. It automates the generation of daily and monthly analytical datasets used for sales insights, incentive calculations, and customer spend reporting. The final curated outputs are stored in a data mart to support downstream analytics and BI dashboards.

The structure for the project is as follows -
```plaintext
Project structure:-
DE_project1/
├── docs/
│   └── readme.md
├── resources/
│   ├── __init__.py
│   ├── dev/
│   │    ├── config.py
│   │    └── requirement.txt
│   └── qa/
│   │    ├── config.py
│   │    └── requirement.txt
│   └── prod/
│   │    ├── config.py
│   │    └── requirement.txt
│   ├── sql_scripts/
│   │    └── table_scripts.sql
├── src/
│   ├── main/
│   │    ├── __init__.py
│   │    └── delete/
│   │    │      ├── aws_delete.py
│   │    │      ├── database_delete.py
│   │    │      └── local_file_delete.py
│   │    └── download/
│   │    │      └── aws_file_download.py
│   │    └── move/
│   │    │      └── move_files.py
│   │    └── read/
│   │    │      ├── aws_read.py
│   │    │      └── database_read.py
│   │    └── transformations/
│   │    │      └── jobs/
│   │    │      │     ├── customer_mart_sql_transform_write.py
│   │    │      │     ├── dimension_tables_join.py
│   │    │      │     ├── main.py
│   │    │      │     └──sales_mart_sql_transform_write.py
│   │    └── upload/
│   │    │      └── upload_to_s3.py
│   │    └── utility/
│   │    │      ├── encrypt_decrypt.py
│   │    │      ├── logging_config.py
│   │    │      ├── s3_client_object.py
│   │    │      ├── spark_session.py
│   │    │      └── my_sql_session.py
│   │    └── write/
│   │    │      ├── database_write.py
│   │    │      └── parquet_write.py
│   ├── test/
│   │    ├── scratch_pad.py.py
│   │    └── generate_csv_data.py
```

What all Applications/Technologies is required - 
1. PyCharm IDE
2. Spark/PySpark
3. Python (lower than 3.11 as PySpark does not support higher versions)
4. MySQL

How to run the program in Pycharm:-
1. Open the pycharm editor.
2. Upload or pull the project from GitHub.
3. Open terminal from bottom pane.
4. Go to virtual environment and activate it. Let's say you have venv as virtual environament.i) cd venv ii) cd Scripts iii) activate (if activate doesn't work then use ./activate)
5. Download the requirements.txt file using - 'pip install -r requirements.txt' to get the required libraries. 
6. You will have to create a user on AWS also and assign s3 full access and provide secret key and access key to the config file. 
7. Should encrypt the important config details using encrypt_decrypt.py module in src.main.utility.encrypt_decrypt. These can we imported into the modules whenever required and used after decryption.
8. Run main.py from green play button on top right hand side after setting the environment variables and selecting the "main.py" script that should run on starting.
9. It should work as expected. 
