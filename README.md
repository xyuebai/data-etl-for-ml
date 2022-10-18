# Data ETL for ML Training

## Project Description

The ETL job include data crawling from online, data cleaning, and feature engineering. The data is saved in parquet format locally and also upload to AWS S3

## File Structure

```
 .
 ├── data
 │   ├── raw
 │   ├── clean 
 │   └──fea               
 ├── config.ini             # setting file
 ├── Dockerfile
 ├── prepare_data.py        # main function
 ├── run.sh 
 ├── requirements.txt 
 └── README.md


```
## 0. Prerequest

1. AWS account
2. Docker 

## 1. How to Run
```
$ docker build -t <Container_Name> .
$ docker run -it <Container_Name> /bin/bash
$ docker run \
    -e AWS_ACCESS_KEY_ID=<access_key> \
    -e AWS_SECRET_ACCESS_KEY=<access_secret> \
    -e mfa_serial=<mfa_serial> \
    -e region=eu-west-1 \
    -it data_clean\ 
    /bin/bash 
```