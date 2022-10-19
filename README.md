# Data ETL for ML Training

## Project Description

This project includes data crawling, data processing, and data loading to S3. The data is crawled from a public apartment trading website and then it is cleaned, formatted and the features are extracted from the data. Finally, the data form different cities are saved in parquet format locally and also in S3.

## File Structure

```
 .
 ├── data
 │   ├── raw
 │   ├── clean 
 │   └── fea               
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
## 2. Project Screenshot
![alt text](https://yue-cv-pic.s3.eu-west-1.amazonaws.com/data-etl-pic.jpg)
