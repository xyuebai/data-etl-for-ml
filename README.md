## Docker Run
```
$ docker build -t data_clean .
$ docker run -it data_clean /bin/bash
$ docker run \
    -e AWS_ACCESS_KEY_ID=<access_key> \
    -e AWS_SECRET_ACCESS_KEY=<access_secret> \
    -e mfa_serial=<mfa_serial> \
    -e region=eu-west-1 \
    -it data_clean\ 
    /bin/bash 
```