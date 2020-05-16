# Data-Processing

## installing dependencies
```python
pip3 install virtualenv
virtualenv env
pip3 install -r requirements
```


Chromium driver needed to scrap dynamic pages

## Run this commands to install chromium driver
```bash
apt-get update
```
```bash
apt install chromium-chromedriver
```
```bash
cp /usr/lib/chromium-browser/chromedriver /usr/bin
```
 
## Download database
```python

!pip install gdown
import gdown

url = 'https://drive.google.com/uc?id=1-HCgfFSd4caN4YSjGQbnNdf37defFlEh'
output = 'web_update.db'
gdown.download(url, output, quiet=False)

```
