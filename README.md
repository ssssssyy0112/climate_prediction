# Climate data analysis and prediction project
## Introduction
This project is a course-based team project. In this project, we use a dataset from ECA&D and it contains decades to hundreds of years of European climate data.
The detailed dataset description is in from dataset_intro.md, and some more specific description can be found in the ECA&D website.

## Project Structure
Below is generated by linux tool `tree`.

`dataset_intro.md` is a detailed description of our dataset.

`elastic` is the docker-compose yaml to deploy a 3-node elasticsearch clusters with 1-node kibana server.

`export.ndjson` is an exported mapping file, and you can import it in your kibana with previous data process steps to see those beautiful maps.

`pictures` includes pictures drawn by matplotlib.pyplot and screenshots from kibana mappings. The distribution features of the data are shown there.
Also, it contains several gif to demonstrate the time trend with a year or across several years.

`seaweedfs` is the docker-compose yaml to deploy a distributed file system `SeaweedFS`, master node, volume node and filer included.

`spark` directory contains not only the docker-compose yaml with its Dockerfile to construct a 1-master-4-worker spark cluster, it also contains our core logic codes in this project.
Those Python files are about data analysis and machine learning(prediction) using `PySpark`, `pandas` and other tools. Each Python file has a high-level description of its purpose in the beginning lines.

`sync_gif.py` is a helper Python file using `imageIO` to bring pngs into a gif. Our gif pictures are produced this way.

```
.
├── dataset_intro.md
├── elastic
│   └── docker-compose.yml
├── export.ndjson
├── pictures
│   ├── avg_temp
│   ├── cloud_19951001.png
│   ├── gif
│   ├── high_temp
│   ├── humidity
│   ├── humidity_19951001.png
│   ├── low_temp
│   ├── matplotlib_pic
│   ├── mix_19950401.png
│   ├── pressure
│   ├── rain
│   ├── sunshine
│   ├── wind
│   └── wind_speed
├── seaweedfs
│   └── docker-compose.yml
├── spark
│   ├── Dockerfile
│   ├── KMeans.py
│   ├── add_level.py
│   ├── aggregate_analysis.py
│   ├── country.py
│   ├── docker-compose.yml
│   ├── draw_analysis.py
│   ├── intermediate_data
│   ├── location.py
│   ├── migration.py
│   ├── pictures
│   ├── station_positions
│   ├── statistics.py
│   ├── training.py
│   └── write_data.py
└── sync_gif.py
```

## How to use?
Considering that our dataset is too large, the total size has exceeded the upper limit supported by Github GitLFS (1GB), 
we have placed the original dataset and Elasticsearch image on PKU disk. You can directly download the data file zip package on the network disk, 
run the pre-process codes in it, and then use above data processing and analysis code in `spark` directory of this project for later steps.

### Steps
1. Download data from ECA&D website. Unzip them, removing the not needed first 20 lines in each txt. (We've provided code to do this inside the RawData files in the outer links, but you can do it by yourself with a more convenient way.)
2. Deploy the SeaweedFS and EXPOSE port 8888, save those text files into SeaweedFS.
3. Deploy the Spark cluster.
4. Deploy the ElasticSearch cluster and EXPOSE port 9200. (If you also deployed kibana, EXPOSE port 5601)
5. Use code `spark/write_data.py` to fetch files from SeaweedFS, use spark to process them and write them into ElasticSearch.
6. Use code `country.py` and `location.py` to add geo_point fileds into ElasticSearch. Use other Python files to do statistics, train and visualize data.

Note: The urls of Filesystem, ElasticSearch might vary depending on your machine and settings. You need to modify them by yourself.


### Outer links
RawData files  https://disk.pku.edu.cn:443/link/880E890CCF0D51599FFD307D503D4493    Validation: 2024-02-29 23:59

Next are a few zip files, which contain ready-to-use big-data storage components. You only need to unzip it and run the binary in the `bin` directory to get the data we have already processed and stored. They are ready to use!

Those two can run in my computer with `Linux {username} 5.15.133.1-microsoft-standard-WSL2 #1 SMP Thu Oct 5 21:02:42 UTC 2023 x86_64 x86_64 x86_64 GNU/Linux`

ElasticSearch  https://disk.pku.edu.cn:443/link/318E4F28C506B9B6992C364DBF013768    Validation：2024-02-29 23:59

Kibana  https://disk.pku.edu.cn:443/link/2068BE598A9D627FFF295448200640BC     Validation：2024-02-29 23:59

After downloading ES and Kibana, unzip them, you will have two folders: `elasticsearch-8.11.1` and `kibana-8.11.1`.
Then you can use `./elasticsearch-8.11.1/bin/elasticsearch` and `./kibana-8.11.1` to run them, and jump to the Step 6 above to explore the data directly!

Our presentation pptx with personal information erased: https://disk.pku.edu.cn:443/link/BD67CA76FD40708F2F0432BF852487E3

Docker Hub： https://hub.docker.com/repository/docker/ssssssyy189/climate_prediction/general    You can download images from this public hub which we used in this project.

```Shell
docker pull ssssssyy189/climate_prediction:kibana
docker pull ssssssyy189/climate_prediction:elasticsearch
docker pull ssssssyy189/climate_prediction:seaweedfs
docker pull ssssssyy189/climate_prediction:spark-spark-master
```

## Deploy Code
Below are some deploy code we used. You can copy it and run directly on your machine.

### SeaweedFS
```Shell
cd ~
# Download seaweedfs and start
curl -O https://github.com/seaweedfs/seaweedfs/releases/download/3.59/linux_amd64.tar.gz
tar -xvzf linux_amd64.tar.gz
rm -rf linux_amd64.tar.gz
mkdir weed_store
mkdir weed_store2
mkdir weed_store3
mkdir weed_logs
nohup ./weed master > weed_logs/master.log &
nohup ./weed volume -dir="~/weed_store" -max=5 -mserver="localhost:9333" -port=10001 > weed_logs/volume1.log &
nohup ./weed volume -dir="~/weed_store2" -max=5 -mserver="localhost:9333" -port=10002 > weed_logs/volume2.log &
nohup ./weed volume -dir="~/weed_store3" -max=5 -mserver="localhost:9333" -port=10003 > weed_logs/volume3.log &
nohup ./weed filer > weed_logs/filer.log &

# make source data directories
mkdir raw_data && cd raw_data
touch rename.py

# rename.py filter not used lines in the beginning
echo "import os
import sys
folder_list=['cc','dd','fg','fx','hu','pp','qq','rr','sd','ss','tg','tn','tx']
limit_list=[20,20,20,20,20,20,20,20,22,20,20,20,20]
def f(folder_path,limit):
    for filename in os.listdir(folder_path):
        if filename.endswith('.txt'):
            file_path = os.path.join(folder_path, filename)
            with open(file_path, 'r') as file:
                lines = file.readlines()
            lines = lines[limit+1:]
            with open(file_path, 'w') as file:
                file.writelines(lines)
if len(sys.argv)!=2:
    print('invalid dir name')
else:
    i = folder_list.index(sys.argv[1])
    f(folder_list[i],limit_list[i])" > rename.py
    
# Sequentially fetch, unzip and store into SeaweedFS. 
curl -O https://knmi-ecad-assets-prd.s3.amazonaws.com/download/ECA_blend_tx.zip
unzip ECA_blend_tx.zip -d ./tx
rm -rf ECA_blend_tx.zip
python3 rename.py tx
cd tx
find . -type f ! -name 'TX*' -exec rm -f {} \;
echo '#!/bin/bash
for FILENAME in ./*; do
  curl -F file=@./${FILENAME} http://localhost:8888/${FILENAME}
done' > writedata.sh
bash ./writedata.sh
cd ..
rm -rf tx

curl -O https://knmi-ecad-assets-prd.s3.amazonaws.com/download/ECA_blend_tn.zip
unzip ECA_blend_tn.zip -d ./tn
rm -rf ECA_blend_tn.zip
python3 rename.py tn
cd tn
find . -type f ! -name 'TN*' -exec rm -f {} \;
echo '#!/bin/bash
for FILENAME in ./*; do
  curl -F file=@./${FILENAME} http://localhost:8888/${FILENAME}
done' > writedata.sh
bash ./writedata.sh
cd ..
rm -rf tn

curl -O https://knmi-ecad-assets-prd.s3.amazonaws.com/download/ECA_blend_tg.zip
unzip ECA_blend_tg.zip -d ./tg
rm -rf ECA_blend_tg.zip
python3 rename.py tg
cd tg
find . -type f ! -name 'TG*' -exec rm -f {} \;
echo '#!/bin/bash
for FILENAME in ./*; do
  curl -F file=@./${FILENAME} http://localhost:8888/${FILENAME}
done' > writedata.sh
bash ./writedata.sh
cd ..
rm -rf tg

curl -O https://knmi-ecad-assets-prd.s3.amazonaws.com/download/ECA_blend_cc.zip
unzip ECA_blend_cc.zip -d ./cc
rm -rf ECA_blend_cc.zip
python3 rename.py cc
cd cc
find . -type f ! -name 'CC*' -exec rm -f {} \;
echo '#!/bin/bash
for FILENAME in ./*; do
  curl -F file=@./${FILENAME} http://localhost:8888/${FILENAME}
done' > writedata.sh
bash ./writedata.sh
cd ..
rm -rf cc

curl -O https://knmi-ecad-assets-prd.s3.amazonaws.com/download/ECA_blend_dd.zip
unzip ECA_blend_dd.zip -d ./dd
rm -rf ECA_blend_dd.zip
python3 rename.py dd
cd dd
find . -type f ! -name 'DD*' -exec rm -f {} \;
echo '#!/bin/bash
for FILENAME in ./*; do
  curl -F file=@./${FILENAME} http://localhost:8888/${FILENAME}
done' > writedata.sh
bash ./writedata.sh
cd ..
rm -rf dd

curl -O https://knmi-ecad-assets-prd.s3.amazonaws.com/download/ECA_blend_fg.zip
unzip ECA_blend_fg.zip -d ./fg
rm -rf ECA_blend_fg.zip
python3 rename.py fg
cd fg
find . -type f ! -name 'FG*' -exec rm -f {} \;
echo '#!/bin/bash
for FILENAME in ./*; do
  curl -F file=@./${FILENAME} http://localhost:8888/${FILENAME}
done' > writedata.sh
bash ./writedata.sh
cd ..
rm -rf fg

curl -O https://knmi-ecad-assets-prd.s3.amazonaws.com/download/ECA_blend_fx.zip
unzip ECA_blend_fx.zip -d ./fx
rm -rf ECA_blend_fx.zip
python3 rename.py fx
cd fx
find . -type f ! -name 'FX*' -exec rm -f {} \;
echo '#!/bin/bash
for FILENAME in ./*; do
  curl -F file=@./${FILENAME} http://localhost:8888/${FILENAME}
done' > writedata.sh
bash ./writedata.sh
cd ..
rm -rf fx

curl -O https://knmi-ecad-assets-prd.s3.amazonaws.com/download/ECA_blend_hu.zip
unzip ECA_blend_hu.zip -d ./hu
rm -rf ECA_blend_hu.zip
python3 rename.py hu
cd hu
find . -type f ! -name 'HU*' -exec rm -f {} \;
echo '#!/bin/bash
for FILENAME in ./*; do
  curl -F file=@./${FILENAME} http://localhost:8888/${FILENAME}
done' > writedata.sh
bash ./writedata.sh
cd ..
rm -rf hu

curl -O https://knmi-ecad-assets-prd.s3.amazonaws.com/download/ECA_blend_pp.zip
unzip ECA_blend_pp.zip -d ./pp
rm -rf ECA_blend_pp.zip
python3 rename.py pp
cd pp
find . -type f ! -name 'PP*' -exec rm -f {} \;
echo '#!/bin/bash
for FILENAME in ./*; do
  curl -F file=@./${FILENAME} http://localhost:8888/${FILENAME}
done' > writedata.sh
bash ./writedata.sh
cd ..
rm -rf pp

curl -O https://knmi-ecad-assets-prd.s3.amazonaws.com/download/ECA_blend_qq.zip
unzip ECA_blend_qq.zip -d ./qq
rm -rf ECA_blend_qq.zip
python3 rename.py qq
cd qq
find . -type f ! -name 'QQ*' -exec rm -f {} \;
echo '#!/bin/bash
for FILENAME in ./*; do
  curl -F file=@./${FILENAME} http://localhost:8888/${FILENAME}
done' > writedata.sh
bash ./writedata.sh
cd ..
rm -rf qq

curl -O https://knmi-ecad-assets-prd.s3.amazonaws.com/download/ECA_blend_rr.zip
unzip ECA_blend_rr.zip -d ./rr
rm -rf ECA_blend_rr.zip
python3 rename.py rr
cd rr
find . -type f ! -name 'RR*' -exec rm -f {} \;
echo '#!/bin/bash
for FILENAME in ./*; do
  curl -F file=@./${FILENAME} http://localhost:8888/${FILENAME}
done' > writedata.sh
bash ./writedata.sh
cd ..
rm -rf rr

curl -O https://knmi-ecad-assets-prd.s3.amazonaws.com/download/ECA_blend_sd.zip
unzip ECA_blend_sd.zip -d ./sd
rm -rf ECA_blend_sd.zip
python3 rename.py sd
cd sd
find . -type f ! -name 'SD*' -exec rm -f {} \;
echo '#!/bin/bash
for FILENAME in ./*; do
  curl -F file=@./${FILENAME} http://localhost:8888/${FILENAME}
done' > writedata.sh
bash ./writedata.sh
cd ..
rm -rf sd

curl -O https://knmi-ecad-assets-prd.s3.amazonaws.com/download/ECA_blend_ss.zip
unzip ECA_blend_ss.zip -d ./ss
rm -rf ECA_blend_ss.zip
python3 rename.py ss
cd ss
find . -type f ! -name 'SS*' -exec rm -f {} \;
echo '#!/bin/bash
for FILENAME in ./*; do
  curl -F file=@./${FILENAME} http://localhost:8888/${FILENAME}
done' > writedata.sh
bash ./writedata.sh
cd ..
rm -rf ss
```


### Spark
After using `docker-compose up` to run the spark, use code below to submit job `write_data.py` to spark master.
```Shell
docker exec -it spark-spark-master-1 /bin/bash -c "/opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 /app/write_data.py"
```


### ElasticSearch
If you only need a single-node elasticsearch locally, try to use the command below.
```Shell
cd ~
wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-8.11.1-linux-x86_64.tar.gz
tar -xvzf elasticsearch-8.11.1-linux-x86_64.tar.gz
rm -rf elasticsearch-8.11.1-linux-x86_64.tar.gz
./elasticsearch-8.11.1/bin/elasticsearch
```
The same applies to kibana, just replace elasticsearch with kibana.



## Others
Before trying to run our projects, you'll need to have a basic knowledge about how to run apps with `docker-compose`.

Also, `kibana` has many features, and you may need to search its website for more usages.

