# Climate data analysis and prediction project
## Desciption
This project is a course-based team project. In this project, we use a dataset from ECA&D and it contains decades to hundreds years of European climate data.
The detailed dataset description is  from shown from dataset_intro.md, and some more specific description can be found there.

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
