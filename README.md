# 2-minute-cluster
Demonstration of Google Dataproc use

# Requirements

+ Docker
+ Docker Compose
+ Fabric

## Gcloud
Configure credentials and project:
```
docker pull google/cloud-sdk
docker run -t -i --name gcloud-config google/cloud-sdk gcloud init
```

Navigate to https://console.developers.google.com/apis/api/dataproc.googleapis.com/overview and enable API access to Dataproc.

Get configuration info:
```docker run --rm -ti --volumes-from gcloud-config google/cloud-sdk gcloud info```

Get list of Fabric tasks:
```fab -l```

# Run

## Locally:
```fab run_locally```

## On a cloud:
Create a file `config.json` from config.template.json.

Create cluster
```
fab read_config:config.json create_cluster
# or with number of worker nodes (default=4)
# fab read_config:config.json create_cluster:15
```

Upload data and source files.
For this purpose create a cloud storage under your project with the name equal to the one in config.json.
Running these commands will create two directories in the bucket: /data and /src and upload files from corresponding lcoal directories.
```
fab read_config:config.json upload_data_files
fab read_config:config.json upload_source_files
```

And finally:
```
fab read_config:config.json run_remote
```
