"""
Task for calling gcloud.
"""
import json

from fabric.api import task, require, local, env


@task
def build_image():
    local('docker build -t spark_standalone:latest .')


@task
def read_config(path):
    """
    Read configuration from JSON file and fill in env variable.

    :param str path: path to configuration file
    :return: None
    """
    with open(path, 'r') as f:
        config = json.loads(f.read())
        env['data_files_dir'] = config['data_files_dir']
        env['source_files_dir'] = config['source_files_dir']
        env['storage_name'] = config['storage_name']
        env['cluster_name'] = config['cluster_name']


@task
def create_cluster(workers_count=4):
    """
    Runs gcloud command to create a cluster.

    :param str cluster_name: name of cluster to create
    :return: None
    """
    require('cluster_name')
    command = (
        'gcloud dataproc clusters create'
        ' {}'
        ' --num-workers {}'
        ' --master-boot-disk-size 10GB'
        ' --worker-boot-disk-size 10GB'
        ' --preemptible-worker-boot-disk-size 10GB'
        ' --master-machine-type n1-standard-1'
        ' --worker-machine-type n1-standard-1'
        ' --num-master-local-ssds 0'
        ' --num-worker-local-ssds 0'
        ' --zone europe-west1-b'.format(
            workers_count, env.cluster_name)
    )
    local(command)


@task
def delete_cluster(cluster_name):
    """
    Runs gcloud command to delete a cluster.

    :param str cluster_name: name of cluster to delete
    :return: None
    """
    local('gcloud --quiet dataproc clusters delete {}'.format(cluster_name))


@task
def upload_data_files():
    """
    Runs gcloud to upload data files from env.data_files_dir
    to cloud storage.

    :param str storage_name: cloud storage name
    :return: None
    """
    require('data_files_dir')
    require('storage_name')
    local(
        'docker run --rm -ti --volumes-from gcloud-config -v {}:/tmp/data google/cloud-sdk'
        ' gsutil -m cp -r /tmp/data gs://{}/data'.format(
            env.data_files_dir, env.storage_name)
    )


@task
def upload_source_files():
    """
    Runs gcloud to upload source files from env.data_files_dir
    to cloud storage.

    :param str storage_name: cloud storage name
    :return: None
    """
    require('source_files_dir')
    require('storage_name')
    local(
        'docker run --rm -ti --volumes-from gcloud-config -v {}:/tmp/src google/cloud-sdk'
        ' gsutil -m cp -r /tmp/src gs://{}'.format(
            env.source_files_dir, env.storage_name)
    )


@task
def run_locally():
    local(
        'docker-compose -f docker-compose.yml -f local.yml run'
        ' -T --rm --name spark_standalone spark_standalone'
        ' /spark/bin/spark-submit /src/main.py local'
    )


@task
def run_remote():
    local(
        'docker run --rm -ti --volumes-from gcloud-config google/cloud-sdk'
        ' gcloud dataproc jobs submit pyspark --cluster {cluster_name}'
        ' gs://{storage_name}/src/main.py'
        ' gcloud -s {storage_name}'.format(
            cluster_name=env.cluster_name,
            storage_name=env.storage_name)
    )


@task
def create_cluster(workers='4'):
    command_parts = (
        'docker run --rm -ti --volumes-from gcloud-config google/cloud-sdk'
        ' gcloud dataproc clusters create',
        ' {}'.format(env.cluster_name),
        '--num-preemptible-workers 0',
        '--num-workers {}'.format(workers),
        '--master-machine-type n1-highmem-2',
        '--worker-machine-type n1-standard-1',
        '--num-master-local-ssds 0',
        '--num-worker-local-ssds 0',
        '--zone europe-west1-b'
    )
    command = ' '.join(command_parts)
    local(command)


@task
def delete_cluster():
    local(
        'docker run --rm -ti --volumes-from gcloud-config google/cloud-sdk'
        ' gcloud --quiet dataproc clusters delete {}'.format(env.cluster_name)
    )
