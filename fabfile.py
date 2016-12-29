"""
Task for calling gcloud.
"""
import json

from fabric.api import task, require, local, env


DEFAULT_CLUSTER_NAME = '2-minute-cluster'


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


@task
def create_cluster(cluster_name):
    """
    Runs gcloud command to create a cluster.

    :param str cluster_name: name of cluster to create
    :return: None
    """
    command = (
        'gcloud dataproc clusters create'
        ' {}'
        ' --num-workers 4'
        ' --master-machine-type n1-standard-1'
        ' --worker-machine-type n1-standard-1'
        ' --num-master-local-ssds 0'
        ' --num-worker-local-ssds 0'
        ' --zone europe-west1-b'.format(cluster_name)
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
def upload_data_files(storage_name):
    """
    Runs gcloud to upload data files from env.data_files_dir
    to cloud storage.

    :param str storage_name: cloud storage name
    :return: None
    """
    require('data_files_dir')
    local('gsutil -m cp -r {} gs://{}/data'.format(
        env.data_files_dir, storage_name)
    )


@task
def upload_source_files(storage_name):
    """
    Runs gcloud to upload source files from env.data_files_dir
    to cloud storage.

    :param str storage_name: cloud storage name
    :return: None
    """
    require('source_files_dir')
    local('gsutil -m cp -r {} gs://{}/src'.format(
        env.source_files_dir, storage_name)
    )


def run_log_analysis(cluster_name):
    """
    Run gcloud to submit task via pysubmit.

    :type str cluster_name: name of cluster to run the task on
    :return:
    """
    raise NotImplementedError


@task
def run_locally():
    local(
        'docker-compose -f docker-compose.yml -f local.yml run'
        ' -T --rm --name spark_standalone spark_standalone'
        ' /spark/bin/spark-submit /src/main.py local'
    )


@task
def create_cluster(cluster_name=DEFAULT_CLUSTER_NAME, workers='8'):
    command_parts = (
        'gcloud dataproc clusters create',
        '{}'.format(cluster_name),
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
def delete_cluster(cluster_name=DEFAULT_CLUSTER_NAME):
    local('gcloud --quiet dataproc clusters delete {}'.format(cluster_name))
