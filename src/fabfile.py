"""
Task for calling gcloud.
"""
from fabric.api import task, require, local


@task
def read_config(path):
    """
    Read configuration from JSON file and fill in env variable.

    :param str path: path to configuration file
    :return: None
    """
    raise NotImplementedError


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

    :type str storage_name: cloud storage name
    :return: None
    """
    require('data_files_dir')
    raise NotImplementedError


@task
def upload_source_files(storage_name):
    """
    Runs gcloud to upload source files from env.data_files_dir
    to cloud storage.

    :type str storage_name: cloud storage name
    :return: None
    """
    require('source_files_dir')
    raise NotImplementedError


def run_log_analysis(cluster_name):
    """
    Run gcloud to submit task via pysubmit.

    :type str cluster_name: name of cluster to run the task on
    :return:
    """
    raise NotImplementedError
