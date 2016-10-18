"""
Task for calling gcloud.
"""
from fabric.api import task, require


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
    raise NotImplementedError


@task
def delete_cluster(cluster_name):
    """
    Runs gcloud command to delete a cluster.

    :param str cluster_name: name of cluster to delete
    :return: None
    """
    raise NotImplementedError


@task
def upload_data_files():
    """
    Runs gcloud to upload data files from env.data_files_dir
    to cloud storage.

    :return: None
    """
    require('data_files_dir')
    raise NotImplementedError


@task
def upload_source_files():
    """
    Runs gcloud to upload source files from env.data_files_dir
    to cloud storage.

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
