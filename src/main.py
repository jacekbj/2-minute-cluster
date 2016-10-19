import argparse

from pyspark import SparkConf, SparkContext, SparkFiles


DEPLOYMENT_TARGETS = ('local', 'gcloud')
PY_FILES = ('src/main.py',)
DATA_FILE = '/data/access.log'


# CLI args parser
parser = argparse.ArgumentParser(description='Target platform selector.')
parser.add_argument('deployment', action='store', choices=DEPLOYMENT_TARGETS)
parser.add_argument('-s --storage', action='store', dest='storage')


if __name__ == '__main__':
    deployment_target = parser.parse_args().deployment
    storage = parser.parse_args().storage

    data_file = DATA_FILE
    py_files = PY_FILES

    # Change paths for cloud storage
    if deployment_target == 'gcloud':
        py_files = ['gs://{}/{}'.format(storage, file_name)
                    for file_name in py_files]
        data_file = 'gs://{}'.format(data_file)

    # Create Spark context
    conf = SparkConf().setAppName('2minuteCluster')
    sc = SparkContext(conf=conf, pyFiles=py_files)

    #
    log = sc.textFile(
        data_file, use_unicode=False
    )
