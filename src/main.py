import argparse
import pprint
import re

from pyspark import SparkConf, SparkContext, SparkFiles


DEPLOYMENT_TARGETS = ('local', 'gcloud')
PY_FILES = ('src/main.py',)
DATA_FILE = '/data/access.log'

pp = pprint.PrettyPrinter(indent=2)


overly_simple_log_parser = (
    r'(?P<ip>\d+\.\d+\.\d+\.\d+) - - \[(?P<date>.*?)\] "(?P<method>\w+)'
    r' (?P<url>.*?) HTTP/1\.1" (?P<http_code>\d+) \d+ "(?P<url2>.*?)"'
    r' "(?P<user_agent>.*?)"'
)
regex = re.compile(overly_simple_log_parser)


def get_user_agent(line):
    try:
        agent = regex.search(line).group('user_agent')
    except AttributeError:
        agent = 'unknown'
    return agent, 1


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

    # Parallelize file content
    log = sc.textFile(
        data_file, use_unicode=False
    ).cache()

    # --- User agent counts ---
    user_agents_count = log.map(
        get_user_agent
    ).countByKey().items()

    print(30*'*' + '\n')
    print('User agents: \n')
    pp.pprint(sorted(user_agents_count, key=lambda x: x[1]))
    print('\n' + 30*'*')
