import argparse
from operator import add
import pprint
import re

from pyspark import SparkConf, SparkContext, SparkFiles


DEPLOYMENT_TARGETS = ('local', 'gcloud')
PY_FILES = ('src/main.py',)
DATA_FILE = 'data/access.log'

pp = pprint.PrettyPrinter(indent=2)


# regex to extract:
#   + IP
#   + date
#   + HTTP method
#   + URL
#   + HTTP response code
#   + user agent
overly_simple_log_parser = (
    r'(?P<ip>\d+\.\d+\.\d+\.\d+) - - \[(?P<date>.*?)\] "(?P<method>\w+)'
    r' (?P<url>.*?) HTTP/1\.1" (?P<http_code>\d+) \d+ "(?P<url2>.*?)"'
    r' "(?P<user_agent>.*?)"'
)
regex = re.compile(overly_simple_log_parser)


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
        data_file = 'gs://{}/{}'.format(storage, data_file)

    # Create Spark context
    conf = SparkConf().setAppName('2minuteCluster')
    sc = SparkContext(conf=conf, pyFiles=py_files)

    # Parallelize file content
    log = sc.textFile(
        data_file, use_unicode=True
    ).cache()

    # --- User agent counts ---
    def get_user_agent(line):
        try:
            agent = regex.search(line).group('user_agent')
        except AttributeError:
            agent = 'unknown'
        return agent

    user_agents_count = log.map(
        get_user_agent
    ).countByValue(
    ).items()
    user_agents_count = sorted(user_agents_count, key=lambda x: -x[1])

    print(30*'*' + '\n')
    print('First 10 User agents: \n')
    pp.pprint(user_agents_count[:10])
    print('\n' + 30*'*')

    # --- Unique urls ---
    def get_url(line):
        try:
            url = regex.search(line).group('url')
        except AttributeError:
            url = 'unknown'
        return url, 1

    unique_urls = log.map(
        get_url
    ).countByKey().items()
    unique_urls = sorted(unique_urls, key=lambda x: -x[1])

    print('\n' + 30*'*' + '\n')
    print('First 10 unique URLs: \n')
    pp.pprint(unique_urls[:10])
    print('\n' + 30*'*')

    # --- HTTP code 200 per day, no css and js ---
    def get_url_and_date(line):
        search_result = regex.search(line)
        try:
            url = search_result.group('url')
        except AttributeError:
            url = 'unknown'
        try:
            date = search_result.group('date')
            date = date.split(':')[0]
        except AttributeError:
            date = 'unknown'
        return (date, url), 1

    requests_per_day = log.map(
        get_url_and_date
    ).filter(
        lambda x: '.js' not in x[0][1]
    ).filter(
        lambda x: '.css' not in x[0][1]
    ).reduceByKey(
        add
    ).map(
        lambda x: (x[0][0], (x[0][1], x[1]))
    ).groupByKey(
    ).mapValues(
        list
    ).collect()

    print('\n' + 30*'*' + '\n')
    print('URLs: \n')
    pp.pprint(requests_per_day)
    print('\n' + 30*'*')
