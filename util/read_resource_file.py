import yaml

with open('../resource/config.yaml', 'r') as file:
    config = yaml.safe_load(file)

MYSQL_HOST = str(config['mysql']['host'])
MYSQL_DRIVER = str(config['mysql']['driver'])
MYSQL_USER = str(config['mysql']['username'])
MYSQL_USER_PASSWORD = str(config['mysql']['password'])
MYSQL_DATABASE = str(config['mysql']['database'])
MYSQL_URL = str(config['mysql']['url'])

REDIS_HOST = str(config['redis']['host'])
REDIS_PORT = str(config['redis']['port'])
REDIS_USER = str(config['redis']['username'])
REDIS_USER_PASSWORD = str(config['redis']['password'])

PROXY_POOL_IP = str(config['proxy_pool']['ip'])
PROXY_POOL_PORT = str(config['proxy_pool']['port'])
