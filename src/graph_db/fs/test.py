import json

with open('../../../configs/config.json') as f:
    d = json.load(f)
    print(d)
    print(d['manager_config']['workers'][0])
    for worker in d['manager_config']['workers']:
        print(worker['stores']['NodeStorage'])