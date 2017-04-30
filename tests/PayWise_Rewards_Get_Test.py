import scripts.PayWise_Rewards_Get as Subject

test_data = [
    {
        'description': 'Happy Path With Store Name',
        'input': {
            'user_id': '10001',
            'name': 'amazon'
        },
        'output': {
            'status': 200
        }
    },
    {
        'description': 'Happy Path With Domain',
        'input': {
            'user_id': '10001',
            'domain': 'amazon.com'
        },
        'output': {
            'status': 200
        }
    },
    {
        'description': 'Happy Path With Domain and Category',
        'input': {
            'user_id': '10001',
            'domain': 'amazon.com',
            'category': 'SUPERMARKET'
        },
        'output': {
            'status': 200
        }
    }
]


def run_test():
    response = Subject.handler(test['input'], None)
    if response['status_code'] != test['output']['status']:
        raise Exception(test['description'] + ' validation failed: ')


if __name__ == '__main__':
    specific_test = ''
    for test in test_data:
        if not specific_test or specific_test is test['description']:
            print(test['description'])
            run_test()
