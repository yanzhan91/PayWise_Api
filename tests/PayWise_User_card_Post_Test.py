import scripts.PayWise_User_Card_Post as Subject

test_data = [
    {
        'description': 'Happy Path',
        'input': {
            'user_id': '10001',
            'card_name': 'Chase Freedom'
        },
        'output': {
            'status': 200
        }
    },
    {
        'description': 'Missing user_id',
        'input': {
            'card_name': 'Chase Freedom'
        },
        'output': {
            'status': 400
        }
    },
    {
        'description': 'Missing card_name',
        'input': {
            'user_id': '10001'
        },
        'output': {
            'status': 400
        }
    },
    {
        'description': 'Invalid card name',
        'input': {
            'user_id': '10001',
            'card_name': 'Test Card Name'
        },
        'output': {
            'status': 400
        }
    }
]


def run_test():
    response = Subject.handler(test['input'], None)
    if response['status_code'] != test['output']['status']:
        raise Exception(test['description'] + ' validation failed: ')


if __name__ == '__main__':
    specific_test = 'Invalid card name'
    for test in test_data:
        if not specific_test or specific_test is test['description']:
            print(test['description'])
            run_test()
