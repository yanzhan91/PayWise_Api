import boto3
from boto3.dynamodb.conditions import Key


def handler(event, context):
    try:
        print(event)
        response = start_request(event)
        print(response)
        return response
    except Exception as e:
        print(e.args[0])
        raise e


def start_request(event):
    if 'user_id' not in event or not event['user_id']:
        raise Exception('Bad Request: Missing user id in request')
    if not event['domain'] and not event['name'] and not event['category']:
        raise Exception('Bad Request: Missing one of domain, name, and category in request')

    user_id = event['user_id']
    store_name = ''
    if event['name']:
        store_name = event['name'].lower()
        category = get_category_from_name(store_name)
    elif event['domain']:
        store_name, category = get_name_and_category_from_domain(event['domain'])
    else:
        category = event['category'].upper()

    if not category:
        raise Exception('Internal Error: Cannot get category')

    card_list = get_users_cards(user_id)
    rewards = get_rewards(card_list, store_name, category)

    rewards.sort(key=lambda x: x['reward'], reverse=True)

    return rewards


def get_name_and_category_from_domain(domain):
    stores_table = boto3.resource('dynamodb').Table('PayWise_Stores')
    stores_items = stores_table.query(
        IndexName='store_domain-index',
        KeyConditionExpression=Key('store_domain').eq(domain),
        Limit=1
    )['Items']
    if not stores_items:
        raise Exception({400, 'Invalid domain in request'})
    return stores_items[0]['store_name'], stores_items[0]['store_category']


def get_category_from_name(name):
    stores_table = boto3.resource('dynamodb').Table('PayWise_Stores')
    response = stores_table.get_item(
        Key={
            'store_name': name
        }
    )
    if 'Item' not in response:
        raise Exception('Bad Request: name not found in database: ' + name)
    return response['Item']['store_category']


def get_rewards(card_list, store_name, category):
    card_list = map(lambda x: get_card_info(x), card_list)
    return list(map(lambda x: calc_rewards(x, store_name, category), card_list))


def get_card_info(card_id):
    card_table = boto3.resource('dynamodb').Table('PayWise_Cards')
    return card_table.get_item(Key={'card_id': card_id})['Item']


def get_users_cards(user_id):
    user_table = boto3.resource('dynamodb').Table('PayWise_Users')
    return user_table.get_item(Key={'user_id': user_id})['Item']['card_ids']


def calc_rewards(card_info, store_name, category):
    rewards = None
    if store_name and 'store_name' in card_info['rewards']:
        if store_name in card_info['rewards']['store_name']:
            rewards = card_info['rewards']['store_name'][store_name]
    if rewards is None:
        if category in card_info['rewards']['categories']:
            rewards = card_info['rewards']['categories'][category]
        else:
            rewards = card_info['rewards']['categories']['ALL']

    card_info['reward'] = float(rewards)
    card_info['rewards_desc'] = list(card_info['rewards_desc'])

    del card_info['rewards']
    del card_info['card_id']

    return card_info


if __name__ == '__main__':
    print(start_request({
        'user_id': '10001',
        'name': '',
        "domain": "",
        "category": "GROCERY"
    }))
