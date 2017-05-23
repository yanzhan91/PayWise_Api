import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr
import time


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
    resource = event['resource']
    if resource == 'card-names':
        table = 'PayWise_Cards'
        attribute = 'card_name'
        return get_attribute_value_list(table, attribute)
    elif resource == 'store-names':
        table = 'PayWise_Stores'
        attribute = 'store_name'
        return get_attribute_value_list(table, attribute)
    elif resource == 'store-categories':
        table = 'PayWise_Stores'
        attribute = 'store_category'
        return list(filter(lambda x: x != 'ALL' and x != 'NA', get_attribute_value_list(table, attribute)))
    elif resource == 'user-cards':
        table = 'PayWise_Users'
        attribute = 'card_ids'
        if 'user_id' not in event or not event['user_id']:
            raise Exception('Bad Request: No user_id found')
        user_id = event['user_id']
        return get_user_cards(table, attribute, user_id)
    else:
        raise Exception('Internal Error: No resource found: ' + resource)


def get_attribute_value_list(table, attribute):
    card_table = boto3.resource('dynamodb').Table(table)
    response = card_table.scan(
        ProjectionExpression=attribute
    )
    return list(set(map(lambda x: x[attribute], response['Items'])))


def get_user_cards(table, attribute, user_id):
    card_ids = get_attribute_value_list_with_key(table, attribute, user_id)
    if not card_ids:
        return []
    return map_ids_to_card_name(card_ids)


def get_attribute_value_list_with_key(table, attribute, user_id):
    card_table = boto3.resource('dynamodb').Table(table)
    response = card_table.query(
        ProjectionExpression=attribute,
        KeyConditionExpression=Key('user_id').eq(user_id),
        Limit=1
    )

    if not response['Items'] or attribute not in response['Items'][0]:
        return []

    return response['Items'][0][attribute]


def map_ids_to_card_name(card_ids):
    table = boto3.resource('dynamodb').Table('PayWise_Cards')
    response = table.scan(
        FilterExpression=Attr('card_id').is_in(card_ids),
        ProjectionExpression='card_name,card_img,card_url'
    )
    return response['Items']


if __name__ == '__main__':
    def current_milli_time(): return int(round(time.time() * 1000))
    print(current_milli_time())
    handler({
        'resource': 'store-names',
        'user_id': '10005'
    }, None)
    print(current_milli_time())
