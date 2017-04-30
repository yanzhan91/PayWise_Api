import boto3
from boto3.dynamodb.conditions import Key


def handler(event, context):
    print(event)
    response = start_request(event)
    print(response)
    return response


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
    elif resource == 'user-cards':
        table = 'PayWise_Users'
        attribute = 'card_ids'
        if 'user_id' not in event or not event['user_id']:
            return generate_error_response(400, 'No user_id found')
        user_id = event['user_id']
        try:
            return get_user_cards(table, attribute, user_id)
        except Exception as e:
            return e.args[0]
    else:
        return generate_error_response(500, 'No resource found: ' + resource)


def get_attribute_value_list(table, attribute):
    card_table = boto3.resource('dynamodb').Table(table)
    response = card_table.scan(
        ProjectionExpression=attribute
    )
    return list(map(lambda x: x[attribute], response['Items']))


def get_user_cards(table, attribute, user_id):
    card_id_list = get_attribute_value_list_with_key(table, attribute, user_id)
    if not card_id_list:
        raise Exception(generate_error_response(400, 'User_id not found: ' + user_id))
    return list(map(lambda x: map_ids_to_card_name(x), card_id_list[0]))


def get_attribute_value_list_with_key(table, attribute, user_id):
    card_table = boto3.resource('dynamodb').Table(table)
    response = card_table.query(
        ProjectionExpression=attribute,
        KeyConditionExpression=Key('user_id').eq(user_id),
        Limit=1
    )
    return list(map(lambda x: x[attribute], response['Items']))


def map_ids_to_card_name(card_id):
    table = boto3.resource('dynamodb').Table('PayWise_Cards')
    response = table.query(
        ProjectionExpression='card_name',
        KeyConditionExpression=Key('card_id').eq(card_id),
        Limit=1
    )
    
    return response['Items'][0]['card_name']


def generate_error_response(status_code, message):
    return {
        'status_code': status_code,
        'message': message
    }


if __name__ == '__main__':
    handler({
        'method': 'get',
        'resource': 'user-cards',
        'user_id': '10001'
    }, None)
