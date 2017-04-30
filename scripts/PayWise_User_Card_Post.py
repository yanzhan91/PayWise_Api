import boto3
from boto3.dynamodb.conditions import Key


def handler(event, context):
    print(event)
    response = start_request(event)
    print(response)
    return response


def start_request(event):
    if 'user_id' not in event:
        return generate_response(400, 'No user_id found.')

    if 'card_name' not in event:
        return generate_response(400, 'No card_name found.')

    try:
        card_id = get_card_id_from_name(event['card_name'])
        add_card_id_to_user(event['user_id'], card_id)
    except Exception as e:
        return e.args[0]

    return generate_response(200, 'Card added successfully')


def get_card_id_from_name(card_name):
    card_table = boto3.resource('dynamodb').Table('PayWise_Cards')
    response = card_table.query(
        IndexName='card_name-index',
        KeyConditionExpression=Key('card_name').eq(card_name),
        Limit=1
    )['Items']
    if not response:
        raise Exception(generate_response(400, 'Cannot find card name: ' + card_name))
    return response[0]['card_id']


def add_card_id_to_user(user_id, card_id):
    user_table = boto3.resource('dynamodb').Table('PayWise_Users')
    response = user_table.update_item(
        Key={
            'user_id': user_id
        },
        UpdateExpression='add card_ids :c',
        ExpressionAttributeValues={
            ':c': {card_id}
        }
    )['ResponseMetadata']
    if response['HTTPStatusCode'] != 200:
        raise Exception(generate_response(500, 'Failed to update Users table'))


def generate_response(status_code, message):
    return {
        'status_code': status_code,
        'message': message
    }
