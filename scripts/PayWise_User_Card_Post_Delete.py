import boto3
from boto3.dynamodb.conditions import Key
import difflib


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
    if 'user_id' not in event:
        raise Exception('Bad Request: No user_id found')

    if 'card_name' not in event:
        raise Exception('Bad Request: No card_name found')

    if 'operation' not in event:
        raise Exception('Internal Error: No operation received')

    card_name = find_existing_card_name(event['card_name'].title(), event['device'])
    card_id = get_card_id_from_name(card_name)
    add_card_id_to_user(event['user_id'], card_id, event['operation'])

    return card_name


def find_existing_card_name(card_name, device):
    if device != 'Alexa':
        return card_name

    card_table = boto3.resource('dynamodb').Table('PayWise_Cards')
    response = card_table.scan(
        ProjectionExpression='card_name'
    )
    all_cards = list(map(lambda x: x['card_name'], response['Items']))
    card_names = difflib.get_close_matches(card_name, all_cards, 1)
    if not card_names:
        raise Exception('Bad Request: Cannot find card name in database: ' + card_name)
    return card_names[0]


def get_card_id_from_name(card_name):
    card_table = boto3.resource('dynamodb').Table('PayWise_Cards')

    response = card_table.query(
        IndexName='card_name-index',
        KeyConditionExpression=Key('card_name').eq(card_name),
        Limit=1
    )['Items']
    if not response:
        raise Exception('Bad Request: Cannot find card name in database: ' + card_name)
    return response[0]['card_id']


def add_card_id_to_user(user_id, card_id, operation):
    update_exp = operation + ' card_ids :c'
    user_table = boto3.resource('dynamodb').Table('PayWise_Users')
    response = user_table.update_item(
        Key={
            'user_id': user_id
        },
        UpdateExpression=update_exp,
        ExpressionAttributeValues={
            ':c': {card_id}
        }
    )['ResponseMetadata']
    if response['HTTPStatusCode'] != 200:
        raise Exception('Internal Error: Failed to update Users table')


if __name__ == '__main__':
    print(start_request({
        'user_id': '10001',
        'card_name': 'cas free',
        'operation': 'delete'
    }))
