import boto3
from boto3.dynamodb.conditions import Key


# Deprecated
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

    card_id = get_card_id_from_name(event['card_name'].title())
    delete_card_id_from_user(event['user_id'], card_id)

    return 'Card deleted successfully'


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


def delete_card_id_from_user(user_id, card_id):
    user_table = boto3.resource('dynamodb').Table('PayWise_Users')
    response = user_table.update_item(
        Key={
            'user_id': user_id
        },
        UpdateExpression='delete card_ids :c',
        ExpressionAttributeValues={
            ':c': {card_id}
        }
    )['ResponseMetadata']
    if response['HTTPStatusCode'] != 200:
        raise Exception('Internal Error: Failed to update Users table')


if __name__ == '__main__':
    handler({
        'user_id': '10001',
        'card_name': 'Chase Freedom'
    }, None)
