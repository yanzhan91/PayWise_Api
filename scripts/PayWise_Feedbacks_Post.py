import boto3
import uuid
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
    event['feedback_id'] = str(uuid.uuid4())

    if not event['message']:
        del event['message']

    if not event['email']:
        del event['email']

    feedbacks_table = boto3.resource('dynamodb').Table('PayWise_Feedbacks')
    response = feedbacks_table.put_item(
        Item=event
    )
    return response['ResponseMetadata']['HTTPStatusCode']


if __name__ == '__main__':
    def current_milli_time(): return int(round(time.time() * 1000))
    print(current_milli_time())
    handler({
        'rating': 5,
        'message': '',
        'email': 'test'
    }, None)
    print(current_milli_time())
