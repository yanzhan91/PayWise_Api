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
    event['timestamp'] = time.strftime('%m-%d-%Y %H:%M:%S %Z')

    feedbacks_table = boto3.resource('dynamodb').Table('PayWise_Feedbacks')
    feedbacks_table.put_item(
        Item=dict((k, v) for k, v in event.items() if v)
    )
    return 200


if __name__ == '__main__':
    def current_milli_time(): return int(round(time.time() * 1000))
    print(current_milli_time())
    handler({
        'rating': 5,
        'message': '',
        'email': 'test'
    }, None)
    print(current_milli_time())
