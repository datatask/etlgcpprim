from google.cloud import pubsub_v1, logging
import time
import json

def example_callback(message):
    '''
    Example of callback function tu retrieve message and attributes before acking
    '''
    attrs = dict(message.attributes)
    text = message.data.decode('utf-8')
    logger.log_text(text="Message read from pubsub : {}, with attributes : {}".format(text,json.dumps(attrs)), severity="DEBUG")
    message.ack()


def subscribe(logger,labels,subscriber_client,project_id,topic_name,subscription_name,callback):
    '''
    Function to compute message from a PubSub subscription
    Input :
        logger : GCP logger client
        labels : labels
        subscriber_client : GCP PubSub subscriber client
        project_id : id of the GCP project
        topic_name : name of the topic where the message are published
        subscription_name : name of the subscription where the message are received
        callback : function to compute the messages received (arg of this function is message, a PubSub message)
    Output :
        None
    '''
    subscription_path = subscriber_client.subscription_path(project_id,subscription_name)
    try:
        subscriber_client.get_subscription(subscription_path)
        subscription_request_status = "Subscription exists"
    except:
        topic_path = publisher_client.topic_path(project_id,topic_name)
        subscriber_client.create_subscription(subscription_path,topic_path)
        subscription_request_status = "Subscription created"
    logger.log_text(text="Subscription request status : {}".format(subscription_request_status), severity="DEBUG",labels=labels)
    future = subscriber_client.subscribe(subscription_path,callback)
    try:
        future.result()
    except KeyboardInterrupt:
        future.cancel()



if __name__ == "__main__":
    
    ### vars to be define by reading a parameter file or env vars ...
    project_id = ""
    topic_name = ""
    subscription_name = ""
    logger_name = ""

    ### set the GOOGLE_APPLICATION_CREDENTIALS env var for service account authentication
    publisher_client = pubsub_v1.PublisherClient()
    subscriber_client = pubsub_v1.SubscriberClient()
    logging_client = logging.Client(project=project_id)
    logger = logging_client.logger(logger_name)

