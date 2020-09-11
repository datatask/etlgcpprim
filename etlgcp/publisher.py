from google.cloud import pubsub_v1, logging
import time
import json

def publish(logger, labels, publisher_client, project_id, topic_name, message, attributes = dict(), retry=0):
    '''
    Function to publish a message in PubSub topic
    Input :
        logger : GCP logger client
        labels : labels
        publisher_client : GCP PubSub publisher client
        project_id : id of the GCP project
        topic_name : name of the topic where the message will be published
        message : message to publish (str)
        attributes (optionnal) : a dict with some additional data
    Output :
        True if the message has been published, False otherwise
    '''
    topic_path = publisher_client.topic_path(project_id,topic_name)
    try:
        publisher_client.get_topic(topic_path)
        topic_request_status = "topic exists"
    except:
        publisher_client.create_topic(topic_path)
        topic_request_status = "topic created"
    logger.log_text(text="Topic request status : {}".format(topic_request_status), severity="DEBUG", labels=labels)
    data = message.encode("utf-8")
    if isinstance(attributes,dict):
        attrs = json.dumps(attributes)
    else:
        attrs=''
        logger.log_text(text="Error : attributes ignored in message (must be a dictonary)", severity="ERROR", labels=labels)
    try:
        publisher_client.publish(topic_path, data=data, attributes = attrs)
        logger.log_text(text="Message : {} published with attributes : {}".format(data,attrs), severity="INFO", labels=labels)
        return True
    except Exception as e:
        logger.log_text(text="Error in publish : {} :: retry : {}".format(e,str(retry+1)), severity="ERROR", labels=labels)
        # Should work on 2nd try
        if retry < 2:
            time.sleep(10)
            return publish(logger,labels,publisher_client,project_id,topic_name,message,attributes,retry=retry+1)
        else:
            return False


if __name__ == "__main__":
    
    ### vars to be define by reading a parameter file or env vars ...
    project_id = ""
    topic_name = ""
    logger_name = ""

    ### set the GOOGLE_APPLICATION_CREDENTIALS env var for service account authentication
    publisher_client = pubsub_v1.PublisherClient()

    logging_client = logging.Client(project=project_id)
    logger = logging_client.logger(logger_name)
    
