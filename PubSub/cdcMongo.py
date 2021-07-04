import os
import pymongo
import json

from google.cloud import pubsub_v1


class personTarget: 
    def __init__(self, id, intent, payment_method, merchant_fantasy_name, soft_descriptor, payer_full_name,payer_document_number ): 
        self.id = id
        self.intent = intent
        self.payment_method = payment_method
        self.merchant_fantasy_name = merchant_fantasy_name
        self.soft_descriptor = soft_descriptor
        self.payer_full_name = payer_full_name
        self.ayer_document_number = payer_document_number
    
    def __repr__(self): 
        pass
        #return "Test a:% s b:% s" % (self.a, self.b) 


def getTransactionValues(document, d):
    
    if 'transaction' in document:
        transaction = document['transaction']
        d['payment_method']        = transaction['payment_method'] if 'payment_method' in transaction else None
        d['merchant_fantasy_name'] = transaction['merchant_fantasy_name'] if 'merchant_fantasy_name' in transaction else None
        d['soft_descriptor']       = transaction['soft_descriptor'] if 'soft_descriptor' in transaction else None

    else:
        d['payment_method'] = None
        d['merchant_fantasy_name'] = None
    
    return d

def getPayerValues(document, d):
    if 'payer' in document:
        payer       = document['payer']
        d['full_name']       =  payer['full_name']  if 'full_name' in payer else None
        d['document_number'] =  payer['document_number'] if 'document_number' in payer else None
    else:
        d['full_name'] = None
        d['document_number'] = None

    return d 

def getGlobalValues(document, d):

    d['_id']        =  str(document['_id']) if '_id' in  document else None
    d['intent']     =  document['state'] if 'state' in  document else None
    d['state']      =  document['payment_method'] if 'payment_method' in  document else None
    d['create_time']=  str(document['create_time']) if 'create_time' in  document else None
    return d

# Para sistema de recomendacion
def getSKUProducts():
    pass
#https://github.com/blainemincey/change-stream-demo/blob/master/change_stream_listener.py

def cdcInsertData():
    data_test = set()


    #Conexión MongoDB
    client = pymongo.MongoClient(os.environ.get('connection_string', '-1'))
    db     = client[os.environ.get('db_name', '-1')]
    collection = db[os.environ.get('collection_name', '-1')]
    
    #Objeto publisher - PubSub
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(os.environ.get('project_id', '-1'), os.environ.get('topic_id', '-1'))

    # Change stream pipeline
    pipeline = [
        {'$match': {'operationType': 'insert'}}
    ]
    cont = 0
    d = {}
    try:
        #CDC Insert values
        for document in collection.watch(pipeline=pipeline, full_document='updateLookup'):
            cont += 1
            if 'fullDocument' in document:
                document    = document['fullDocument']   
                d = getGlobalValues(document, d)
                d = getTransactionValues (document, d)
                d = getPayerValues(document, d)
                sendRutPubsub(d, publisher, topic_path, cont, data_test)
            
    except Exception as e:
        print(e)
        #print(document)

def sendRutPubsub(target, publisher, topic_path, cont, data_test):
   

    if cont % 10000 == 0:
        print('Rows {}'.format(cont))

    #------------Logica de negocio --------------
    if target['document_number'] in data_test:
        data = json.dumps(target).encode('utf-8')
        future = publisher.publish( topic_path, data, origin="cdc-insert-mongodb", username="data-engineer-fpay")
        print(future.result())
        print(target)


def insertBigQuery():
    pass



if __name__ == "__main__":
    os.environ["connection_string"] = ${CONNECTION}
    os.environ["db_name"]           = ${DB_NAME}
    os.environ["collection_name"]   = ${COLLECTION_NAME}
    os.environ["project_id"]        = ${PROJECT_ID_GCP}
    os.environ["topic_id"]          = ${TOPIC_ID_GCP}
    
    cdcInsertData()