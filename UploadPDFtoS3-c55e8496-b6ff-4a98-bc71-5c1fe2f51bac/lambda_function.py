import boto3
import urllib.parse
import email
import traceback
import logging
import os
import re

S3_BUCKET_1 = os.environ.get('S3_BUCKET_1')
S3_BUCKET_2 = os.environ.get('S3_BUCKET_2')
BUCKET_1_FOLDER = os.environ.get('BUCKET_1_FOLDER')
BUCKET_2_FOLDER = os.environ.get('BUCKET_2_FOLDER')


#logging - log settings
logger = logging.getLogger()
logger.setLevel(logging.WARNING)

s3ClientUpload = boto3.client('s3', region_name='ap-southeast-1')
#bucketUpload = "textract-pdf-store"

s3Client = boto3.client('s3')
s3Resource = boto3.resource('s3')
# bucketGet = 'axrail-email-bucket'
#key = "tcoeio56er4vks872ot07btjeubdteanlcjb92g1" #onigiri
#key = "ctpj222oluht6ilj7a7jsjssb2sv35i5ec3pbao1" #bento
# key = "ogsdemua9ugh1odfolq0chrhm99hmv53783pbao1" #sandwich
emailTempFile = "/tmp/emailReceived"
attachmentTempFile = "/tmp/attachment.pdf"

class Attachment(object):
    def __init__(self):
        self.data = None
        self.content_type = None
        self.size = None
        self.name = None
        
def lambda_handler(event, context):
    print(event)
    try:
        bucketGet = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
        
        s3Resource.Bucket(bucketGet).download_file(key, emailTempFile)
        openEmailFile = open(emailTempFile, 'r')
        message = email.message_from_file(openEmailFile) #shorthand for Parser().parse
        
        spamStatus = False
        
        #check virus and spam status
        if('X-SES-Spam-Verdict' in message and 'X-SES-Virus-Verdict' in message):
            if(message['X-SES-Spam-Verdict'] != 'PASS' or message['X-SES-Virus-Verdict'] != 'PASS'):
                spamStatus = True
        
        if(spamStatus == False):
            print("Processing email content...")
            #print(message['Subject'])
            if(message.is_multipart()):
                for part in message.walk():
                    content_type = part.get_content_type()
                    content_dispo = str(part.get('Content-Disposition'))
                    if("attachment" in content_dispo):
                        print(content_type)
                        if(content_type == "application/pdf") or (content_type == "application/vnd.ms-excel") or (content_type == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"):
                            attachment = Attachment()
                            attachment.name = part.get_filename()
                            attachment.data = part.get_payload(decode=True)
                            attachment.content_type = content_type
                            attachment.size = len(attachment.data)
                            
                            if(bool(attachment.name)):
                                fp = open(attachmentTempFile, 'wb')
                                fp.write(attachment.data)
                                fp.close()

                            try:
                                subject = message['Subject'].lower()
                                tnb = re.match('tnb', subject.lower())
                                fm_invoice = re.match('fminvoice', subject.lower())
                                with open(attachmentTempFile, "rb") as f:
                                    #if subject == 'cost':
                                    if (content_type == "application/vnd.ms-excel") or (content_type == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"):
                                        bucketUpload = S3_BUCKET_2
                                        s3ClientUpload.upload_fileobj(f, bucketUpload, '{}{}'.format(BUCKET_2_FOLDER, attachment.name))
                                        print(attachment.name + " uploaded")
                                        print("File uploaded to ", S3_BUCKET_2)   
                                        
                                    else:
                                        bucketUpload = S3_BUCKET_1
                                        s3ClientUpload.upload_fileobj(f, bucketUpload, '{}{}'.format(BUCKET_1_FOLDER, attachment.name))
                                        print(attachment.name + " uploaded")
                                        print("File uploaded to ", S3_BUCKET_1)
                            except:
                                 print("Error occurred while uploading file object: ")
                                 traceback.print_exc()

        else:
            print("Spam detected")
            
            openEmailFile.close()
            print("File closed!")
                    
    except Exception as error:                
        logger.exception(error)
        response = {
            'status': 500,
            'error': {
                'function_name': "lambda_handler",
                'type': type(error).__name__,
                'description': str(error)
            },
        }        
                
 