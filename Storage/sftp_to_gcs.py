import os, re
import paramiko
from datetime import datetime, timedelta, date
from google.cloud import storage
#from dateutil.relativedelta import relativedelta




def upload_binary_to_gcs(uploaded_file,file_name, bucket_name):

    if not uploaded_file:
        return 'No file uploaded.'

    # Create a Cloud Storage client.
    gcs = storage.Client()

    # Get the bucket that the file will be uploaded to.
    bucket = gcs.get_bucket(bucket_name)

    # Create a new blob and upload the file's content.
    blob = bucket.blob(file_name)

    blob.upload_from_string(
        uploaded_file.read(),)
        #content_type=uploaded_file.conte


def get_data_from_sftp(hostname, username, password, port, pathpem=None ):

    # Tunnel creation
    transport = paramiko.Transport((hostname,port))
    transport.connect(username=username, pkey=key)

    # Connection
    with paramiko.SFTPClient.from_transport(transport) as sftp:  


        keyfile = os.path.expanduser(pathpem)

        # Key definition
        key = paramiko.RSAKey.from_private_key_file(keyfile, password)

        print("Connection succesfully stablished ...  \n")
        path_remote = ''
        sftp.chdir(path_remote)

        files_to_transfer = []

        print("The current directory is: ",sftp.getcwd(),"\n")
        print("The elements in the current directory are: ")

        #extract date from - X days
        third_day = date.today() + timedelta(days=-5)

        print('-',third_day.strftime('%Y%m%d'))

        for element in sftp.listdir():
            #Conditional to extract trough regexp

            match =  re.search(r'(((19|20)\d{2}(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])))',element)

            if match:
                date_object = datetime.strptime(match.group(1), '%Y%m%d')
                print('---',date_object.strftime('%Y%m%d'))
                if (third_day.strftime('%Y%m%d') < date_object.strftime('%Y%m%d') ) and '8075_CLICK_' in element:
                    print('----',element)
                    responsys_path = '/home/.../download/' + element
                    #local_path = 'C:\SFTP_PY/File/' + element
                    #sftp.get(responsys_path, local_path, callback=None)
                    result_binary_sftp= sftp.open(responsys_path, mode='r', bufsize=-1)
                    with result_binary_sftp as f:
                    	upload_binary_to_gcs(f,element)
        

            

# Variables
hostnamet = 'xxx'
port = 22
username = 'xxx'
password = 'xxx'
pathpem  = '/path/../PrivKey.pem'

get_data_from_sftp(hostname, username, password, port, pathpem)


