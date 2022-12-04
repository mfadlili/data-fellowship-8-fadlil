from UploadGCStorage import UploadGCStorage
import os
import json

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= 'key.json'

if __name__ == '__main__':
    with open("input.json") as f:
        data = json.load(f)

    project_id = data["project_id"]
    url = data["url"]
    bucket = data["bucket"]

    client = UploadGCStorage(project_id, url)
    client.download_file()
    try :
        if bucket in client.bucket_list():
            client.upload_file(bucket)
        else:
            print("Bucket not found, trying to create it")
            try:
                client.create_bucket(bucket)
                print("A new bucket has created!")
                client.upload_file(bucket)
            except:
                print("The bucket namespace is shared by all users of the system. Please select a different name and try again!")
    except:
        print("Wrong Project ID!")
