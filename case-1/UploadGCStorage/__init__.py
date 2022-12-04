from google.cloud import storage
import requests
import os

class UploadGCStorage:

    def __init__(self, project_id, url):
        self.client = storage.Client(project=project_id)
        self.url = url
        self.filename = url.split('/')[-1]
    
    def bucket_list(self):
        bucket_lst = []
        for bucket in self.client.list_buckets():
            bucket_lst.append(str(bucket).split(' ')[1][:-1])
        return bucket_lst
    
    def create_bucket(self, bucket_name):
        self.client.create_bucket(bucket_name)
    
    def download_file(self):
        try:
            response = requests.get(self.url)
            with open(self.filename, "wb") as f:
                f.write(response.content)
            print('download success')
        except:
            print("download failed")
    
    def upload_file(self, bucket_name):
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(self.filename)
        blob.upload_from_filename(self.filename)
        os.remove(self.filename)
        print('upload success')
    

    
