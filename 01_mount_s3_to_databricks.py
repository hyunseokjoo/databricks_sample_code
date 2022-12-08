import os

# IAM 사용자 만들기에서 만든 사용자 access key
ACCESS_KEY = "Your Access Key"
SECRET_KEY = "Your Secret Key"
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME = "Your Bucket Name"
MOUNT_NAME = "Your Databricks dbfs Path Name"

# databricks는 dbfs라는 경로에 파일을 보관 마운트 되면 아래 경로에 저장
confirm = os.path.isdir(f"/dbfs/mnt/{MOUNT_NAME}")

if confirm == True:
   print("alreay mount")
else: 
   dbutils.fs.mount("s3a://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)
   print("mount success")




ㄴ