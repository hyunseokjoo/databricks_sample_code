# dbutils 라는 함수를 사용하여 다양한 작업을 할 수 있음 
# https://docs.databricks.com/dev-tools/databricks-utils.html 참조

# help 명령어 
dbutils.fs.help("cp")
dbutils.fs.help("mv")

# File System에 접근(DBFS)
# 조회
dbutils.fs.ls("/tmp")

# 삭제 
dbutils.fs.rm("path/file")

# 복사
dbutils.fs.cp("/FileStore/old_file.txt", "/tmp/new/new_file.txt")


# 이동
dbutils.fs.mv("/FileStore/my_file.txt", "/tmp/parent/child/grandchild")

# 디렉토리 만들기 
dbutils.fs.mkdirs("/tmp/parent/child/grandchild")

# 마운트
aws_bucket_name = "my-bucket"
mount_name = "s3-my-bucket"

dbutils.fs.mount("s3a://%s" % aws_bucket_name, "/mnt/%s" % mount_name)

# 언마운트
dbutils.fs.unmount("/mnt/<mount-name>")