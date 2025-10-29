import boto3
from botocore.exceptions import NoCredentialsError, ClientError

def upload_to_s3(local_file: str, bucket_name: str, s3_path: str):
    """
    Upload a local file to S3 using credentials from aws configure.

    Args:
        local_file (str): Local path to file
        bucket_name (str): S3 bucket name
        s3_path (str): S3 path including filename
    """
    try:
        # boto3 automatically uses credentials saved by `aws configure`
        s3 = boto3.client('s3')
        s3.upload_file(local_file, bucket_name, s3_path)
        print(f"✅ Uploaded {local_file} → s3://{bucket_name}/{s3_path}")

    except FileNotFoundError:
        print(f"❌ File not found: {local_file}")
    except NoCredentialsError:
        print("❌ AWS credentials not found. Run `aws configure` first.")
    except ClientError as e:
        print(f"❌ Failed to upload: {e}")

if __name__ == "__main__":
    print("🔍 Starting upload...")  # ADD THIS
    # Example usage
    local_file = "src/data/air_quality/s3/processed_air_quality.csv"
    bucket_name = "kma-data-storage"
    s3_path = "processed/air_quality/airquality_daily.csv"

    print(f"🔍 Local file: {local_file}")  # ADD THIS
    print(f"🔍 Bucket: {bucket_name}")  # ADD THIS
    print(f"🔍 S3 path: {s3_path}")  # ADD THIS
    
    upload_to_s3(local_file, bucket_name, s3_path)
    print("🔍 Script finished")  # ADD THIS