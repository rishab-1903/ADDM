from google.cloud import storage
import os
from utils.logger import PipelineLogger

class GCSManager:
    def __init__(self, bucket_name="addm-app"):
        self.bucket_name = bucket_name
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(bucket_name)
        self.logger = PipelineLogger("GCSManager")
    
    def upload_string_to_gcs(self, destination_path, content):
        """Upload string content to GCS"""
        try:
            blob = self.bucket.blob(destination_path)
            blob.upload_from_string(content)
            gcs_path = f"gs://{self.bucket_name}/{destination_path}"
            self.logger.info(f"Uploaded to GCS: {gcs_path}")
            return gcs_path
        except Exception as e:
            self.logger.error(f"Failed to upload {destination_path}: {str(e)}")
            raise
    
    def upload_file_to_gcs(self, local_file_path, destination_path):
        """Upload local file to GCS"""
        try:
            blob = self.bucket.blob(destination_path)
            blob.upload_from_filename(local_file_path)
            gcs_path = f"gs://{self.bucket_name}/{destination_path}"
            self.logger.info(f"Uploaded file to GCS: {gcs_path}")
            return gcs_path
        except Exception as e:
            self.logger.error(f"Failed to upload file {local_file_path}: {str(e)}")
            raise
    
    def upload_credentials_and_env(self, credentials_content, env_data):
        """Upload AWS credentials and .env file to GCS"""
        self.logger.log_stage("Upload Credentials and Environment")
        
        try:
            # Upload AWS credentials
            credentials_path = "cred-aws/credentials"
            creds_gcs_path = self.upload_string_to_gcs(credentials_path, credentials_content)
            
            # Create .env content
            env_content = (
                f"NEO4J_URI={env_data['NEO4J_URI']}\n"
                f"NEO4J_USER={env_data['NEO4J_USER']}\n"
                f"NEO4J_PASSWORD={env_data['NEO4J_PASSWORD']}\n"
                f"AWS_PROFILE={env_data['AWS_PROFILE']}\n"
                f"AWS_DEFAULT_REGION={env_data['AWS_DEFAULT_REGION']}\n"
                f"CREDENTIALS_FILE=gs://{self.bucket_name}/{credentials_path}\n"
            )
            
            # Upload .env file
            env_path = ".env"
            env_gcs_path = self.upload_string_to_gcs(env_path, env_content)
            
            self.logger.log_completion("Upload Credentials and Environment")
            return {
                "credentials_path": creds_gcs_path,
                "env_path": env_gcs_path
            }
            
        except Exception as e:
            self.logger.log_failure("Upload Credentials and Environment", e)
            raise
    
    def upload_output_files(self, local_output_dir):
        """Upload all output files from local directory to GCS"""
        self.logger.log_stage("Upload Output Files")
        
        try:
            uploaded_files = {}
            output_files = [
                "export0.csv",
                "export1.csv", 
                "neo4j_nodes_by_labels.xlsx",
                "node_to_root_mapping.xlsx"
            ]
            
            for filename in output_files:
                local_path = os.path.join(local_output_dir, filename)
                if os.path.exists(local_path):
                    gcs_destination = f"output/{filename}"
                    gcs_path = self.upload_file_to_gcs(local_path, gcs_destination)
                    uploaded_files[filename] = gcs_path
                else:
                    self.logger.warning(f"Output file not found: {local_path}")
            
            self.logger.log_completion("Upload Output Files")
            return uploaded_files
            
        except Exception as e:
            self.logger.log_failure("Upload Output Files", e)
            raise