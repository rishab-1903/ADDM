import subprocess
import time
import os
from utils.logger import PipelineLogger

class DockerManager:
    def __init__(self):
        self.logger = PipelineLogger("DockerManager")
    
    def get_config(self):
        """Get configuration from app context or environment"""
        try:
            from flask import current_app
            if hasattr(current_app, 'config') and 'PIPELINE_CONFIG' in current_app.config:
                # Create a simple config object from stored dict
                class SimpleConfig:
                    def __init__(self, config_dict):
                        for key, value in config_dict.items():
                            setattr(self, key, value)
                return SimpleConfig(current_app.config['PIPELINE_CONFIG'])
        except:
            pass
        
        # Fallback to environment config
        from utils.config import Config
        return Config()
    
    def run_command(self, command, check=True):
        """Execute a shell command with logging"""
        self.logger.info(f"Executing: {' '.join(command)}")
        try:
            result = subprocess.run(
                command, 
                check=check, 
                capture_output=True, 
                text=True,
                timeout=1800  # 30 minutes timeout
            )
            if result.stdout:
                self.logger.info(f"Command output: {result.stdout}")
            return result
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Command failed: {e}")
            if e.stderr:
                self.logger.error(f"Error output: {e.stderr}")
            raise
        except subprocess.TimeoutExpired as e:
            self.logger.error(f"Command timed out: {e}")
            raise
    
    def start_neo4j_container(self):
        """Start Neo4j container with environment variables"""
        self.logger.log_stage("Start Neo4j Container")
        
        try:
            # Stop and remove existing container if it exists
            self.logger.info("Cleaning up existing Neo4j container...")
            subprocess.run(["docker", "stop", "cartography-neo4j"], 
                         capture_output=True, check=False)
            subprocess.run(["docker", "rm", "cartography-neo4j"], 
                         capture_output=True, check=False)
            
            # Get configuration
            config = self.get_config()
            
            # Debug: Print config values
            self.logger.info(f"Neo4j Config - URI: {config.NEO4J_URI}, User: {config.NEO4J_USER}, Password: {'***' if config.NEO4J_PASSWORD else 'None'}")
            
            # Neo4j 4.4 requires username to be 'neo4j', so we'll force it
            neo4j_user = "neo4j"
            neo4j_password = config.NEO4J_PASSWORD or "test123"  # Default password if none provided
            
            # Start Neo4j container with environment variables
            neo4j_command = [
                "docker", "run", "-d",
                "--name", "cartography-neo4j",
                "--network", "cartography-network",
                "-p", "7474:7474",
                "-p", "7687:7687", 
                "-v", "neo4j-data:/data",
                "-e", f"NEO4J_AUTH={neo4j_user}/{neo4j_password}",
                "-e", "NEO4J_PLUGINS=[\"apoc\"]",  # Enable APOC plugin for cartography
                "neo4j:4.4-community"
            ]
            
            # Create network first
            subprocess.run(["docker", "network", "create", "cartography-network"], 
                         capture_output=True, check=False)
            
            self.run_command(neo4j_command)
            
            # Wait for Neo4j to be ready
            self.logger.info("Waiting for Neo4j to start...")
            time.sleep(45)  # Give Neo4j more time to initialize
            
            # Test Neo4j connection
            self.test_neo4j_connection(neo4j_user, neo4j_password)
            
            self.logger.log_completion("Start Neo4j Container")
            
        except Exception as e:
            self.logger.log_failure("Start Neo4j Container", e)
            raise
    
    def test_neo4j_connection(self, username, password):
        """Test Neo4j connection"""
        try:
            self.logger.info("Testing Neo4j connection...")
            
            # Test connection using cypher-shell in the container
            test_command = [
                "docker", "exec", "cartography-neo4j",
                "cypher-shell", "-u", username, "-p", password,
                "RETURN 'Connection successful' AS status"
            ]
            
            result = subprocess.run(test_command, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                self.logger.info("Neo4j connection test successful")
            else:
                self.logger.warning(f"Neo4j connection test failed: {result.stderr}")
                
        except Exception as e:
            self.logger.warning(f"Could not test Neo4j connection: {str(e)}")
            # Don't raise here as Neo4j might still work for cartography
    
    def start_cartography_container(self):
        """Start Cartography container"""
        self.logger.log_stage("Start Cartography Container")
        
        try:
            # Stop and remove existing container if it exists
            subprocess.run(["docker", "stop", "cartography-account1"], 
                         capture_output=True, check=False)
            subprocess.run(["docker", "rm", "cartography-account1"], 
                         capture_output=True, check=False)
            
            # Get configuration and GCS manager
            from services.gcs_manager import GCSManager
            
            gcs_manager = GCSManager()
            config = self.get_config()
            
            # Debug: Print config values
            self.logger.info(f"Cartography Config - NEO4J_URI: {config.NEO4J_URI}, AWS_PROFILE: {config.AWS_PROFILE}")
            
            # Validate required config
            if not config.NEO4J_URI:
                raise ValueError("NEO4J_URI is empty or not set")
            if not config.NEO4J_PASSWORD:
                raise ValueError("NEO4J_PASSWORD is empty or not set")
            
            # Create local temp directories
            os.makedirs("./tmp/aws-creds", exist_ok=True)
            
            # Download AWS credentials from GCS to local
            self.logger.info("Downloading AWS credentials from GCS...")
            creds_blob = gcs_manager.bucket.blob("cred-aws/credentials")
            creds_blob.download_to_filename("./tmp/aws-creds/credentials")
            
            # Set permissions on credentials file
            os.chmod("./tmp/aws-creds/credentials", 0o777)
            
            current_dir = os.getcwd()
            aws_creds_path = os.path.join(current_dir, "tmp", "aws-creds")
            # Use the container network name for Neo4j URI
            neo4j_uri = config.NEO4J_URI
            if "localhost" in neo4j_uri:
                neo4j_uri = neo4j_uri.replace("localhost", "cartography-neo4j")
            
            self.logger.info(f"Using Neo4j URI: {neo4j_uri}")
            
            # Start cartography container - the image already has 'cartography' as entrypoint
            cartography_command = [
                "docker", "run", "-d",
                "--name", "cartography-account1", 
                "--network", "cartography-network",
                "-v", f"{aws_creds_path}:/var/cartography/.aws",
                "-e", f"NEO4J_URI={neo4j_uri}",
                "-e", f"NEO4J_USER=neo4j",
                "-e", f"NEO4J_PASSWORD={config.NEO4J_PASSWORD}",
                "-e", f"AWS_PROFILE={config.AWS_PROFILE}",
                "-e", f"AWS_DEFAULT_REGION={config.AWS_DEFAULT_REGION}",
                "ghcr.io/cartography-cncf/cartography:latest",
                "--neo4j-uri", neo4j_uri,
                "--neo4j-user", "neo4j",
                "--neo4j-password-env-var", "NEO4J_PASSWORD"
            ]
            
            self.logger.info(f"Starting cartography with command: {' '.join(cartography_command)}")
            self.run_command(cartography_command)
            
            # Wait for cartography to complete (monitor container status)
            completed = self.wait_for_container_completion("cartography-account1", max_wait_minutes=20)
            if not completed:
                self.logger.info("Cartography container still running. Proceeding with available data...")

            
            # Clean up temp files
            subprocess.run(["rm", "-rf", "./tmp/aws-creds"], check=False)
            
            self.logger.log_completion("Start Cartography Container")
            
        except Exception as e:
            # Clean up temp files on error
            subprocess.run(["rm", "-rf", "/tmp/aws-creds"], check=False)
            self.logger.log_failure("Start Cartography Container", e)
            raise
    
    def wait_for_container_completion(self, container_name, max_wait_minutes=30):
        """Wait for a container to complete its work"""
        self.logger.info(f"Waiting for container {container_name} to complete...")
        
        max_wait_seconds = max_wait_minutes * 60
        waited_seconds = 0
        
        while waited_seconds < max_wait_seconds:
            try:
                # Check container status
                result = subprocess.run(
                    ["docker", "inspect", container_name, "--format", "{{.State.Status}}"],
                    capture_output=True, text=True, check=True
                )
                
                status = result.stdout.strip()
                self.logger.info(f"Container {container_name} status: {status}")
                
                if status == "exited":
                    # Check exit code
                    exit_code_result = subprocess.run(
                        ["docker", "inspect", container_name, "--format", "{{.State.ExitCode}}"],
                        capture_output=True, text=True, check=True
                    )
                    exit_code = int(exit_code_result.stdout.strip())
                    
                    if exit_code == 0:
                        self.logger.info(f"Container {container_name} completed successfully")
                        return True
                    else:
                        # Get container logs for debugging
                        logs_result = subprocess.run(
                            ["docker", "logs", container_name],
                            capture_output=True, text=True, check=False
                        )
                        self.logger.error(f"Container {container_name} failed with exit code {exit_code}")
                        self.logger.error(f"Container logs: {logs_result.stdout}")
                        raise Exception(f"Container {container_name} failed with exit code {exit_code}")
                
                elif status == "running":
                    time.sleep(30)  # Wait 30 seconds before checking again
                    waited_seconds += 30
                else:
                    raise Exception(f"Container {container_name} in unexpected status: {status}")
                    
            except subprocess.CalledProcessError as e:
                self.logger.error(f"Failed to check container status: {e}")
                raise
        
        self.logger.warning(f"Container {container_name} did not complete within {max_wait_minutes} minutes. Proceeding with partial data.")
        return False
    
    def setup_docker_environment(self):
        """Create necessary Docker network and volumes"""
        self.logger.log_stage("Setup Docker Environment")
        
        try:
            # Create network (ignore if exists)
            subprocess.run(["docker", "network", "create", "cartography-network"], 
                         capture_output=True, check=False)
            
            # Create volume (ignore if exists)
            subprocess.run(["docker", "volume", "create", "neo4j-data"], 
                         capture_output=True, check=False)
            
            self.logger.log_completion("Setup Docker Environment")
            
        except Exception as e:
            self.logger.log_failure("Setup Docker Environment", e)
            raise
    
    def cleanup_containers(self):
        """Clean up containers if needed"""
        self.logger.info("Cleaning up existing containers...")
        
        containers = ["cartography-neo4j", "cartography-account1"]
        for container in containers:
            subprocess.run(["docker", "stop", container], 
                         capture_output=True, check=False)
            subprocess.run(["docker", "rm", container], 
                         capture_output=True, check=False)