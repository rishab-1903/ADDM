import os
from dotenv import load_dotenv

class Config:
    def __init__(self):
        load_dotenv()
        
        # GCS Configuration
        self.GCS_BUCKET = os.getenv("GCS_BUCKET", "addm-app")
        
        # Neo4j Configuration
        self.NEO4J_URI = os.getenv("NEO4J_URI")
        self.NEO4J_USER = os.getenv("NEO4J_USER") 
        self.NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
        
        # AWS Configuration
        self.AWS_PROFILE = os.getenv("AWS_PROFILE")
        self.AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")
        
        # Directory Configuration
        self.OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./tmp/output")
        
        # Docker Configuration
        self.CARTOGRAPHY_WAIT_TIME = int(os.getenv("CARTOGRAPHY_WAIT_TIME", "720"))  # 12 minutes
        
    def validate_required_config(self):
        """Validate that all required configuration is present"""
        required_vars = [
            "NEO4J_URI", "NEO4J_USER", "NEO4J_PASSWORD",
            "AWS_PROFILE", "AWS_DEFAULT_REGION"
        ]
        
        missing_vars = []
        for var in required_vars:
            if not getattr(self, var):
                missing_vars.append(var)
        
        if missing_vars:
            raise ValueError(f"Missing required configuration: {', '.join(missing_vars)}")
        
        return True
    
    def to_env_string(self):
        """Convert configuration to .env file format"""
        return (
            f"NEO4J_URI={self.NEO4J_URI}\n"
            f"NEO4J_USER={self.NEO4J_USER}\n"
            f"NEO4J_PASSWORD={self.NEO4J_PASSWORD}\n"
            f"AWS_PROFILE={self.AWS_PROFILE}\n"
            f"AWS_DEFAULT_REGION={self.AWS_DEFAULT_REGION}\n"
            f"CREDENTIALS_FILE=gs://{self.GCS_BUCKET}/cred-aws/credentials\n"
        )
    
    @classmethod
    def from_request_data(cls, data):
        """Create config from request payload"""
        config = cls()
        config.NEO4J_URI = data.get("NEO4J_URI")
        config.NEO4J_USER = "neo4j"
        config.NEO4J_PASSWORD = data.get("NEO4J_PASSWORD")
        config.AWS_PROFILE = data.get("AWS_PROFILE")
        config.AWS_DEFAULT_REGION = data.get("AWS_DEFAULT_REGION")
        return config