import logging
import sys
from datetime import datetime

class PipelineLogger:
    def __init__(self, name="pipeline"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        
        # Remove existing handlers to avoid duplicates
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(formatter)
        
        self.logger.addHandler(console_handler)
    
    def info(self, message):
        self.logger.info(message)
    
    def error(self, message):
        self.logger.error(message)
    
    def warning(self, message):
        self.logger.warning(message)
    
    def debug(self, message):
        self.logger.debug(message)
    
    def log_stage(self, stage_name):
        self.info(f"=== STARTING STAGE: {stage_name} ===")
    
    def log_completion(self, stage_name):
        self.info(f"=== COMPLETED STAGE: {stage_name} ===")
    
    def log_failure(self, stage_name, error):
        self.error(f"=== FAILED STAGE: {stage_name} - {str(error)} ===")