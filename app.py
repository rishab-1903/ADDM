from flask import Flask, request, jsonify, send_from_directory, abort, render_template_string
import os
import time
import subprocess
from services.gcs_manager import GCSManager
from services.docker_manager import DockerManager
from services.neo4j_exporter import Neo4jExporter
from services.data_processor import DataProcessor
from utils.config import Config
from utils.logger import PipelineLogger

app = Flask(__name__)

# Initialize services
gcs_manager = GCSManager()
docker_manager = DockerManager()
logger = PipelineLogger("MainApp")

def get_ui_html():
    # You'll need to save the HTML content to a file called 'ui.html'
    try:
        with open('ui.html', 'r') as f:
            return f.read()
    except FileNotFoundError:
        return """
        <html><body>
        <h1>UI file not found</h1>
        <p>Please create ui.html file with the UI content</p>
        </body></html>
        """
    
@app.route('/')
def home():
    """Serve the main UI"""
    return get_ui_html()

@app.route('/end-session', methods=['POST'])
def end_session():
    """Clean up all Docker resources and delete credentials from GCS"""
    try:
        logger.log_stage("End Session Cleanup")
        
        # Stop and remove containers
        containers = ["cartography-neo4j", "cartography-account1"]
        for container in containers:
            try:
                subprocess.run(["docker", "stop", container], capture_output=True, check=False)
                subprocess.run(["docker", "rm", container], capture_output=True, check=False)
                logger.info(f"Removed container: {container}")
            except Exception as e:
                logger.warning(f"Failed to remove container {container}: {str(e)}")
        
        # Remove networks
        try:
            subprocess.run(["docker", "network", "rm", "cartography-network"], capture_output=True, check=False)
            logger.info("Removed network: cartography-network")
        except Exception as e:
            logger.warning(f"Failed to remove network: {str(e)}")
        
        # Remove volumes
        try:
            subprocess.run(["docker", "volume", "rm", "neo4j-data"], capture_output=True, check=False)
            logger.info("Removed volume: neo4j-data")
        except Exception as e:
            logger.warning(f"Failed to remove volume: {str(e)}")
        
        # Delete credentials from GCS (keep .env and output files)
        try:
            creds_blob = gcs_manager.bucket.blob("cred-aws/credentials")
            if creds_blob.exists():
                creds_blob.delete()
                logger.info("Deleted credentials from GCS")
        except Exception as e:
            logger.warning(f"Failed to delete credentials from GCS: {str(e)}")
        
        # Clean up local temp files
        try:
            subprocess.run(["rm", "-rf", "./tmp/aws-creds"], check=False)
            logger.info("Cleaned up local temp files")
        except Exception as e:
            logger.warning(f"Failed to clean temp files: {str(e)}")
        
        logger.log_completion("End Session Cleanup")
        
        return jsonify({
            "status": "success",
            "message": "Session ended successfully. All Docker resources cleaned up and credentials deleted."
        }), 200
        
    except Exception as e:
        logger.log_failure("End Session Cleanup", e)
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/logo.png')
def serve_logo():
    """Serve the logo file"""
    try:
        return send_from_directory('.', 'logo.png')
    except FileNotFoundError:
        abort(404)
        
@app.route('/upload-config', methods=['POST'])
def upload_config():
    """Upload AWS credentials and environment configuration to GCS"""
    try:
        data = request.json
        
        # Validate required fields
        required_fields = ["AWS_PROFILE", "AWS_DEFAULT_REGION", "NEO4J_USER", 
                          "NEO4J_PASSWORD", "NEO4J_URI", "credentials"]
        
        missing_fields = [field for field in required_fields if not data.get(field)]
        if missing_fields:
            return jsonify({
                "status": "error", 
                "message": f"Missing required fields: {', '.join(missing_fields)}"
            }), 400
        
        # Create config from request data
        config = Config.from_request_data(data)
        config.validate_required_config()
        
        # Upload to GCS
        credentials_content = data.get("credentials")
        uploaded_paths = gcs_manager.upload_credentials_and_env(
            credentials_content, 
            {
                "NEO4J_URI": config.NEO4J_URI,
                "NEO4J_USER": config.NEO4J_USER, 
                "NEO4J_PASSWORD": config.NEO4J_PASSWORD,
                "AWS_PROFILE": config.AWS_PROFILE,
                "AWS_DEFAULT_REGION": config.AWS_DEFAULT_REGION
            }
        )
        
        return jsonify({
            "status": "success",
            "message": "Configuration uploaded to GCS successfully",
            **uploaded_paths
        }), 200
        
    except Exception as e:
        logger.log_failure("Upload Config", e)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/start-analysis', methods=['POST'])
def start_analysis():
    """Start Neo4j and Cartography containers for AWS data collection"""
    try:
        logger.log_stage("Start Analysis Pipeline")
        
        # Get request data to access configuration
        data = request.json or {}
        
        # Create config from request data if provided, otherwise use environment
        if data:
            config = Config.from_request_data(data)
        else:
            config = Config()
        
        # Debug log
        logger.info(f"Using config - NEO4J_URI: {config.NEO4J_URI}, NEO4J_PASSWORD: {'***' if config.NEO4J_PASSWORD else 'None'}")
        
        # Setup Docker environment
        docker_manager.setup_docker_environment()
        
        # Start Neo4j container
        docker_manager.start_neo4j_container()
        
        # Start Cartography container
        docker_manager.start_cartography_container()
        
        logger.log_completion("Start Analysis Pipeline")
        
        return jsonify({
            "status": "success",
            "message": "Analysis containers started and AWS data collection completed"
        }), 200
        
    except Exception as e:
        logger.log_failure("Start Analysis Pipeline", e)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/process-data', methods=['POST'])
def process_data():
    """Export Neo4j data and generate Excel reports"""
    try:
        logger.log_stage("Process Data and Generate Reports")
        
        # Load config from environment
        config = Config()
        
        # Ensure output directory exists
        os.makedirs(config.OUTPUT_DIR, exist_ok=True)
        
        # Export data from Neo4j and process
        neo4j_exporter = Neo4jExporter(
            config.NEO4J_URI,
            config.NEO4J_USER, 
            config.NEO4J_PASSWORD
        )
        
        # Export Neo4j data to CSVs and generate Excel files
        output_files = neo4j_exporter.export_data_to_csv(config.OUTPUT_DIR)
        
        # Process the exported data
        data_processor = DataProcessor(
            output_files["nodes_csv"],
            output_files["relationships_csv"]
        )
        
        # Load data and process
        data_processor.load_data()
        data_processor.process_nodes()
        data_processor.find_node_roots()
        
        # Generate Excel files
        nodes_excel = os.path.join(config.OUTPUT_DIR, "neo4j_nodes_by_labels.xlsx")
        mapping_excel = os.path.join(config.OUTPUT_DIR, "node_to_root_mapping.xlsx")
        
        data_processor.create_excel_by_labels(nodes_excel)
        data_processor.create_node_root_mapping_excel(mapping_excel)
        
        # Upload all output files to GCS
        uploaded_files = gcs_manager.upload_output_files(config.OUTPUT_DIR)
        
        logger.log_completion("Process Data and Generate Reports")
        
        return jsonify({
            "status": "success",
            "message": "Data processing completed and files uploaded to GCS",
            "output_files": uploaded_files
        }), 200
        
    except Exception as e:
        logger.log_failure("Process Data and Generate Reports", e)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/run-complete-pipeline', methods=['POST'])
def run_complete_pipeline():
    """Run the entire pipeline from start to finish"""
    try:
        logger.log_stage("Complete Pipeline Execution")
        
        data = request.json
        if not data:
            return jsonify({"status": "error", "message": "No data provided"}), 400
        
        # Step 1: Upload configuration
        logger.info("Step 1: Uploading configuration...")
        try:
            # Create config from request data
            config = Config.from_request_data(data)
            config.validate_required_config()
            
            # Upload to GCS
            credentials_content = data.get("credentials")
            if not credentials_content:
                return jsonify({"status": "error", "message": "Missing credentials"}), 400
                
            uploaded_paths = gcs_manager.upload_credentials_and_env(
                credentials_content, 
                {
                    "NEO4J_URI": config.NEO4J_URI,
                    "NEO4J_USER": config.NEO4J_USER, 
                    "NEO4J_PASSWORD": config.NEO4J_PASSWORD,
                    "AWS_PROFILE": config.AWS_PROFILE,
                    "AWS_DEFAULT_REGION": config.AWS_DEFAULT_REGION
                }
            )
            logger.info("Configuration uploaded successfully")
        except Exception as e:
            logger.error(f"Failed to upload configuration: {str(e)}")
            return jsonify({"status": "error", "message": f"Config upload failed: {str(e)}"}), 500
        
        # Step 2: Start analysis containers
        logger.info("Step 2: Starting analysis containers...")
        try:
            # Store config in app context for docker manager to use
            app.config['PIPELINE_CONFIG'] = config.__dict__
            
            # Setup Docker environment
            docker_manager.setup_docker_environment()
            
            # Start Neo4j container
            docker_manager.start_neo4j_container()
            
            # Start Cartography container  
            docker_manager.start_cartography_container()
            
            logger.info("Analysis containers started successfully")
        except Exception as e:
            logger.error(f"Failed to start analysis: {str(e)}")
            return jsonify({"status": "error", "message": f"Analysis start failed: {str(e)}"}), 500
        
        # Step 3: Process data and generate reports
        logger.info("Step 3: Processing data and generating reports...")
        try:
            # Ensure output directory exists
            os.makedirs(config.OUTPUT_DIR, exist_ok=True)
            logger.info("/tmp created")

            if "cartography-neo4j" in config.NEO4J_URI:
                config.NEO4J_URI = config.NEO4J_URI.replace("cartography-neo4j", "localhost")
            # Export data from Neo4j and process
            neo4j_exporter = Neo4jExporter(
                config.NEO4J_URI,
                config.NEO4J_USER, 
                config.NEO4J_PASSWORD
            )
            
            # Export Neo4j data to CSVs
            output_files = neo4j_exporter.export_data_to_csv(config.OUTPUT_DIR)
            
            # Process the exported data
            data_processor = DataProcessor(
                output_files["nodes_csv"],
                output_files["relationships_csv"]
            )
            
            # Load data and process
            data_processor.load_data()
            data_processor.process_nodes()
            data_processor.find_node_roots()
            
            # Generate Excel files
            nodes_excel = os.path.join(config.OUTPUT_DIR, "neo4j_nodes_by_labels.xlsx")
            mapping_excel = os.path.join(config.OUTPUT_DIR, "node_to_root_mapping.xlsx")
            
            data_processor.create_excel_by_labels(nodes_excel)
            data_processor.create_node_root_mapping_excel(mapping_excel)
            
            # Upload all output files to GCS
            uploaded_files = gcs_manager.upload_output_files(config.OUTPUT_DIR)
            
            logger.info("Data processing completed successfully")
        except Exception as e:
            logger.error(f"Failed to process data: {str(e)}")
            return jsonify({"status": "error", "message": f"Data processing failed: {str(e)}"}), 500
        
        logger.log_completion("Complete Pipeline Execution")
        
        return jsonify({
            "status": "success",
            "message": "Complete pipeline executed successfully",
            "details": "AWS data collected, processed, and Excel reports generated",
            "output_files": uploaded_files
        }), 200
        
    except Exception as e:
        logger.log_failure("Complete Pipeline Execution", e)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/status', methods=['GET'])
def get_status():
    """Get current status of the pipeline"""
    try:
        # Check if containers are running
        import subprocess
        
        status_info = {}
        
        # Check Neo4j container
        try:
            result = subprocess.run(
                ["docker", "inspect", "cartography-neo4j", "--format", "{{.State.Status}}"],
                capture_output=True, text=True, check=True
            )
            status_info["neo4j"] = result.stdout.strip()
        except:
            status_info["neo4j"] = "not_running"
        
        # Check Cartography container
        try:
            result = subprocess.run(
                ["docker", "inspect", "cartography-account1", "--format", "{{.State.Status}}"],
                capture_output=True, text=True, check=True
            )
            status_info["cartography"] = result.stdout.strip()
        except:
            status_info["cartography"] = "not_running"
        
        # Check if output files exist
        config = Config()
        output_files = ["export0.csv", "export1.csv", "neo4j_nodes_by_labels.xlsx", "node_to_root_mapping.xlsx"]
        files_status = {}
        
        for filename in output_files:
            file_path = os.path.join(config.OUTPUT_DIR, filename)
            files_status[filename] = os.path.exists(file_path)
        
        return jsonify({
            "status": "success",
            "containers": status_info,
            "output_files": files_status
        }), 200
        
    except Exception as e:
        logger.error(f"Failed to get status: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/download/<path:filename>")
def download_file(filename):
    """Download output files from local directory"""
    try:
        config = Config()
        return send_from_directory(config.OUTPUT_DIR, filename, as_attachment=True)
    except FileNotFoundError:
        abort(404)


if __name__ == '__main__':
    logger.info("Starting Flask application...")
    app.run(host='0.0.0.0', port=5000, debug=True)