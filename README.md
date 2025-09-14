# CloudWerx ADDM Tool

**Application Discovery and Dependency Mapping Tool**

A web-based tool that automatically discovers and maps AWS infrastructure dependencies using graph database technology.

## What This Tool Does

This application scans your AWS environment and creates a comprehensive map of all resources and their relationships. It's like having an X-ray vision of your cloud infrastructure - showing not just what you have, but how everything connects.

**Key Capabilities:**
- Discovers all AWS resources across services (EC2, S3, RDS, Lambda, VPC, IAM, etc.)
- Maps relationships and dependencies between resources
- Stores data in a Neo4j graph database for visual exploration
- Generates Excel reports organized by resource type
- Provides a web interface for easy operation

## How It Works

The tool uses a multi-stage pipeline:

1. **Authentication**: Securely handles AWS credentials via Google Cloud Storage
2. **Discovery**: Uses Netflix's Cartography tool to scan AWS APIs
3. **Storage**: Loads discovered data into a Neo4j graph database
4. **Processing**: Analyzes relationships and creates hierarchical mappings
5. **Export**: Generates downloadable CSV and Excel reports

## Architecture Flow

```
Web UI → Flask App → Docker Containers → Neo4j Database → Reports
    ↓         ↓            ↓               ↓            ↓
  Login  → Credentials → Cartography → Graph Data → Downloads
```

**Components:**
- **Flask Web App**: User interface and API endpoints
- **Neo4j Container**: Graph database for storing infrastructure data
- **Cartography Container**: AWS discovery and data collection
- **Google Cloud Storage**: Secure credential storage
- **Docker**: Container orchestration for isolated execution

## Prerequisites

- Python 3.9 or higher
- Docker and Docker Compose
- Google Cloud SDK with authentication
- AWS account with appropriate read permissions

## Installation and Setup

### 1. Clone the Repository
```bash
git clone <repository-url>
cd cloudwerx-addm-tool
```

### 2. Set Up Python Environment
```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
# On macOS/Linux:
source venv/bin/activate
# On Windows:
venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Configure Google Cloud
```bash
# Install Google Cloud SDK (if not already installed)
# Visit: https://cloud.google.com/sdk/docs/install

# Authenticate with Google Cloud
gcloud auth application-default login

# Create GCS bucket for credential storage
gsutil mb gs://addm-app

# Verify bucket creation
gsutil ls gs://addm-app
```

### 4. Set Up Docker
```bash
# Ensure Docker is running
docker --version
docker-compose --version

# Pull required images (optional, will auto-pull)
docker pull neo4j:4.4-community
docker pull ghcr.io/cartography-cncf/cartography:latest
```

### 5. Add Your Logo (Optional)
```bash
# Place your company logo in the project root
cp /path/to/your/logo.png ./logo.png
```

### 6. Configure AWS Permissions

Your AWS credentials need the following permissions:
- SecurityAudit policy (AWS managed)
- Or custom policy with read access to all AWS services

## Running the Application

### Start the Application
```bash
# Make sure you're in the project directory
cd cloudwerx-addm-tool

# Activate virtual environment (if not already active)
source venv/bin/activate

# Start the Flask application
python app.py
```

The application will start on `http://localhost:5000`

### Using the Web Interface

1. **Access the Tool**: Open `http://localhost:5000` in your browser

2. **Authentication**: 
   - Create an account with any username/password
   - This is for session management (not production-grade security)

3. **Configure AWS Settings**:
   - Enter your AWS account details
   - Provide AWS access keys (temporary or permanent)
   - Set a Neo4j database password

4. **Run Analysis**:
   - Click "Run Complete Analysis"
   - Wait 15-30 minutes for completion (depends on AWS account size)
   - Monitor progress in the interface

5. **Access Results**:
   - Download CSV/Excel reports from Downloads tab
   - Explore graph database via Neo4j Browser tab
   - View infrastructure relationships visually

## Output Files

After successful analysis, you'll receive:

- **export0.csv**: Raw nodes data (all AWS resources)
- **export1.csv**: Raw relationships data (resource connections)
- **neo4j_nodes_by_labels.xlsx**: Resources organized by type (EC2, S3, etc.)
- **node_to_root_mapping.xlsx**: Hierarchical account mappings

## File Structure

```
Cartography-loc/
├── app.py                  # Main Flask application
├── ui.html                 # Web interface
├── logo.png               # Company logo (add your own)
├── requirements.txt       # Python dependencies
├── services/
│   ├── gcs_manager.py     # Google Cloud Storage operations
│   ├── docker_manager.py  # Docker container management
│   ├── neo4j_exporter.py  # Database export functionality
│   └── data_processor.py  # Report generation
├── utils/
│   ├── config.py          # Configuration management
│   └── logger.py          # Centralized logging
└── tmp/                   # Temporary files (auto-created: output, cred, .env etc & deleted after use)
```

## Troubleshooting

### Common Issues

**Docker Permission Errors:**
```bash
# Add user to docker group (Linux)
sudo usermod -aG docker $USER
# Log out and back in
```

**GCS Authentication Failures:**
```bash
# Re-authenticate with Google Cloud
gcloud auth application-default login
# Verify bucket access
gsutil ls gs://addm-app
```

**Neo4j Connection Issues:**
```bash
# Check if port 7474 is available
lsof -i :7474
# Wait 2-3 minutes after pipeline completes before accessing Neo4j
```

**AWS Authentication Errors:**
- Verify access keys are correct and active
- Ensure SecurityAudit permissions are attached
- Check if session token is expired (if using temporary credentials, session token is not mandatory, depends on your cloud provider's account security setting)

### Viewing Logs

Monitor application logs in the terminal where you started Flask:
```bash
# Flask application logs appear in the console
# Check Docker container logs:
docker logs cartography-neo4j
docker logs cartography-account1
```

## Development Notes

- The tool uses Cartography for AWS discovery
- Neo4j 4.4 Community Edition for graph storage
- Flask for web interface and API endpoints
- Docker for containerized execution environments

## Security Considerations

- This is a development/internal tool
- Implement proper authentication for production use(Only with the suggested permissions or roles)
- Credentials are temporarily stored in GCS and cleaned up after use(permanently deleted)
- Use IAM roles instead of access keys when possible

## Stopping the Application

```bash
# Stop Flask app: Ctrl+C in terminal

# Clean up all resources using the web interface:
# Navigate to "Session & Settings" → "End Session & Cleanup"

# Or manually cleanup:
python -c "
import subprocess
subprocess.run(['docker', 'stop', 'cartography-neo4j', 'cartography-account1'], check=False)
subprocess.run(['docker', 'rm', 'cartography-neo4j', 'cartography-account1'], check=False)
subprocess.run(['docker', 'network', 'rm', 'cartography-network'], check=False)
subprocess.run(['docker', 'volume', 'rm', 'neo4j-data'], check=False)
"
```
## For questions or improvements, feel free to open an issue or PR\
