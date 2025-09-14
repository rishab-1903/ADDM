from neo4j import GraphDatabase
import csv
import os
from utils.logger import PipelineLogger

class Neo4jExporter:
    def __init__(self, neo4j_uri, neo4j_user, neo4j_password):
        self.neo4j_uri = neo4j_uri
        self.neo4j_user = neo4j_user
        self.neo4j_password = neo4j_password
        self.driver = None
        self.logger = PipelineLogger("Neo4jExporter")
    
    def connect(self):
        """Establish connection to Neo4j"""
        try:
            self.driver = GraphDatabase.driver(
                self.neo4j_uri, 
                auth=(self.neo4j_user, self.neo4j_password)
            )
            self.logger.info(f"Connected to Neo4j at {self.neo4j_uri}")
        except Exception as e:
            self.logger.error(f"Failed to connect to Neo4j: {str(e)}")
            raise
    
    def disconnect(self):
        """Close Neo4j connection"""
        if self.driver:
            self.driver.close()
            self.logger.info("Disconnected from Neo4j")
    
    def export_nodes(self, tx, filename):
        """Export all nodes to CSV"""
        query = """
        MATCH (n)
        RETURN id(n), labels(n), properties(n)
        """
        
        with open(filename, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(["id(n)", "labels(n)", "properties(n)"])
            
            record_count = 0
            for record in tx.run(query):
                writer.writerow([
                    record["id(n)"],
                    record["labels(n)"],
                    record["properties(n)"]
                ])
                record_count += 1
            
            self.logger.info(f"Exported {record_count} nodes to {filename}")
    
    def export_relationships(self, tx, filename):
        """Export all relationships to CSV"""
        query = """
        MATCH (a)-[r]->(b)
        RETURN id(r) as relationship_id,
               type(r) as relationship_type,
               properties(r) as relationship_properties,
               id(a) as source_id,
               id(b) as target_id
        """
        
        with open(filename, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow([
                "relationship_id", "relationship_type", "relationship_properties", 
                "source_id", "target_id"
            ])
            
            record_count = 0
            for record in tx.run(query):
                writer.writerow([
                    record["relationship_id"],
                    record["relationship_type"],
                    record["relationship_properties"],
                    record["source_id"],
                    record["target_id"]
                ])
                record_count += 1
            
            self.logger.info(f"Exported {record_count} relationships to {filename}")
    
    def export_data_to_csv(self, output_dir):
        """Export Neo4j data to CSV files"""
        self.logger.log_stage("Export Neo4j Data")
        
        try:
            if not self.driver:
                self.connect()
            
            nodes_file = os.path.join(output_dir, "export0.csv")
            relationships_file = os.path.join(output_dir, "export1.csv")
            
            with self.driver.session() as session:
                session.execute_read(self.export_nodes, nodes_file)
                session.execute_read(self.export_relationships, relationships_file)
            
            self.logger.log_completion("Export Neo4j Data")
            return {
                "nodes_csv": nodes_file,
                "relationships_csv": relationships_file
            }
            
        except Exception as e:
            self.logger.log_failure("Export Neo4j Data", e)
            raise