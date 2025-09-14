import pandas as pd
import re
import json
from collections import defaultdict, deque
import os
from utils.logger import PipelineLogger

class DataProcessor:
    def __init__(self, nodes_csv_path, relationships_csv_path):
        self.nodes_csv_path = nodes_csv_path
        self.relationships_csv_path = relationships_csv_path
        self.nodes_df = None
        self.relationships_df = None
        self.parsed_nodes = None
        self.node_to_root = {}
        self.logger = PipelineLogger("DataProcessor")

    def load_data(self):
        """Load CSV data into DataFrames"""
        self.logger.log_stage("Load CSV Data")
        
        try:
            self.nodes_df = pd.read_csv(self.nodes_csv_path)
            self.relationships_df = pd.read_csv(self.relationships_csv_path)
            
            self.logger.info(f"Loaded {len(self.nodes_df)} nodes and {len(self.relationships_df)} relationships")
            self.logger.log_completion("Load CSV Data")
            
        except Exception as e:
            self.logger.log_failure("Load CSV Data", e)
            raise

    def parse_properties(self, prop_string):
        """Parse Neo4j properties string to dictionary"""
        if pd.isna(prop_string) or prop_string.strip() == '{}':
            return {}
        try:
            cleaned = prop_string.strip()
            cleaned = re.sub(r'(\w+):', r'"\1":', cleaned)
            cleaned = re.sub(r':\s*([^",\{\}\[\]]+?)(?=,|\})',
                             lambda m: f': "{m.group(1).strip()}"', cleaned)
            cleaned = cleaned.replace(': "true"', ': true')
            cleaned = cleaned.replace(': "false"', ': false')
            cleaned = cleaned.replace(': "null"', ': null')
            cleaned = cleaned.replace(': ""', ': null')
            return json.loads(cleaned)
        except:
            return {"raw_properties": prop_string}

    def parse_labels(self, labels_string):
        """Parse Neo4j labels string to list"""
        if pd.isna(labels_string):
            return ['Unknown']
        labels_string = labels_string.strip('[]')
        return [label.strip() for label in labels_string.split(',')]

    def process_nodes(self):
        """Process nodes and expand by labels"""
        self.logger.log_stage("Process Nodes")
        
        try:
            processed_nodes = []
            for idx, row in self.nodes_df.iterrows():
                node_id = row['id(n)']
                labels = self.parse_labels(row['labels(n)'])
                properties = self.parse_properties(row['properties(n)'])
                properties['node_id'] = node_id
                
                for label in labels:
                    processed_nodes.append({
                        'node_id': node_id,
                        'primary_label': label,
                        'all_labels': '|'.join(labels),
                        **properties
                    })
            
            self.parsed_nodes = pd.DataFrame(processed_nodes)
            self.logger.info(f"Processed {len(self.parsed_nodes)} node-label combinations")
            self.logger.log_completion("Process Nodes")
            
        except Exception as e:
            self.logger.log_failure("Process Nodes", e)
            raise

    def find_root_nodes(self):
        """Find nodes that have no incoming relationships (root nodes)"""
        target_nodes = set(self.relationships_df['target_id'].unique())
        all_nodes = set(self.nodes_df['id(n)'].unique())
        return all_nodes - target_nodes

    def build_parent_map(self):
        """Build mapping of child nodes to their parents"""
        parent_map = defaultdict(list)
        for _, row in self.relationships_df.iterrows():
            parent_map[row['target_id']].append(row['source_id'])
        return parent_map

    def find_node_roots(self):
        """Find root node for each node in the graph"""
        self.logger.log_stage("Find Node Roots")
        
        try:
            root_nodes = self.find_root_nodes()
            parent_map = self.build_parent_map()
            memo = {}
            
            self.logger.info(f"Found {len(root_nodes)} root nodes")
            
            def find_root(n):
                if n in memo:
                    return memo[n]
                if n in root_nodes:
                    memo[n] = n
                    return n
                
                visited = set()
                queue = deque([n])
                
                while queue:
                    curr = queue.popleft()
                    if curr in visited:
                        continue
                    visited.add(curr)
                    
                    if curr in root_nodes:
                        for v in visited:
                            memo[v] = curr
                        return curr
                    
                    queue.extend(parent_map.get(curr, []))
                
                memo[n] = n
                return n

            for node_id in set(self.nodes_df['id(n)'].unique()):
                self.node_to_root[node_id] = find_root(node_id)
            
            self.logger.log_completion("Find Node Roots")
            
        except Exception as e:
            self.logger.log_failure("Find Node Roots", e)
            raise

    def create_excel_by_labels(self, output_path):
        """Create Excel file with nodes organized by labels"""
        self.logger.log_stage("Create Nodes by Labels Excel")
        
        try:
            # Create node-to-name mapping
            node_to_name = {}
            for _, row in self.parsed_nodes.iterrows():
                node_id = row['node_id']
                if node_id not in node_to_name:
                    name = row.get('name') if pd.notna(row.get('name', None)) else None
                    if not name and 'arn' in row:
                        arn = str(row['arn'])
                        if '/' in arn:
                            name = arn.split('/')[-1]
                        elif ':' in arn:
                            name = arn.split(':')[-1]
                    node_to_name[node_id] = name if name else f"Node_{node_id}"

            # Group by labels and create sheets
            label_groups = self.parsed_nodes.groupby('primary_label')
            
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                for label, group in label_groups:
                    sheet_name = re.sub(r'[^\w\-_\. ]', '', str(label))[:31] or "Unknown_Label"
                    
                    # Prepare output DataFrame
                    exclude_cols = ['primary_label', 'all_labels']
                    output_df = group.drop(columns=exclude_cols).copy()
                    
                    # Add root and account information
                    output_df['root_id'] = output_df['node_id'].map(self.node_to_root)
                    output_df['account_name'] = output_df['root_id'].map(node_to_name)
                    
                    # Keep only non-null columns
                    non_null_cols = [col for col in output_df.columns 
                                   if output_df[col].notna().any() or col in ['node_id', 'root_id', 'account_name']]
                    output_df = output_df[non_null_cols]
                    
                    # Reorder columns with priority columns first
                    priority_cols = ['node_id', 'root_id', 'account_name']
                    other_cols = [c for c in output_df.columns if c not in priority_cols]
                    output_df = output_df[priority_cols + other_cols]
                    
                    # Write to Excel
                    output_df.to_excel(writer, sheet_name=sheet_name, index=False)
                    self.logger.info(f"Created sheet '{sheet_name}' with {len(output_df)} rows")
            
            self.logger.log_completion("Create Nodes by Labels Excel")
            
        except Exception as e:
            self.logger.log_failure("Create Nodes by Labels Excel", e)
            raise

    def create_node_root_mapping_excel(self, output_path):
        """Create Excel file with node-to-root mappings"""
        self.logger.log_stage("Create Node Root Mapping Excel")
        
        try:
            # Create mapping data (exclude self-mappings)
            mapping_data = [
                {'node_id': n, 'root_id': r} 
                for n, r in self.node_to_root.items() 
                if n != r
            ]
            mapping_df = pd.DataFrame(mapping_data)
            
            # Add label information for nodes and roots
            node_labels = self.parsed_nodes[['node_id', 'primary_label', 'all_labels']].rename(
                columns={'primary_label': 'node_label', 'all_labels': 'node_all_labels'}
            )
            root_labels = self.parsed_nodes[['node_id', 'primary_label', 'all_labels']].rename(
                columns={'node_id': 'root_id', 'primary_label': 'root_label', 'all_labels': 'root_all_labels'}
            )
            
            # Merge label information
            result_df = mapping_df.merge(node_labels, on='node_id', how='left') \
                                  .merge(root_labels, on='root_id', how='left')
            
            # Reorder columns
            result_df = result_df[['node_id', 'node_label', 'root_id', 'root_label', 
                                  'node_all_labels', 'root_all_labels']]
            
            # Write to Excel
            result_df.to_excel(output_path, index=False)
            self.logger.info(f"Created node-root mapping with {len(result_df)} rows")
            self.logger.log_completion("Create Node Root Mapping Excel")
            
        except Exception as e:
            self.logger.log_failure("Create Node Root Mapping Excel", e)
            raise

    def process_all_data(self, output_dir):
        """Complete data processing pipeline"""
        self.logger.log_stage("Complete Data Processing")
        
        try:
            # Ensure output directory exists
            os.makedirs(output_dir, exist_ok=True)
            
            # Connect to Neo4j
            self.connect()
            
            # Export CSVs
            csv_files = {}
            nodes_csv = os.path.join(output_dir, "export0.csv")
            relationships_csv = os.path.join(output_dir, "export1.csv")
            
            with self.driver.session() as session:
                session.execute_read(self.export_nodes, nodes_csv)
                session.execute_read(self.export_relationships, relationships_csv)
            
            csv_files["nodes_csv"] = nodes_csv
            csv_files["relationships_csv"] = relationships_csv
            
            # Process data
            self.load_data()
            self.process_nodes()
            self.find_node_roots()
            
            # Generate Excel files
            excel_files = {}
            nodes_excel = os.path.join(output_dir, "neo4j_nodes_by_labels.xlsx")
            mapping_excel = os.path.join(output_dir, "node_to_root_mapping.xlsx")
            
            self.create_excel_by_labels(nodes_excel)
            self.create_node_root_mapping_excel(mapping_excel)
            
            excel_files["nodes_excel"] = nodes_excel
            excel_files["mapping_excel"] = mapping_excel
            
            # Disconnect from Neo4j
            self.disconnect()
            
            self.logger.log_completion("Complete Data Processing")
            
            return {
                **csv_files,
                **excel_files
            }
            
        except Exception as e:
            self.logger.log_failure("Complete Data Processing", e)
            if self.driver:
                self.disconnect()
            raise