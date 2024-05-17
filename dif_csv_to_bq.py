import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import WriteToBigQuery
from apache_beam.io import filesystems
import logging
import yaml
import argparse
# Function to process each row
def process(element, schema_fields):
    # Split the row by the delimiter '||' 
    colm = element.split('||')
    
    # Convert the split values into a dictionary matching the schema
    if len(colm) != len(schema_fields):
        return None  # Filter out rows that do not match the schema length

    return {schema_fields[i]['name']: colm[i].strip() for i in range(len(schema_fields))}


# Function to read schema from YAML configuration file
def read_schema_from_yaml(config_file):
    with filesystems.FileSystems.open(config_file) as file:
        config = yaml.safe_load(file)
    return config['schema']

# Set up the Beam pipeline

def run_pipeline(argv=None):
    # Create an argument parser
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',dest='input', required=True  , help='GCS file path for input CSV file')
    parser.add_argument('--config',dest='config_file', required=True , help='ymal file path in GCS')
    parser.add_argument('--output',dest='output', required=True , help='BigQuery table in the format dataset.table')
    
    # Parse the command-line arguments
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # Create PipelineOptions using the parsed arguments
    pipeline_options = PipelineOptions(pipeline_args)
    

    schema = read_schema_from_yaml(known_args.config_file)
     # Extract schema fields
    schema_fields = schema['fields']
    # Create the pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        (
        p
        | 'Read CSV' >> beam.io.ReadFromText(known_args.input, skip_header_lines=1)
        | 'Process Rows' >> beam.Map(process,  schema_fields=schema_fields)
        | 'Filter out null values' >> beam.Filter(lambda row: row is not None)
        | 'Write to BigQuery' >> WriteToBigQuery(
            table=known_args.output,
            schema = schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )
    )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run_pipeline()
