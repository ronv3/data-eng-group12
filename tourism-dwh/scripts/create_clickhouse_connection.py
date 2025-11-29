import os
import uuid
from dotenv import load_dotenv
from metadata.generated.schema.entity.services.connections.database.clickhouseConnection import ClickhouseConnection
from metadata.generated.schema.entity.services.databaseService import DatabaseService, DatabaseServiceType
from metadata.generated.schema.metadataIngestion.workflow import OpenMetadataWorkflowConfig
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.ometa.client import REST

# ClickHouse connection configuration using .env properties
clickhouse_connection = ClickhouseConnection(
    hostPort="clickhouse:8123",
    username="default",
    password="default"
)

# Wrap ClickhouseConnection
clickhouse_connection_dict = {
    "config": clickhouse_connection  # Pass the ClickhouseConnection object
}

# Database service configuration
clickhouse_service = DatabaseService(
    id=str(uuid.uuid4()),
    name="clickhouse_service",               # Name of the service
    serviceType=DatabaseServiceType.Clickhouse,
    connection=clickhouse_connection_dict,  # Use the corrected connection dictionary
    description="ClickHouse database service"  # Optional description
)

#define OpenMetadataWorkflowConfig
workflow_config = OpenMetadataWorkflowConfig(
    source={
        "type": "DatabaseMetadata",
        "serviceName": "clickhouse_service",
        "sourceConfig": {
            "config": {
                "includeTables": True,
                "includeViews": True,
            }
        }
    },
    workflowConfig={
        "openMetadataServerConfig": {
            "hostPort": "http://localhost:8585",
            "authProvider": "openmetadata"
        }, # Add this field if required
    }
)

# Initialize OpenMetadata client
metadata = OpenMetadata(workflow_config)

# Create or update the ClickHouse service
created_service = metadata.create_or_update(clickhouse_service)

# Output the result
print(f"Created ClickHouse service: {created_service.name}")