from metadata.generated.schema.entity.services.connections.database.clickhouseConnection import ClickhouseConnection
from metadata.generated.schema.entity.services.databaseService import DatabaseService, DatabaseServiceType
from metadata.generated.schema.metadataIngestion.workflow import OpenMetadataWorkflowConfig
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.ometa.client import REST

# OpenMetadata server configuration
server_config = {
    "hostPort": "http://localhost:8585",  # Replace with your OpenMetadata server URL
    "authProvider": "no-auth",           # Adjust if authentication is enabled
}

# ClickHouse connection configuration
clickhouse_connection = ClickhouseConnection(
    hostPort="clickhouse:8123",  # Replace with your ClickHouse host and port
    username="default",          # Replace with your ClickHouse username
    password="",                 # Replace with your ClickHouse password
    database="default"           # Replace with your ClickHouse database name
)

# Database service configuration
clickhouse_service = DatabaseService(
    name="clickhouse_service",               # Name of the service
    serviceType=DatabaseServiceType.Clickhouse,
    connection=clickhouse_connection,
    description="ClickHouse database service"  # Optional description
)

# Initialize OpenMetadata client
metadata = OpenMetadata(OpenMetadataWorkflowConfig(openMetadataServerConfig=server_config))

# Create or update the ClickHouse service
created_service = metadata.create_or_update(clickhouse_service)

# Output the result
print(f"Created ClickHouse service: {created_service.name}")