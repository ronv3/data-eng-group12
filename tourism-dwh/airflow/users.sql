CREATE USER IF NOT EXISTS service_openmetadata IDENTIFIED WITH sha256_password BY 'omd_very_secret_password';
GRANT ALL PRIVILEGES ON airflow_db.* TO 'openmetadata_user'@'%';
FLUSH PRIVILEGES;