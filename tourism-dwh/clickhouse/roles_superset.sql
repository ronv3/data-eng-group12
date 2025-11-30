-- Superset must access gold layer only

-- Create a role for Superset
CREATE ROLE IF NOT EXISTS role_superset;

-- Create service user for Superset
CREATE USER IF NOT EXISTS service_superset
IDENTIFIED BY 'superset_pass';

-- Assign the role to user
GRANT role_superset TO service_superset;

-- Allow Superset to read ONLY gold schema
GRANT SELECT ON default_gold.* TO role_superset;
