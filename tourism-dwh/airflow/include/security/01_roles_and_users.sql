-- Roles
CREATE ROLE IF NOT EXISTS analyst_full;
CREATE ROLE IF NOT EXISTS analyst_limited;

-- Users (demo passwords; adjust if needed)
CREATE USER IF NOT EXISTS analyst_full_user
  IDENTIFIED BY 'full123';
CREATE USER IF NOT EXISTS analyst_limited_user
  IDENTIFIED BY 'limited123';

-- Attach roles
GRANT analyst_full   TO analyst_full_user;
GRANT analyst_limited TO analyst_limited_user;

-- Make roles the default for each user
SET DEFAULT ROLE analyst_full    TO analyst_full_user;
SET DEFAULT ROLE analyst_limited TO analyst_limited_user;
