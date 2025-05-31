-- Create the test database
CREATE DATABASE fao;

-- Connect to the new database to grant schema permissions
\c fao

-- Grant usage and create permissions on public schema
GRANT USAGE, CREATE ON SCHEMA public TO mickey;

-- Grant all privileges on all tables in public schema (current and future)
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO mickey;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO mickey;

-- Grant privileges on future tables/sequences created in public schema
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO mickey;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON SEQUENCES TO mickey;