-- Local development credentials only. Run as the cluster administrator.
-- Also supports the original volume: connect as its existing admin `queue`.
\set ON_ERROR_STOP on
BEGIN;
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'queue_migrator') THEN
        CREATE ROLE queue_migrator;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'queue_runtime') THEN
        CREATE ROLE queue_runtime;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'queue_tools') THEN
        CREATE ROLE queue_tools;
    END IF;
END $$;
ALTER ROLE queue_migrator LOGIN PASSWORD 'queue_migrator' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
ALTER ROLE queue_runtime LOGIN PASSWORD 'queue_runtime' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
ALTER ROLE queue_tools LOGIN PASSWORD 'queue_tools' NOSUPERUSER CREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
ALTER DATABASE queue OWNER TO queue_migrator;
REVOKE ALL ON DATABASE queue FROM PUBLIC;
GRANT CONNECT ON DATABASE queue TO queue_runtime, queue_tools;
ALTER SCHEMA public OWNER TO queue_migrator;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
-- Transfer only the known application tables when upgrading an existing volume.
DO $$
DECLARE table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY['jobs', 'job_attempts', 'job_effects', 'schema_migrations'] LOOP
        IF to_regclass('public.' || table_name) IS NOT NULL THEN
            EXECUTE format('ALTER TABLE public.%I OWNER TO queue_migrator', table_name);
        END IF;
    END LOOP;
END $$;
COMMIT;
