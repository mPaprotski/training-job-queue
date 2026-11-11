-- Admission counts only live jobs, independent of terminal history size.
CREATE INDEX jobs_outstanding_idx ON jobs (id) WHERE state IN ('queued', 'running');
