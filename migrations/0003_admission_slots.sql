-- Preserve existing live jobs before enforcing one reservation per live job.
ALTER TABLE jobs ADD COLUMN admission_slot INTEGER;
WITH numbered AS (
    SELECT id, row_number() OVER (ORDER BY created_at, id)::integer AS slot
    FROM jobs WHERE state IN ('queued', 'running')
)
UPDATE jobs SET admission_slot = numbered.slot FROM numbered WHERE jobs.id = numbered.id;
ALTER TABLE jobs ADD CONSTRAINT jobs_admission_slot_positive
    CHECK (admission_slot IS NULL OR admission_slot > 0);
ALTER TABLE jobs ADD CONSTRAINT jobs_live_requires_slot
    CHECK (state NOT IN ('queued', 'running') OR admission_slot IS NOT NULL);
CREATE UNIQUE INDEX jobs_live_admission_slot_idx ON jobs (admission_slot)
    WHERE state IN ('queued', 'running');
