-- Run this once in Supabase: Project -> SQL Editor -> New query -> Run.

create table if not exists job_applications (
  id bigint generated always as identity primary key,
  recruiter_email text default '',
  recruiter_name text default '',
  company text default '',
  job_title text default '',
  location text default '',
  job_match_score int default 0,
  priority text default '',
  email_subject text default '',
  email_body text default '',
  email_status text default 'Not Sent',
  sent_date date,
  follow_up_date date,
  source_screenshot text default '',
  created_at timestamptz default now()
);

-- Also create a Storage bucket named "screenshots" (or whatever you set
-- SUPABASE_BUCKET to in .env): Project -> Storage -> New bucket.
-- Upload your recruiter/job screenshots into that bucket - the app reads
-- from it, and deletes each file once it's been extracted and saved above.
