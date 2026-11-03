-- Threads 0.7: Threads no longer asks Jobs for a title; a title is set by
-- command only. The title Job's state goes. A title Job still RUNNING when this
-- migration runs is forgotten: Threads neither ends nor cancels it at Jobs, and
-- its runner's title operations no longer exist. The `TitleChanged` facts it
-- recorded (`by: TITLE_JOB`) stay as they were.
DROP TABLE threads.title_job;
