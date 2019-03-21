SELECT
	*
from
	(
	SELECT
		count(*) as nb_thread_task
	from
		THREAD_HEADER
	LEFT OUTER join THREAD on
		THREAD_HEADER.link = THREAD.link
	where
		thread.link ISNULL
		or thread_header.lastpost > thread.mod_date ),
	(
	SELECT
		count(*) as nb_threads
	from
		THREAD),
	(
	SELECT
		count(*) as nb_atts
	from
		ATTACHEMENT),
	(
	SELECT
		count(*) as nb_atts_finish
	from
		ATTACHEMENT
	where
		not attachement.content ISNULL);