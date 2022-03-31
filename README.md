# mssql_calculate_next_run_time

This repo contains only one single SQL Server UDF that uses the data from [msdb].[dbo].[sysschedules] to calculate the next run time for any schedule.
It is a rather complex calculation and it is not yet fully tested.

You use it like this

```sql
SELECT
	dbo.[fn_schedule_next_run]([schedule_id]) as [next_run_date_and_time],
	[name],
	[enabled],
	[freq_type],
	[freq_interval],
	[freq_subday_type],
	[freq_subday_interval],
	[freq_relative_interval],
	[freq_recurrence_factor],
	[active_start_date],
	[active_end_date],
	[active_start_time],
	[active_end_time]
FROM 
	[msdb].[dbo].[sysschedules]

```
