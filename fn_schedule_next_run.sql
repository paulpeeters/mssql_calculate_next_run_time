CREATE FUNCTION [fn_schedule_next_run]
(
	@schedule_id int
)
RETURNS DATETIME
AS
BEGIN

/*
This will calculate and return the next rundate/time for any schedule in [msdb].[dbo].[sysschedules].
If there is no next rundate/time, the function returns NULL

The fields in [msdb].[dbo].[sysschedules] are defined as follows 
(as seen on https://docs.microsoft.com/en-us/sql/relational-databases/system-tables/dbo-sysschedules-transact-sql?view=sql-server-ver15) 

freq_type - int

	How frequently a job runs for this schedule.

	1 = One time only
	4 = Daily
	8 = Weekly
	16 = Monthly
	32 = Monthly, relative to freq_interval
	64 = Runs when the SQL Server Agent service starts
	128 = Runs when the computer is idle

freq_interval - int	

	Days that the job is executed. Depends on the value of freq_type. 
	The default value is 0, which indicates that freq_interval is unused. 
	See the table below for the possible values and their effects.

freq_subday_type - int

	Units for the freq_subday_interval. The following are the possible 
	values and their descriptions.

	1 : At the specified time
	2 : Seconds
	4 : Minutes
	8 : Hours

freq_subday_interval - int

	Number of freq_subday_type periods to occur between each execution of the job.

freq_relative_interval - int	

	When freq_interval occurs in each month, if freq_type is 32 (monthly relative). 
	Can be one of the following values:

	0 = freq_relative_interval is unused
	1 = First
	2 = Second
	4 = Third
	8 = Fourth
	16 = Last

freq_recurrence_factor - int

	Number of weeks or months between the scheduled execution of a job. 
	freq_recurrence_factor is used only if freq_type is 8, 16, or 32. 
	If this column contains 0, freq_recurrence_factor is unused.

active_start_date - int

	Date on which execution of a job can begin. The date is formatted as YYYYMMDD. NULL indicates today's date.

active_end_date - int

	Date on which execution of a job can stop. The date is formatted YYYYMMDD.

active_start_time - int

	Time on any day between active_start_date and active_end_date that job begins executing. 
	Time is formatted HHMMSS, using a 24-hour clock.

active_end_time - int

	Time on any day between active_start_date and active_end_date that job stops executing. 
	Time is formatted HHMMSS, using a 24-hour clock.

Value of freq_type				Effect on freq_interval
-------------------------------------------------------
	1 (once)					freq_interval is unused (0)
	4 (daily)					Every freq_interval days
	8 (weekly)					freq_interval is one or more of the following:
									1 = Sunday
									2 = Monday
									4 = Tuesday
									8 = Wednesday
									16 = Thursday
									32 = Friday
									64 = Saturday
	16 (monthly)				On the freq_interval day of the month
	32 (monthly, relative)		freq_interval is one of the following:
									1 = Sunday
									2 = Monday
									3 = Tuesday
									4 = Wednesday
									5 = Thursday
									6 = Friday
									7 = Saturday
									8 = Day
									9 = Weekday
									10 = Weekend day
	64 (starts when SQL Server Agent service starts)	freq_interval is unused (0)
	128 (runs when computer is idle)					freq_interval is unused (0)

-------------------------------------------------------------
This is more or less the algorithm used to find the next time
-------------------------------------------------------------
if freq_subday_type is not in (0, 1, 2, 4, 8)
	set time = null
if now < active_start_time
	set time = active_start_time, today = true
if now > active_and_time
	set time = active_start_time, today = false
if now >= active_start_time and now <= active_and_time
	if freq_subday_type = 1
		set time = active_start_time, today = false
	if freq_subday_type = 2
		set seconds = seconds since active_start_time until now
		if (seconds mod freq_subday_interval) = 0
			set time = active_start_time + seconds, today = true
		else
			set time = active_start_time + freq_subday_interval * ((seconds div freq_subday_interval) + 1), today = 1
			if time > active_end_time
				set time = active_start_time, today = 0
	if freq_sub_type = 4
		set minutes = minutes since active_start_time until now
		if (minutes mod freq_subday_interval) = 0 
			if (seconds(now) <= seconds(active_start_time))
				set time = active_start_time + minutes, today = 1
			else
				set time = active_start_time + freq_subday_interval * ((minutes div freq_subday_interval) + 1), today = 1
				if (time > active_end_time)
					set time = active_start_time, today = 0
		else
			set time = active_start_time + freq_subday_interval * ((minutes div freq_subday_interval) + 1), today = 1
			if (time > active_end_time)
				set time = active_start_time, today = 0
	if freq_sub_type = 8
		set hours = hours since active_start_time until now 
		if (hours mod freq_subday_interval) = 0
			if (minutes(now) < minutes(active_start_time)) or (minutes(now) = minutes(active_start_time) and seconds(now) <= seconds(active_start_time))
				set time = active_start_time + hours, today = 1
			else
				set time = active_start_time + freq_subday_interval * ((hours div freq_subday_interval) + 1), today = 1
				if (time > active_end_time)
					set time = active_start_time, today = 1
		else
			set time = active_start_time + freq_subday_interval * ((hours div freq_subday_interval) + 1), today = 1
			if (time > active_end_time)
				set time = active_start_time, today = 1
				
-------------------------------------------------------------
This is more or less the algorithm used to find the next date
-------------------------------------------------------------
if today > active_end_date then return null
set date = active_start_date
if freq_type = 1 (once) => 
	if date == today then return date else return null
if freq_type = 4 (daily) => 
	if date == today then return date
	if freq_interval = 0 then return null
	while (date < today && date < active_end_date) date += freq_interval
	if date < active_end_date then return date else return null
if freq_type = 8 (weekly) =>
	while (date < today && date < active_end_date) date += 7 * freq_recurrence_factor
	while (date < active_end_date)
		set next_week_date = date + 8
		while (date < next_week_date && weekday(date) not in freq_interval) date += 1
		if (date < next_week_date) ==> return date
		date += next_week_date - 8 + 7 * freq_recurrence_factor
	end while
	return null
if freq_type = 16 (monthly) =>
	while (day(date) < freq_interval && date < active_end_dateday) date += 1
	while (date < today && date < active_end_date) set month(date) = month(date) + freq_recurrence_factor
	if (date < active_end_date) then return date else return null
if freq_type = 32 (monthly relative) =>
	while (date < active_end_date)
		set year = year(date)
		set month = month(date)
		set day = first/second/third/fourth/last (freq_relative_interval) mo/tu/we/th/fr/sa/su/day/weekday/weekendday (freq_interval) of month/year
		if (date == today) return date
		set month(date) = month(date) + freq_recurrence_factor ; set year accordingly
	end
	return null;

*/

	DECLARE
		@freq_type int,
		@freq_interval int,
		@freq_subday_type int,
		@freq_subday_interval int,
		@freq_relative_interval int,
		@freq_recurrence_factor int,
		@active_start_date int,
		@active_end_date int,
		@active_start_time int,
		@active_end_time int;

	SELECT
		@freq_type = [freq_type],
		@freq_interval = [freq_interval],
		@freq_subday_type = [freq_subday_type],
		@freq_subday_interval = [freq_subday_interval],
		@freq_relative_interval = [freq_relative_interval],
		@freq_recurrence_factor = [freq_recurrence_factor],
		@active_start_date = [active_start_date],
		@active_end_date = [active_end_date],
		@active_start_time = [active_start_time],
		@active_end_time = [active_end_time]
	FROM 
		[msdb].[dbo].[sysschedules]
	WHERE
		[schedule_id] = @schedule_id

	-- 'AT TIME ZONE' since SQL Server 2016
	DECLARE @CurrentDate DATE = GETUTCDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'W. Europe Standard Time'
	DECLARE @CurrentTime TIME = GETUTCDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'W. Europe Standard Time'
	DECLARE @CurrentDateAndTime DATETIME = CAST(@CurrentDate AS DATETIME) + CAST(@CurrentTime AS DATETIME)

	DECLARE @StartDateAndTime DATETIME = CAST(@CurrentDate AS datetime) + STUFF(STUFF(RIGHT('000000' + CAST(@active_start_time AS VARCHAR), 6), 5, 0, ':'), 3, 0, ':')
	DECLARE @EndDateAndTime DATETIME = CAST(@CurrentDate AS datetime) + STUFF(STUFF(RIGHT('000000' + CAST(@active_end_time AS VARCHAR), 6), 5, 0, ':'), 3, 0, ':')

	DECLARE @NextTime DATETIME = NULL
	DECLARE @ForToday BIT = 0
	DECLARE @Seconds INT
	DECLARE @Minutes INT
	DECLARE @Hours INT

	IF (@freq_subday_type NOT IN (0, 1, 2, 4, 8) OR @freq_subday_interval < 0 OR @freq_subday_interval > 100) BEGIN
		SET @NextTime = NULL
		SET @ForToday = 1
	END
	ELSE IF @CurrentDateAndTime < @StartDateAndTime BEGIN
		SET @NextTime = @StartDateAndTime
		SET @ForToday = 1
	END
	ELSE IF (@CurrentDateAndTime > @EndDateAndTime) BEGIN
		SET @NextTime = @StartDateAndTime
		SET @ForToday = 0
	END
	ELSE IF (@freq_subday_type IN (0, 1)) BEGIN
		-- At the specified time
		SET @NextTime = @StartDateAndTime
		SET @ForToday = 0
	END
	ELSE IF (@freq_subday_type = 2) BEGIN
		-- Every @freq_subday_interval seconds
		SET @Seconds = DATEDIFF(SECOND, @StartDateAndTime, @CurrentDateAndTime)
		IF (@Seconds % @freq_subday_interval = 0) BEGIN
			SET @NextTime = @CurrentDateAndTime
			SET @ForToday = 1
		END
		ELSE BEGIN
			SET @Seconds = @freq_subday_interval * ((@Seconds / @freq_subday_interval) + 1)
			SET @NextTime = DATEADD(SECOND, @Seconds, @StartDateAndTime)
			IF (@NextTime <= @EndDateAndTime) BEGIN
				SET @ForToday = 1
			END
			ELSE BEGIN
				SET @NextTime = @StartDateAndTime
				SET @ForToday = 0
			END
		END
	END
	ELSE IF (@freq_subday_type = 4) BEGIN
		-- Every @freq_subday_interval minutes
		SET @Minutes = DATEDIFF(MINUTE, @StartDateAndTime, @CurrentDateAndTime)
		IF (@Minutes % @freq_subday_interval = 0) BEGIN
			IF (DATEPART(SECOND, @CurrentDateAndTime) <= DATEPART(SECOND, @StartDateAndTime)) BEGIN
				SET @NextTime = DATEADD(MINUTE, @Minutes, @StartDateAndTime)
				SET @ForToday = 1
			END
			ELSE BEGIN
				SET @Minutes = @freq_subday_interval * ((@Minutes / @freq_subday_interval) + 1)
				SET @NextTime = DATEADD(MINUTE, @Minutes, @StartDateAndTime)
				IF (@NextTime <= @EndDateAndTime) BEGIN
					SET @ForToday = 1
				END
				ELSE BEGIN
					SET @NextTime = @StartDateAndTime
					SET @ForToday = 0
				END
			END
		END
		ELSE BEGIN
			SET @Minutes = @freq_subday_interval * ((@Minutes / @freq_subday_interval) + 1)
			SET @NextTime = DATEADD(MINUTE, @Minutes, @StartDateAndTime)
			IF (@NextTime <= @EndDateAndTime) BEGIN
				SET @ForToday = 1
			END
			ELSE BEGIN
				SET @NextTime = @StartDateAndTime
				SET @ForToday = 0
			END
		END
	END
	ELSE IF (@freq_subday_type = 8) BEGIN
		-- Every @freq_subday_interval hours
		SET @Hours = DATEDIFF(HOUR, @StartDateAndTime, @CurrentDateAndTime)
		IF (@Hours % @freq_subday_interval = 0) BEGIN
			IF (DATEPART(MINUTE, @CurrentDateAndTime) <= DATEPART(MINUTE, @StartDateAndTime) OR (DATEPART(MINUTE, @CurrentDateAndTime) = DATEPART(MINUTE, @StartDateAndTime) AND DATEPART(SECOND, @CurrentDateAndTime) <= DATEPART(SECOND, @StartDateAndTime))) BEGIN
				SET @NextTime = DATEADD(HOUR, @Hours, @StartDateAndTime)
				SET @ForToday = 1
			END
			ELSE BEGIN
				SET @Hours= @freq_subday_interval * ((@Hours / @freq_subday_interval) + 1)
				SET @NextTime = DATEADD(HOUR, @Hours, @StartDateAndTime)
				IF (@NextTime <= @EndDateAndTime) BEGIN
					SET @ForToday = 1
				END
				ELSE BEGIN
					SET @NextTime = @StartDateAndTime
					SET @ForToday = 0
				END
			END
		END
		ELSE BEGIN
			SET @Hours= @freq_subday_interval * ((@Hours / @freq_subday_interval) + 1)
			SET @NextTime = DATEADD(HOUR, @Hours, @StartDateAndTime)
			IF (@NextTime <= @EndDateAndTime) BEGIN
				SET @ForToday = 1
			END
			ELSE BEGIN
				SET @NextTime = @StartDateAndTime
				SET @ForToday = 0
			END
		END
	END

	SET @NextTime = CASE WHEN @NextTime IS NOT NULL AND @ForToday <> 1 THEN DATEADD(DAY, 1, @NextTime) ELSE @NextTime END

	DECLARE @StartDate DATE = STUFF(STUFF(RIGHT('00000000' + CAST(@active_start_date AS VARCHAR), 8), 7, 0, '-'), 5, 0, '-')
	DECLARE @EndDate DATE = STUFF(STUFF(RIGHT('00000000' + CAST(@active_end_date AS VARCHAR), 8), 7, 0, '-'), 5, 0, '-')
	DECLARE @Today DATE = @NextTime
	DECLARE @RunningDate DATE = @NextTime
	DECLARE @NextDate DATE = NULL
	DECLARE @NextWeekDate DATE
	DECLARE @Year INT
	DECLARE @Month INT
	DECLARE @Weekday INT
	DECLARE @Count INT

	IF (@NextTime IS NULL OR @RunningDate > @EndDate) BEGIN
		SET @NextTime = NULL
		GOTO DONE
	END

	SET @RunningDate = @StartDate
	IF (@freq_type = 1) BEGIN
		-- 1 = One time only
		SET @NextDate = CASE WHEN (@RunningDate = @Today) THEN @RunningDate ELSE NULL END
		GOTO DONE
	END

	IF (@freq_type = 4) BEGIN
		-- 4 = Daily
		IF (@RunningDate = @Today) BEGIN
			SET @NextDate = @RunningDate
			GOTO DONE
		END
		IF (@freq_interval = 0) GOTO DONE
		WHILE (@RunningDate < @Today AND @RunningDate < @EndDate) BEGIN
			SET @RunningDate = DATEADD(DAY, @freq_interval, @RunningDate)
		END
		SET @NextDate = CASE WHEN (@RunningDate <= @EndDate) THEN @RunningDate ELSE NULL END
		GOTO DONE
	END

	IF (@freq_type = 8) BEGIN
		-- 8 = Weekly
		WHILE (@RunningDate < @Today AND @RunningDate < @EndDate) BEGIN
			SET @RunningDate = DATEADD(DAY, 7 * @freq_recurrence_factor, @RunningDate)
		END
		WHILE (@RunningDate < @EndDate) BEGIN
			SET @NextWeekDate = DATEADD(DAY, 8, @RunningDate)
			WHILE (@RunningDate < @NextWeekDate AND 0 = (POWER(2, DATEPART(WEEKDAY, @RunningDate)) & @freq_interval)) SET @RunningDate = DATEADD(DAY, 1, @RunningDate)
			IF (@RunningDate < @NextWeekDate) BEGIN
				SET @NextDate = @RunningDate
				GOTO DONE
			END
			SET @RunningDate = DATEADD(DAY, 7 * @freq_recurrence_factor, DATEADD(DAY, -8, @NextWeekDate))
		END
		GOTO DONE
	END

	IF (@freq_type = 16) BEGIN
		-- 16 = Monthly
		WHILE (DATEPART(DAY, @RunningDate) < @freq_interval AND @RunningDate < @EndDate) BEGIN
			SET @RunningDate = DATEADD(DAY, 1, @RunningDate)
		END
		WHILE (@RunningDate < @Today AND @RunningDate < @EndDate) BEGIN
			SET @RunningDate = DATEADD(MONTH, @freq_recurrence_factor, @RunningDate)
		END
		SET @NextDate = CASE WHEN (@RunningDate < @EndDate) THEN @RunningDate ELSE NULL END
		GOTO DONE
	END

	IF (@freq_type = 32) BEGIN
		-- 32 = Monthly, relative to freq_interval
		IF (@freq_relative_interval = 0) GOTO DONE
		IF (@freq_relative_interval = 5) BEGIN
			-- freq_relative_interval = 5
			-- last su/mo/tu/we/th/fr/sa/day/weekday/weekendday
			SET @RunningDate = DATEADD(DAY, -1, DATEADD(MONTH, 1, DATEADD(DAY, 1 - DAY(@RunningDate), @RunningDate)))
			WHILE (@RunningDate < @EndDate) BEGIN
				IF (@freq_interval IN (1,2,3,4,5,6,7)) BEGIN
					-- 1 = Sunday
					-- 2 = Monday
					-- 3 = Tuesday
					-- 4 = Wednesday
					-- 5 = Thursday
					-- 6 = Friday
					-- 7 = Saturday
					WHILE (DATEPART(WEEKDAY, @RunningDate) <> @freq_interval) BEGIN
						SET @RunningDate = DATEADD(DAY, -1, @RunningDate)
					END
				END
				ELSE IF (@freq_interval = 9) BEGIN
					-- 9 = Weekday
					WHILE (DATEPART(WEEKDAY, @RunningDate) IN (1, 7)) BEGIN
						SET @RunningDate = DATEADD(DAY, -1, @RunningDate)
					END
				END
				ELSE IF (@freq_interval = 10) BEGIN
					WHILE (DATEPART(WEEKDAY, @RunningDate) NOT IN (1, 7)) BEGIN
						SET @RunningDate = DATEADD(DAY, -1, @RunningDate)
					END
				END

				IF (@RunningDate >= @Today AND @RunningDate <= @EndDate) BEGIN
					SET @NextDate = @RunningDate
					GOTO DONE
				END
				IF (@RunningDate < @EndDate) BEGIN
					GOTO DONE
				END
				-- try next month - should succeed if less than @EndDate
				SET @RunningDate = DATEADD(DAY, -1, DATEADD(MONTH, 1, DATEADD(DAY, 1 - DAY(@RunningDate), @RunningDate)))
			END
			GOTO DONE
		END
		ELSE BEGIN
			-- freq_relative_interval = 1,2,3,4
			-- first/second/third/fourth su/mo/tu/we/th/fr/sa/day/weekday/weekendday
			SET @RunningDate = DATEADD(DAY, 1 - DAY(@RunningDate), @RunningDate)
			WHILE (@RunningDate <= @EndDate) BEGIN
				IF (@freq_interval IN (1,2,3,4,5,6,7)) BEGIN
					-- 1 = Sunday
					-- 2 = Monday
					-- 3 = Tuesday
					-- 4 = Wednesday
					-- 5 = Thursday
					-- 6 = Friday
					-- 7 = Saturday
					WHILE (DATEPART(WEEKDAY, @RunningDate) <> @freq_interval) BEGIN
						SET @RunningDate = DATEADD(DAY, 1, @RunningDate)
					END
				END
				ELSE IF (@freq_interval = 9) BEGIN
					-- 9 = Weekday
					WHILE (DATEPART(WEEKDAY, @RunningDate) NOT IN (2,3,4,5,6)) BEGIN
						SET @RunningDate = DATEADD(DAY, 1, @RunningDate)
					END
				END
				ELSE IF (@freq_interval = 10) BEGIN
					-- 10 = Weekend day
					WHILE (DATEPART(WEEKDAY, @RunningDate) NOT IN (1, 7)) BEGIN
						SET @RunningDate = DATEADD(DAY, 1, @RunningDate)
					END
				END

				IF (@freq_relative_interval > 1) BEGIN
					IF (@freq_interval IN (1,2,3,4,5,6,7)) BEGIN
						SET @RunningDate = DATEADD(DAY, 7 * (@freq_relative_interval - 1), @RunningDate)
					END
					ELSE IF (@freq_interval = 8) BEGIN
						SET @RunningDate = DATEADD(DAY, @freq_relative_interval - 1, @RunningDate)
					END
					ELSE IF (@freq_interval = 9) BEGIN
						SET @Count = 2
						WHILE (@Count <= @freq_relative_interval) BEGIN
							-- add 1 day for next weekday unless we are Friday where we need to add 3 for next weekday
							SET @RunningDate = DATEADD(DAY, CASE WHEN DATEPART(WEEKDAY, @RunningDate) = 6 THEN 3 ELSE 1 END, @RunningDate)
							SET @Count = @Count + 1
						END
					END
					ELSE IF (@freq_interval = 10) BEGIN
						-- this implementation will take the next weekend, hence not the next weekend day
						SET @Count = 2
						WHILE (@Count <= @freq_relative_interval) BEGIN
							-- add 7 days for next weekend unless we are Sunday where we need to add 6 days to have the Saturday of the next weekend 
							SET @RunningDate = DATEADD(DAY, CASE WHEN DATEPART(WEEKDAY, @RunningDate) = 7 THEN 6 ELSE 7 END, @RunningDate)
							SET @Count = @Count + 1
						END
					END
				END
				IF (@RunningDate >= @Today AND @RunningDate <= @EndDate) BEGIN
					SET @NextDate = @RunningDate
					GOTO DONE
				END
				IF (@RunningDate < @EndDate) BEGIN
					GOTO DONE
				END
				-- try next month - should succeed if less than @EndDate
				SET @RunningDate = DATEADD(MONTH, 1, DATEADD(DAY, 1 - DAY(@RunningDate), @RunningDate))
			END
		END
	END

DONE:
	IF (@NextDate IS NULL OR @NextTime IS NULL) BEGIN
		RETURN NULL
	END

	RETURN CAST(@NextDate AS DATETIME) + CAST(CAST(@NextTime AS TIME) AS DATETIME)
END
GO
