--The CTE at the beginning generates a list of months from June 2015 to the current month. (including months for which there is no data)
--This is done using a recursive CTE that starts with June 2015 and adds one month at a time until it reaches the current month.
--The main query then aggregates the Strava activity data for each month, including counts of activities, distances, and other metrics.

CREATE VIEW [dbo].[view_strava_monthly_data] AS
WITH Months AS (
    SELECT CAST('2015-06-01' AS DATE) AS FirstDayOfMonth
    UNION ALL
    SELECT DATEADD(MONTH, 1, FirstDayOfMonth)
    FROM Months
    WHERE FirstDayOfMonth < EOMONTH(GETDATE())
)
SELECT
	FORMAT(m.FirstDayOfMonth, 'yyyy-MM') AS activity_period,
    COUNT(*) AS activity_count,
	COALESCE(SUM(CASE WHEN [type] = 'RUN' THEN 1 END),0) AS run_count,
	COALESCE(SUM(CASE WHEN [type] in ('RIDE','VIRTUALRIDE') THEN 1 END),0) AS ride_count,
    ROUND(COALESCE(SUM(CASE WHEN [type] = 'RUN' THEN distance END)/1000,0),1) AS total_km_ran,
    ROUND(COALESCE(SUM(CASE WHEN [type] != 'RUN' THEN distance END)/1000,0),1) AS total_km_rode,
    ROUND(SUM(moving_time)/(60*60),1) AS total_time_hours,
    ROUND(SUM(total_elevation_gain),1) AS total_elevation_gain_meters,
    ROUND(SUM(athlete_count),0) AS total_people_exercised_with,
    ROUND(AVG(athlete_count),1) AS avg_people_exercised_with,
    ROUND(AVG(kudos_count), 1) AS avg_kudos,
    ROUND(COALESCE(STDEV(kudos_count),0), 1) AS std_kudos,
	'Monthly' as 'period_type'
FROM Months m
LEFT JOIN strava_activity a
    ON DATEPART(YEAR, a.start_date) = YEAR(m.FirstDayOfMonth)
    AND DATEPART(MONTH, a.start_date) = MONTH(m.FirstDayOfMonth)
WHERE [type] in ('RUN','RIDE','VIRTUALRIDE')
GROUP BY m.FirstDayOfMonth;
GO