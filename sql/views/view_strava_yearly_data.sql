CREATE VIEW [dbo].[view_strava_yearly_data] AS
SELECT
    FORMAT(start_date, 'yyyy') AS activity_period, 
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
	'Yearly' as 'period_type'
FROM strava_activity
WHERE [type] in ('RUN','RIDE','VIRTUALRIDE')
GROUP BY FORMAT(start_date, 'yyyy');
GO