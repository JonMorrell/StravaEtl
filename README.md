# Strava ETL Project
(Strava is a digital platform and social network primarily used by athletes, particularly runners and cyclists, to track and share their fitness activities)

## This is an ETL project of my own Strava data using the Strava API, Python, SQL Server, and Tableau. 

![image](https://github.com/user-attachments/assets/7ae13b6e-fbcd-4482-976b-c07e654cbd75)
*The small Excel logo between the DB and Tableau is because my freebie Tableau account wont connect to SQL directly, so I used Excel as a stepping stone


**I built a Python application that runs on a scheduled task once a week. It connects to the Strava API and gets my most recent activity data. The data is cleaned and transformed before being loaded into a SQL Server database. This data warehouse is consumed by a Tableau dashboard that visualises my exercise data and will one day (hopefully) provide valuable insight!** 

## [Tableau] (https://public.tableau.com/app/profile/jon.morrell/viz/Strava_17471603891190/Dashboard1)
![image](https://github.com/user-attachments/assets/e0ce3e87-1f31-4c6a-a19d-354fbdf27949)


## [Python Application] (https://github.com/JonMorrell/StravaEtl/blob/main/src/main.py)

The python application runs in 4 main stages:
## 1) Get the datetime that the data was last extracted. This value is stored in a very simple SQL Server table.

```python
  def get_last_update_time() -> Tuple[datetime, datetime]:
    """Get the datetime we last extracted data (Stored in local db). Also return current datetime."""
    conn = connect_sql_server()
    last_updated_query = "SELECT MAX(updated_datetime) FROM update_history"
    cursor = conn.cursor()
    cursor.execute(last_updated_query)
    # this funky syntax converts the returned row into its column type (datetime)
    last_updated_db = [row[0] for row in cursor.fetchall()][0]
    current_datetime = datetime.today().now()
    return last_updated_db, current_datetime  # type: ignore
```

## 2) Extract new activities (since last extraction) from the [Strava API] (https://developers.strava.com).

```python
def extract_new_strava_activities(last_updated_warehouse: datetime) -> List[List]:
    """Connect to Strava API and get new data since last_updated_warehouse datetime."""
    header = connect_strava()
    all_activities = []
    activity_num = 1

    while True: # while activity not yet extracted

        if activity_num % 75 == 0:
            print("Hit request limit, sleep for 15 minutes...")
            time.sleep(15 * 60)

        response_json = make_strava_api_request(header, activity_num)

        if len(response_json) == 0:
            break

        converted_date = convert_strava_start_date(response_json["start_date"])
        if converted_date > last_updated_warehouse:
            activity = parse_api_output(response_json)
            all_activities.append(activity)
            activity_num += 1
        else:
           break
        
    return all_activities
```

```python
def make_strava_api_request(header:Dict, activity_num:int)-> Dict[str, str]:
    """Use Strava API to get recent page of new data."""
    parser = configparser.ConfigParser()
    parser.read("config.ini")
    get_activities_url = parser.get("STRAVA_API", "ACTIVITIES_ENDPOINT")
    param = {"per_page": 1, "page": activity_num}
    
    return requests.get(get_activities_url, headers=header, params=param).json()[0]
```

## 3) Load the new activity data into the SQL Server database.

```python
def add_new_activities_to_db(activities:List):
    """Add the most recent activities to the local db"""
    
    #Turn the activities into a dataframe and clean them before adding to the db
    df = clean_data(pd.DataFrame(activities))
    add_to_db(df)
```

## 3a. Before sending the data to the DB, we need to do some cleaning.
We first extract the specific columns of data that we want to use from the json response.

```python
def get_columns_dictionary() -> dict:
    col_dict = {
        'id' : 'int64',
        'name' : 'object',
        'distance' : 'float64',
        'moving_time' : 'float64',
        'elapsed_time' : 'float64',
        'total_elevation_gain' : 'float64',
        'type' : 'object',
        'workout_type' : 'object',
        'location_country' : 'object',
        'achievement_count' : 'int64',
        'kudos_count' : 'int64',
        'comment_count' : 'int64',
        'athlete_count' : 'int64',
        'photo_count' : 'int64',
        'average_speed' : 'float64',
        'max_speed' : 'float64',
        'average_cadence' : 'float64',
        'average_temp' : 'float64',
        'average_heartrate' : 'float64',
        'max_heartrate' : 'int64',
        'suffer_score' : 'int64',
        'start_date' : 'datetime64[ns]',
        'timezone' : 'object',
        'lat' : 'float64',
        'lng' : 'float64'
    }
    return col_dict
```
   
## 3b. We then clean the data. This invloves:
i)   removing inf and nan, and empty string values from numeric columns and replacing them with zeros
ii)  removing na values from string columns and replacing them with ''
iii) converting speed values to km/h from m/s
iv)  converting the data into a Pandas DataFrame and applying strict typying to each column (to prevent any errors when importing to DB)

```python
def clean_data(df:pd.DataFrame) -> pd.DataFrame:
    
    correct_cols = get_columns_dictionary()
    for c in df.columns:
        correct_type = correct_cols[c]

        if correct_type == 'int64' or correct_type == 'float64':
            df[c].replace([np.inf, -np.inf], np.nan, inplace=True)
            df[c].fillna(0,inplace=True)
            df[c].replace('', 0, inplace=True)
            df[c].apply(pd.to_numeric, errors='coerce')

        elif correct_type == 'object':
            df[c].fillna('',inplace=True)            
    
    df['average_speed'] = df['average_speed'] * 3.6
    df['max_speed'] = df['max_speed'] * 3.6

    df = df.astype(get_columns_dictionary())
    df = df.rename(columns={'id': 'activity_id'})

    return df
```

## 3c. Now we have clean data it is relatively simple to import it into the DB

```python
def add_to_db(df:pd.DataFrame):
    try:
        parser = configparser.ConfigParser()
        parser.read("config.ini")
    
        driver = parser.get("DATABASE", "DRIVER")
        server = parser.get("DATABASE", "SERVER")
        dbname = parser.get("DATABASE", "DB_NAME")

        connection_url = f"mssql+pyodbc://@{server}/{dbname}?driver={driver.replace(' ', '+').replace('{','').replace('}','')}&trusted_connection=yes"
        engine = create_engine(connection_url)
        df.to_sql('strava_activity', con=engine, if_exists='replace', index=False)
        print(f"success: {df.shape[0]} rows added")
    except:
        print(f"fail! Something went wrong")
```

## 4. Store the current datetime in the SQL database for use next time we refresh the data

```python
def add_last_updated_time_to_db(current_datetime:datetime):
    """Store current datetime in the db as a record of the last extraction datetime."""
    formatted_date = current_datetime.strftime('%Y-%m-%d %H:%M:%S')
    conn = connect_sql_server()
    insert_statement = f"INSERT INTO update_history ([updated_datetime]) VALUES ('{formatted_date}')"
    cursor = conn.cursor()
    cursor.execute(insert_statement)
    conn.commit()
    cursor.close()
    conn.close()
```

* This simple process runs locally on my PC once a week. In a future project I hope to build an automated cloud-based ETL pipeline. 

## [SQL](https://github.com/JonMorrell/StravaEtl/tree/main/sql)
SQL used to create tables and views:

```sql
CREATE TABLE [dbo].[strava_activity](
	[activity_id] [bigint] NULL,
	[name] [varchar](max) NULL,
	[distance] [float] NULL,
	[moving_time] [float] NULL,
	[elapsed_time] [float] NULL,
	[total_elevation_gain] [float] NULL,
	[type] [varchar](max) NULL,
	[workout_type] [varchar](max) NULL,
	[location_country] [varchar](max) NULL,
	[achievement_count] [bigint] NULL,
	[kudos_count] [bigint] NULL,
	[comment_count] [bigint] NULL,
	[athlete_count] [bigint] NULL,
	[photo_count] [bigint] NULL,
	[average_speed] [float] NULL,
	[max_speed] [float] NULL,
	[average_cadence] [float] NULL,
	[average_temp] [float] NULL,
	[average_heartrate] [float] NULL,
	[max_heartrate] [bigint] NULL,
	[suffer_score] [bigint] NULL,
	[start_date] [datetime] NULL,
	[timezone] [varchar](max) NULL,
	[lat] [float] NULL,
	[lng] [float] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO


CREATE TABLE [dbo].[update_history](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[updated_datetime] [datetime] NOT NULL,
PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

--The block at the beginning ensures there is a data line for every month, even if there is no data
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

```

Finally, thanks to Jack Leitch and his excellent Strava project, which inspired this one.
