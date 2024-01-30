     -- Calculate 5-second average of temperature and humidity
WITH AvgData AS (
    SELECT
        AVG(temperature) AS avgTemperature,
        AVG(humidity) AS avgHumidity,
        System.Timestamp AS WindowEnd
    FROM
        iotinput
    GROUP BY
        TumblingWindow(second, 5)
)

-- Calculate 1-minute maximum and minimum of temperature and humidity
, MinMaxData AS (
    SELECT
        MAX(temperature) AS maxTemperature,
        MIN(temperature) AS minTemperature,
        MAX(humidity) AS maxHumidity,
        MIN(humidity) AS minHumidity,
        System.Timestamp AS WindowEnd
    FROM
        iotinput
    GROUP BY
        TumblingWindow(minute, 1)
)

-- Combine the results with a time-bound JOIN
SELECT
    AvgData.avgTemperature,
    AvgData.avgHumidity,
    MinMaxData.maxTemperature,
    MinMaxData.minTemperature,
    MinMaxData.maxHumidity,
    MinMaxData.minHumidity,
    COALESCE(AvgData.WindowEnd, MinMaxData.WindowEnd) AS WindowEnd
INTO
    powerbioutput
FROM
    AvgData
FULL OUTER JOIN
    MinMaxData
ON
    1 = 1 

SELECT *
INTO 
    ADLSoutput
FROM
    iotinput      
