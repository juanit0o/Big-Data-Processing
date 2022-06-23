DROP TABLE epaMonitors;
DROP TABLE usaStates;

-- create internal table
CREATE TABLE epaMonitors(
stateCode INT,
countyCode INT,
siteNum INT,
parameterCode INT,
poc INT,
latitude FLOAT,
longitude FLOAT,
datum CHAR(20),
parameterName CHAR(100),
sampleDuration CHAR(20),
pollutionStandard CHAR(100),
dateLocal DATE,
unitsMeasure CHAR(100),
eventType CHAR(100),
obsCount INT, 
obsPercent FLOAT,
arithmeticMean FLOAT,
1stMaxVal FLOAT,
1stMaxHour FLOAT,
AQI CHAR(50),
methodCode INT,
methodName CHAR(200),
localSiteName CHAR(200),
address CHAR(200),
stateName CHAR(50),
countyName CHAR(50),
cityName CHAR(50),
cbsaName CHAR(100),
dateLastChange DATE,
quadrant CHAR(3),
nrMonitors FLOAT,
ano DATE,
TotalAirq FLOAT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
tblproperties("skip.header.line.count"="1");

-- load data into table from localfile (to HIVE)
LOAD DATA LOCAL INPATH 'epa_hap_daily_summary-small.csv' INTO TABLE epaMonitors;

-- create internal table
CREATE TABLE usaStates(
state CHAR(30),
stateNameUSA CHAR(50),
minLat FLOAT,
maxLat FLOAT,
minLong FLOAT,
maxLong FLOAT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
tblproperties("skip.header.line.count"="1");


-- load data into table from localfile (to HIVE)
LOAD DATA LOCAL INPATH 'usa_states.csv' INTO TABLE usaStates;

--Exc 1
SELECT stateCode, count(distinct latitude, longitude) AS count FROM epaMonitors GROUP BY stateCode ORDER BY count DESC;

--Exc 2
SELECT stateCode, countyCode, avg(arithmeticMean) AS AveragePolution FROM epaMonitors GROUP BY stateCode, countyCode ORDER BY AveragePolution DESC;

--Exc 3
SELECT YEAR(dateLocal) ano, stateName, avg(arithmeticMean) AveragePolution FROM epaMonitors GROUP BY YEAR(dateLocal),stateName ORDER BY ano,AveragePolution;

--Exc 4
 SELECT newTable.state,AVG(newTable.distance) AS avgDistance 
 FROM (
    SELECT DISTINCT epaMonitors.stateName AS state, 
    SQRT(POWER(epaMonitors.latitude*111-(usaStates.minLat*111+usaStates.maxLat*111)/2,2)+POWER(epaMonitors.longitude*111-(usaStates.minLong*111+usaStates.maxLong*111)/2,2)) AS distance 
    FROM epaMonitors 
    INNER JOIN usaStates ON epaMonitors.stateName=usaStates.stateNameUSA
    ) newTable GROUP BY newTable.state ORDER BY avgDistance DESC;


--Exc 5
SELECT stateName,quadrant, count(nrMonitors) nrMonitors FROM (
	   SELECT stateName, count(DISTINCT stateName, longitude, latitude) nrMonitors,
        CASE 
            WHEN ((latitude <= (minLat+maxLat)/2) AND (longitude <= (minLong+maxLong)/2)) THEN 'NW'
            WHEN ((latitude <= (minLat+maxLat)/2) AND (longitude > (minLong+maxLong)/2)) THEN 'SW'
            WHEN ((latitude > (minLat+maxLat)/2) AND (longitude <= (minLong+maxLong)/2)) THEN 'NE'
            WHEN ((latitude > (minLat+maxLat)/2) AND (longitude > (minLong+maxLong)/2)) THEN 'SE'
        END as Quadrant
        FROM epaMonitors JOIN usaStates ON epaMonitors.stateName = usaStates.stateNameUSA
        GROUP BY stateName, Quadrant, latitude, longitude, minLat, maxLat, minLong, maxLong) a
GROUP BY stateName, Quadrant, nrMonitors
ORDER BY nrMonitors DESC;