-- Creating a "MyDimDate" Dimension Table
create table MyDimDate
(
	dateid integer not null primary key,
	date date,
	year int,
	quarter int,
	quartername varchar(30),
	month int,
	monthname varchar(30),
	day int, 
	weekday int,
	weekdayname varchar(30)
)

-- Creating a "MyDimTruck" Dimension Table
create table mydimtruck(
	truckid integer not null primary key,
	trucktype varchar(30)
)

-- Creating a "MyDimStation" Dimension Table
create table mydimstation(
	stationid integer not null primary key,
	city varchar(50)
)

-- Creating a "MyFactTrips" Fact Table
create table myfacttrips(
	tripid integer not null primary key,
	dateid integer,
	stationid integer,
	truckid integer,
	wastecollected numeric(2,2),
	foreign key(dateid) references mydimdate(dateid),
	foreign key(truckid) references mydimtruck(truckid),
	foreign key(stationid) references mydimstation(stationid) 
)	

-- Create a grouping sets query using the columns stationid, trucktype, total waste collected.
select stationid, trucktype, sum(wastecollected) as totalwastecollected
from myfacttrips f
join mydimtruck d
on f.truckid = d.truckid
group by
grouping sets(stationid, trucktype)

-- Create a rollup query using the columns year, city, stationid, and total waste collected.
select year, city, f.stationid, sum(wastecollected) as totalwastecollected
from myfacttrips f
join mydimdate d
on f.dateid = d.dateid
join mydimstation s
on f.stationid = s.stationid
group by
rollup(year, city, f.stationid)

-- Create a cube query using the columns year, city, stationid, and average waste collected.
select year, city, f.stationid, avg(wastecollected) as totalwastecollected
from myfacttrips f
join mydimdate d
on f.dateid = d.dateid
join mydimstation s
on f.stationid = s.stationid
group by
cube(year, city, f.stationid)

-- Create an MQT named max_waste_stats using the columns city, stationid, trucktype, and max waste collected.
create materialized view maxwastestats(city, stationid, trucktype, maxwastecollected) as 
select city, f.stationid, trucktype, max(wastecollected)
from myfacttrips f
join mydimtruck d
on f.truckid = d.truckid
join mydimstation s
on f.stationid = s.stationid
group by city, f.stationid, trucktype;

