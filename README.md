My current laptop is a bit old since I mostly use cloud tools or a pc provided by my employer for data analysis. My preferred cloud computing providers are: SAS(it's free) and Wolfram.

I use SAS for merging and manipulating data[using their hash methods or my own hash functions] and then analyze it in Wolfram. I do not like Python since it easily breaks. An example: SAS breaks at around the 17 digit basis. Since digital computers store things numerically and do not allow for arbitrary precision, this is important, each break accumulates and leads to wildly different results, especially for complex dynamical systems as seen in chaos theory. Wolfram uses a completely different paradigm to store digits but even then you need a supercomputer to verify even basic results in Science.

The specs: Fujitsu lifebook with 4 GB of ddr 3 RAM clocked at 1600Mhz. Intel(R) Core(TM) i5-3320M CPU @ 2.60GHz

Huston:I had a bad python error that I did not have time to fix so I could not install new packages. I have since fixed the error by removing my externally managed flag. This was done after I had already programmed most of the code using build in packages. This was done for the sake of being able to conform to the deadline given since I work night shift at a different job. But it made me adapt so it was frustratingly fun and a personal challenge more than anything. 

Asynchronous is used for circuits, this pipeline does not benefit from something like that? ( this is maybe my Physics side talking but not using Python, which can not parallel process is more of a benefit than requiring asynchronous things). The question we should be asking: if python can not parallel process then why worry about timing?

Naively we would think that splitting downloads and extracting data in different scripts and running them concurrently would lead to benefits but I think the race to sleep assumes the opposite. Extracting will severely limit drive write speed. So I tried and timed a few methods and found that sequential is the fastest. This is confirmed by banks and the SAS paradigm to big data.

Again Airflow etc make sense for a bigger DB. I can make a plannar graph by hand. Using Airflow will only make me miss the deadline but I will attempt it if I have some to spare.

Part 1.
We need to download data from a source extract it, transform it and then add it to a SQL database

This is easiest using wget -i list.txt . I cannot install the wget module in Jupiter even though  I have it installed both via pip and the arch repository. There is not time to troubleshoot this problem( technical problems can take days to fix), so I will adapt to using build in functions .

download_urls = [
'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip',
'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip',
'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip',
'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip',
]

I would like to add that there were many typos  URI =/= URL. I assume you mean URL.

We use a simple for loop to automate the process. As per our talk, the Chief Risk architect shared his plans for automating pipelines. This usually requires extensive REGEX and an architecture + the client's warehouse schema. I have worked and designed star schemas before. 

We quickly inspect each CSV file to get a feel for the data. I actually like just opening the csv and scrolling around. Afterword I use the info(), head() and list() functions to extract information. In SAS you can use meta information on the 0-th read to create a hash in runtime. 

We use the header=0 option in PD to read the cols , then we use the .head(), print() and info() functions to take note of the total columns for each Quarter and then compare them numerically after each join. We notice that Q2 is not the same as the other 3 zip files.

We use the list function to change the name of the variables whilst being careful that there is a 1:1 correspondence. This should not be automated, ever. A mistake in here can ruin an entire process. 

Compare total cols after merging all files.

Since every record is supposed to be unique I concatenate the files. This is the equivalent of a UNION in sql.

We then create a local SQL database. Since I'm not sure which one the company uses, I'll use the most simple one to setup. The difference between DB is a few lines of code. 

Whatever the choice SQL is here to stay, so we should adapt rather than attempt to reinvent the wheel.

I did not appreciate the lack of DB schema specification in the task sheet, you can not simply say fact and dimension tables since the pivot and index are essential for efficient SQL use. Since the tasksheet did not provide a Schema, defining what a Fact and what a Dimension table is completely arbitrary but I conform to standard definitions provided in textbooks.

We add a Quarter timestamp for quality assurance and an extra quick index. In fact indexing by quarter would save the most in processing power.

Fact table:
trip_id, start_time, end_time, bikeid, tripduration ,usertype, gender, birthyear

Dimension table:

trip_id, from_station_id, from_station_name, to_station_id, to_station_name ,Quarter

We Merge the data into one big csv file so that my computer can actually run it, alternatively you can append each quater via sql. This entirely depends on the data flow and the customers needs. Since it is again unspecified, I've made it so it does not crash my laptop on startup. This is possible if I save the files on my hard drive vs them being in RAM. 

Next we create the Fact and Dimension Tables in a local database
using pd.to_sql + Append

Worflow for scripts:

GET URL LINKS-> DOWLOAD+ exctract_ZIPS -> PANDAS TO SQL-> PANDAS MANIPULATION -> Check if everything is ok

This was automated to be simple scripts you run, with the exception of the Pandas manipulation. We could add some error handling but really it is best to have someone verify this step manually.

PART 2 NOSQL.

The file is a JSON object.

There are several ways to attack the problem but I have never self hosted a NOSQL DB. 

The way I see it is we have a hash object with key:value pairs. If I understand the assignment you want me to fetch the data and manipulate it on my laptop. Another interpretation is that you want me to make direct requests to the api to only get the desired data. It is not clear from the assignment and I don't really have that much experience to tell.

Another issue is that JSON is already a complete nosql DB, so is just saving the file to a json format enough to pass the assignment?

I wished you were more specific. For instance TinyDB, MongoDB and Azure cosmos have completely different syntaxes and flows.

So I decided that I need to quickly learn how to self host a nosql DB and then follow the documentation.

I am a bit pressed for time since again I stress that I have another job.

So Mangodb does not work for my type of linux. TinyDB is best for personal use.

So what I will do is setup a few workflows and you can choose what the assignment wanted to say,

First one will be directly getting the json file converting it in to a dict and putting it in Tiny DB.

workflow: CREATE_TINYDB -> NOSQL_DOWNLOAD_MANIPULATION_INSERTION

Second one is directly recording to json

Third is using pandas.



Part 3 SQL queries

workflow:SQL QUERIES
My advice is to just use dedicated software for this. I used SAS and SQLlite3 viewer
##################
Select Distinct count(*) as counts, from_station_id , to_station_id  from 
  Dimension_D group by  to_station_id , from_station_id
  order by counts desc
LIMIT 10
##################
Select distinct count(DISTINCT trip_id) as count, min(start_time), max(end_time)
from Fact_D
	where (start_time BETWEEN date("2019-01-16") and date("2019-05-17") ) and (end_time BETWEEN date("2019-05-16") and date("2019-05-17") )
	order by count 
##################
CREATE table Merged as
	SELECT DISTINCT  a.trip_id, b.from_station_id,b.trip_id, a.start_time,a.end_time 
	from Fact_D as a
	
	inner  join Dimension_D as b on a.trip_id=b.trip_id
	
WHERE (a.start_time BETWEEN  date("2019-01-01") and date("2019-05-17") and a.end_time BETWEEN date("2019-01-01") and date("2019-05-17"))
##################
SELECT  from_station_id,count(DISTINCT(trip_id)) as count
	from Merged
	
WHERE (start_time BETWEEN  date("2019-01-01") and date("2019-05-17") and end_time BETWEEN date("2019-01-01") and date("2019-05-17"))
GROUP by from_station_id
##################

	SELECT  count(DISTINCT(trip_id)) as count, strftime('%m',start_time) as T
	from Fact_D
	
WHERE (start_time BETWEEN  date("2019-01-01") and date("2019-12-31") and end_time BETWEEN date("2019-01-01") and date("2019-12-31"))
GROUP by strftime('%m',start_time)
##################

	SELECT  count(DISTINCT(trip_id)) as count, date(start_time) as T
	from Fact_D
	
WHERE (start_time BETWEEN  date("2019-01-01") and date("2019-05-17") and end_time BETWEEN date("2019-01-01") and date("2019-05-17"))
GROUP by date(start_time)
