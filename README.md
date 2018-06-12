# PrecisionLenderDB

I used python to take bulk data dumps from https://cdr.ffiec.gov/public/ and put the data into mongodb.
I then used Spark to analyze the data via the Spark MongoDB Connector.

`pip install -r requirements.txt`

`docker-compose up`

`./load.sh`

`python analyze_data.py`


1: In brief describe why you chose the NoSQL database you selected.

  I chose MongoDB because it easily support secondary indexes and a flexible query model.
  Additionally, I did not want to force a schema upon every column. This really simplified 
  getting the data into the database.
  
2: Briefly describe your design. How is the data connected? How is it indexed? Etc.

  The data is connected via the IDRSSD key. It is also indexed on the Reporting Period.
  I ingested the balance sheet and the bank info csv files from the Bulk Data download.
  This is all that was necessary for the given use case, although it could be extended to easily
  download the rest of the data. The description row was used for the column names of the balance sheet.
  

3: Briefly describe the ETL process used to populate your design. Were there any compromises made between ease of load and functionality / ease of use?

  I cast the 'TOTAL ASSETS' column to be an integer or null. Other than that, I left everything as Strings for ease of load.
  The uncasted data would potentially require casting if it was used.

4: Why did you use the design you used, what are the pros and cons of the database /design pair? 5: How would you query your design to support the asset growth question above.

  One pro of the database/design pair is that MongoDB supports a flexible schema so it was easy to load the data in. 
  MongoDB might not be as scalable as other NoSQL DBs, i.e. Cassandra, but the use case isn't heavy on writing.
  MongDB also supports native Aggregations for ETL. The con is that the uncasted data will need to be casted and cleaned
  when used.

6: If you had more time and resources, what would you do differently / next to refine your work? 7: How do we make this assignment better? What would you do differently if you were writing it?

  If I had more time I would've cleaned and casted the other columns. Since I wasn't using them, I didn't. I would use 'IDRSSD' as the shard key.
