Instructions to run the program.

Initialize the databasse by uncommenting the initializeDB() method. I have also provided an initial dump in MongoDbDump folder. You just need to uncomment line number 27 in App to load the db to your own system.

Run the program by giving 3 arguments as

dburi, dbname, number of threads.
Note: if the db is running on local host please specify "localhost" as the 1st argument.
eg:
localhost TopicsInDM 10

The output of the program is the number of successful operations performed by each thread and the sum of those.