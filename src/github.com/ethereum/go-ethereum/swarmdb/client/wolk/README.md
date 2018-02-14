## Wolk CLI Demo 

Below is what we are aiming for in our Venus Release:

// creating a new connection to local 
swarmdb> var conn = swarmdb.openConnection()
<connection object 1>

// creating a new connection to another wolkdb node
swarmdb> var conn2 = swarmdb.openConnection({"ip":"1.2.3.4", "port": 2001})
<connection object 2>

// opening a database locally
swarmdb> var db = conn.openDatabase("friendbook.alina.eth")
'0x1234431...'
 
// opening a database remotely
swarmdb> var db2 = conn2.createDatabase("calendar.wolkinc.eth")
'0x1234431...'
 
// see what is in the local database
swarmdb> db.listTables()
[{"tableName":"cats","root":"0x77776666"}]
 
// see what is in the remote database
swarmdb> db2.listTables()
[{"tableName":"events","root":"0x11112222.."},{"tableName":"user","root":"0x22223333"}]

// see what is in the local database
swarmdb> conn.listTables("friendbook.alina.eth")
[{"tableName":"cats","root":"0x77776666"}]
 
// see what is in the remote database
swarmdb> conn2.listTables("calendar.alina.eth")
[{"tableName":"events","root":"0x11112222.."},{"tableName":"user","root":"0x22223333"}]
 
// create a new "dogs" table in friendbook.alina.eth database
swarmdb> var t = db.createTable("dogs", [{ "columnName": "email", "columnType": "string", "indexType": "bplus", "primary": 1 }, { "columnName": "age", "columnType": "integer" }]) 
'0x88889999...'

// describe the newly created table
swarmdb> t.describe() 
[{ "columnName": "email", "columnType": "string", "indexType": "bplus", "primary": 1 }, { "columnName": "age", "columnType": "integer" }]

// show tables
swarmdb> db.listTables()
[{"tableName":"dogs","root":"0x88889999.."},{"tableName":"cats","root":"0x77776666"}]

// records are first class JSON arrays
swarmdb> var recs = [{ "email": "minnie@wolk.com", "age": 4 }, { "email": "sammy@wolk.com", "age": 13 } ])

// single row added
swarmdb> t.put(recs[0])
1 row(s) added

// multi row put
swarmdb> t.put(recs)
2 row(s) added

// multi row put
swarmdb> t.put([{ "email": "bertie@wolk.com", "age": 11 }, { "email": "happy@wolk.com", "age": 3 } ])
1 row(s) added

// get 
swarmdb> t.get('sammy@wolk.com')
{"age":13,"email":"sammy@test.com"}

// get missing row
swarmdb> t.get('rover@gmail.com')
null

// select
swarmdb> db.query('select email, age from dogs where age < 10 order by age desc limit 2')
[{"age":4,"email":"minnie@wolk.com"},{"age":3,"email":"happy@wolk.com"}]

// delete
swarmdb> t.delete('happy@wolk.com')
'0x567890abc...'

// another select 
swarmdb> db.query('select * from dogs where age < 5 order by age limit 100')
[{"age":4,"email":"minnie@wolk.com"}]

// another get
swarmdb> db.query('happy@wolk.com')
null

// look at the databases
swarmdb> conn.listDatabases("alina.eth")
[{"databaseName": "friendbook.alina.eth", "root": "0x12344321"}, {"databaseName": "wallets.alina.eth", "0x56788765..."}]

// check what is under wolkinc.eth, a different owner with PUBLIC tables
swarmdb> conn2.listDatabases("wolkinc.eth")
[{"databaseName": "videos.wolkinc.eth", "root": "0x12344321"}, {"databaseName": "music.wolkinc.eth", "0x56788765..."}]

// check what the form is of "video" table in "videos.wolkinc.eth" database owned by "wolkinc.eth"
swarmdb> conn2.describeTable("videos.wolkinc.eth", "video")
[{ "columnName": "title", "columnType": "string", "indexType": "fulltext" }, { "columnName": "id", "columnType": "bytes32" }]

// open table "video" of another user
swarmdb> var t2 = db2.openTable("video")
<Table Object 2>

// search table on full text index
swarmdb> var recs2 = t2.search("la la land")
[ ... ]


# WOLK CLI Key Methods

swarmdb:
  openConnection(<options)
  describe()
  
connection:
  listDatabases()
  db = createDatabase(databaseConfig)
  db = openDatabase(databaseName string)
  dropDatabase(databaseName string)
  describeDatabase(databaseName string)
  listTables(databaseName string)
  describeTable(tableName string)
  describe()

database:
  openTable(tableName string)
  createTable(tableName string, tableConfig)
  dropTable(tableName string)
  recs = query(sql string)
  describe()

table:  
  rec = get(key)
  put(records)
  delete(key)
  describe() 

