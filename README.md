# Query Azure SQL DB Read-Only Replicas with the Elastic Database Client library #



## Synopsis ##

Elastic Database  client library  provides with the quite powerful logic for developing sharded applications using hundreds and more of the SQL Azure databases on Microsoft Azure.  Many customers use geo-replication configuration in order to maintain reliable Azure Databases copy and to offload some ‘read -intent’ workloads, such as reporting queries. The geo-replicated Azure Databases are ‘true read only”, where   Elastic Database configuration requires read/write access to the database for managing sharding metadata. This projects demonstrates how to leverage both Elastic Database client library and read-only geo-replica of the SQL Azure DB.

Routing queries to the read-only replica
Depending on the logic, application may need to execute query against all shards (fan out query) or connect only to the shards which have the necessary data based on filtering criteria (data dependent routing). 
Data dependent routing is the ability to route the request to the appropriate database based on the sharding key. Sharding key gets validated against the Range Shard Map to calculate appropriate connection string. 
In cases when the application logic could not benefit from using range mappings the execution of the query on each of the shards might be the viable solution. ShardMapManager will be queried for constructing the connection strings to the individual databases 

## Constructing connection to the secondary Azure DB ##

A ShardMapManagerFactory.GetSqlShardMapManager method takes credentials (including the server name and database name holding the GSM) in the form of a ConnectionString and returns an instance of a ShardMapManager.  If the geo-replication is enabled, we can query master database of the instance where Primary database is hosted. 

            string connectionString = GetMasterConnectionString();
            using (var conn = new SqlConnection(connectionString))
            
            {
 
                var cmd = conn.CreateCommand();
                cmd.CommandText = @"
                    select partner_server from sys.geo_replication_links
                    group by partner_server;";
 
                conn.Open();
 
                var PrimaryServer = conn.DataSource.ToString();
 
                Console.WriteLine(("").PadRight(60, '-'));
                Console.WriteLine("Primary  Server : "+ PrimaryServer);
       
 
                SqlDataReader reader = cmd.ExecuteReader();
 
 
                reader.Read();
 
                RemoteServer = reader.GetString(0).ToString();
                Console.WriteLine("Failover Partner: " + RemoteServer + ".database.windows.net,1433");
                
              
 
                Console.WriteLine(("").PadRight(60, '-'));
                conn.Close();
 
 
            }

ShardMapManager will give the information about the shard database which has to be connected, execution of the DMV sys.geo_replication_links gives the name of the server, where secondary geo-replica DB is stored. Combining both database name and the server name we can construct the connection string to the readable copy: 


      private static string GetGeoConnectionString()
      {
    SqlConnectionStringBuilder connStr = new SqlConnectionStringBuilder(GetCredentialsConnectionString());
    
        string geoRServer = RemoteServer;
        connStr.DataSource = geoRServer + ".database.windows.net";
        connStr.InitialCatalog = ShardDBName;
    // connStr.ApplicationIntent = "readonly";
 
 
    return connStr.ToString();
      }



Executing fan-out query

                ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                GetConnectionString(), ShardMapManagerLoadPolicy.Eager);
                Console.WriteLine("Connected to Shard Map Manager");
 
            // Get Shard Map
            ShardMap map = smm.GetShardMap(s_commandLine.ShardMap);
            RangeShardMap<int> customerRangeShardMap = smm.GetRangeShardMap<int>(s_commandLine.ShardMap);
 
 
 
            try
            {
 
 
 
                // REPL
                Console.WriteLine();
                while (true)
                {
                    // Read command from console
                    string commandText = GetCommand();
                    if (commandText == null)
                    {
                        // Exit requested
                        break;
                    }
 
 
                    foreach (Shard s in map.GetShards())
                    {
                        if (s.Location != null)
                        {
                            Console.WriteLine("shard: " + s.Location.Database);
 
                                ShardDBName = s.Location.Database.ToString();
 
                            //string geoConnectionString = GetGeoConnectionString();
                            SqlConnection RemoteConn = new SqlConnection(GetGeoConnectionString());
                            
                                System.Data.SqlClient.SqlCommand cmd = RemoteConn.CreateCommand();
                                cmd.CommandText = commandText;
 
 
 
 
                            RemoteConn.Open();
                                SqlDataReader reader = cmd.ExecuteReader();
 
                                try
                                {
                                    while (reader.Read())
                                    {
                                        Console.WriteLine(String.Format("{0}, {1}, {2} ",
                                            reader["CustomerID"], reader["Name"], reader["RegionID"]));
                                    }
                                }
 
                                finally
                                {
                                    // Always call Close when done reading.
                                    reader.Close();
                                }
 
 
                            RemoteConn.Close();
                            }
                        }
                    //}
 
 
                }
            }
 
            catch (Exception e)
            {
                // Print exception and exit
                Console.WriteLine(e);
                return;
            }
 
 
        }


## Running the sample  ##

Download the project and compile it.  Open command line and execute query in the format:

      C:\Users\MyUser\<path to project>\ bin\Debug>CMDShardQuery.exe -S <azure db sever>.database.windows.net,1433 -d <Shard Map Manager DB> -sm <ShardMapName> -U <DB User> -P <Password> 

The console should print the names of the Primary and the Secondary servers:

      ------------------------------------------------------------
      Primary  Server : v8r8tefc0a.database.windows.net,1433
      Failover Partner: vn5546sdcd.database.windows.net,1433
      ------------------------------------------------------------
      Connected to Shard Map Manager

As well as invitation to enter the query text

      1> SELECT * from customers where customerid in (15, 75,152,168, 230)
      2> go
      shard: ElasticScaleStarterKit_Shard0
      15, AdventureWorks Cycles, 0
      75, Lucerne Publishing, 0
      shard: ElasticScaleStarterKit_Shard1
      152, Lucerne Publishing, 0
      168, Fabrikam, Inc., 0
      shard: ElasticScaleStarterKit_Shard2
      230, Coho Winery, 0
      1>

