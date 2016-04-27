using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using Microsoft.Azure.SqlDatabase.ElasticScale.Query;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;




namespace MySQLQuery

{
    class Program
    {
        public static string RemoteServer;

        //public static List<int> CustomerID = new List<int> { 15, 75, 152, 168, 230 };
        private static CommandLine s_commandLine;
        //public static shardListClass shardList;
        public static string ShardDBName;


        static void Main(string[] args)
        {


            s_commandLine = new CommandLine(args);

            if (!s_commandLine.IsValid)
            {
                s_commandLine.WriteUsage();
                return;
            }


    
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



            // Get Shard Map Manager
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



        /// <summary>
        /// Reads the next SQL command text from the console.
        /// </summary>
        public static string GetCommand()
        {
            StringBuilder sb = new StringBuilder();
            int lineNumber = 1;
            while (true)
            {
                Console.Write("{0}> ", lineNumber);

                string line = Console.ReadLine().Trim();

                switch (line.ToUpperInvariant())
                {
                    case "GO":
                        if (sb.Length == 0)
                        {
                            // "go" with empty command - reset line number
                            lineNumber = 1;
                        }
                        else
                        {
                            return sb.ToString();
                        }

                        break;

                    case "EXIT":
                        return null;

                    default:
                        if (!string.IsNullOrWhiteSpace(line))
                        {
                            sb.AppendLine(line);
                        }

                        lineNumber++;
                        break;
                }
            }
        }




        private static string GetGeoConnectionString()
        {
            SqlConnectionStringBuilder connStr = new SqlConnectionStringBuilder(GetCredentialsConnectionString());
            
                string geoRServer = RemoteServer;
                connStr.DataSource = geoRServer + ".database.windows.net";
                connStr.InitialCatalog = ShardDBName;
            // connStr.ApplicationIntent = "readonly";


            return connStr.ToString();
        }



        public static string GetConnectionString()
        {
            SqlConnectionStringBuilder connStr = new SqlConnectionStringBuilder(GetCredentialsConnectionString());
            connStr.DataSource = s_commandLine.ServerName;
            connStr.InitialCatalog = s_commandLine.DatabaseName;
            return connStr.ToString();
        }

        public static string GetMasterConnectionString()
        {
            SqlConnectionStringBuilder connStr = new SqlConnectionStringBuilder(GetCredentialsConnectionString());
            connStr.DataSource = s_commandLine.ServerName;
            connStr.InitialCatalog = "master";
            return connStr.ToString();
        }




        /// <summary>
        /// Gets a value indicating whether the command line is valid, i.e. parsing it succeeded.
        /// </summary>


        public static string GetCredentialsConnectionString()
        {
            SqlConnectionStringBuilder connStr = new SqlConnectionStringBuilder
            { 
                ApplicationName = "ESC_CMDv1.0",
                UserID = s_commandLine.UserName ?? string.Empty,
                Password = s_commandLine.Password ?? string.Empty,
                IntegratedSecurity = s_commandLine.UseTrustedConnection


            };
            return connStr.ToString();
        }

        public class shardListClass
        {
            public string Server { get; set; }
            public string Database { get; set; }

        }





        #region Command line parsing

        public class CommandLine
        {
            // Values that are read from the command line
            public string UserName { get; private set; }

            public string Password { get; private set; }

            public string ServerName { get; private set; }

            public string DatabaseName { get; private set; }

            public string ShardMap { get; private set; }

            public bool UseTrustedConnection { get; private set; }

            //public MultiShardExecutionPolicy ExecutionPolicy { get; private set; }

            //public MultiShardExecutionOptions ExecutionOptions { get; private set; }

            public int QueryTimeout { get; private set; }

            /// <summary>
            /// Gets a value indicating whether the command line is valid, i.e. parsing it succeeded.
            /// </summary>
            public bool IsValid
            {
                get
                {
                    // Verify that a correct combination of parameters were provided
                    return this.ServerName != null &&
                           this.DatabaseName != null &&
                           this.ShardMap != null &&
                           (this.UseTrustedConnection ||
                            (this.UserName != null && this.Password != null)) &&
                            !_parseErrors;
                }
            }

            /// <summary>
            /// True if there were any errors while parsing.
            /// </summary>
            private bool _parseErrors = false;

            /// <summary>
            /// Initializes a new instance of the <see cref="CommandLine" /> class and parses the provided arguments.
            /// </summary>
            public CommandLine(string[] args)
            {
                // Default values
                this.QueryTimeout = 60;
                //this.ExecutionPolicy = MultiShardExecutionPolicy.CompleteResults;
                //this.ExecutionOptions = MultiShardExecutionOptions.None;

                _args = args;
                this.ParseInternal();
            }

            // Parsing state variables
            private readonly string[] _args;
            private int _parseIndex;

            /// <summary>
            /// Parses the given command line. Returns true for success.
            /// </summary>
            private void ParseInternal()
            {
                _parseIndex = 0;

                string arg;
                while ((arg = this.GetNextArg()) != null)
                {
                    switch (arg)
                    {
                        case "-S": // Server
                            this.ServerName = this.GetNextArg();
                            break;

                        case "-d": // Shard Map Manager database
                            this.DatabaseName = this.GetNextArg();
                            break;

                        case "-sm": // Shard map
                            this.ShardMap = this.GetNextArg();
                            break;

                        case "-U": // User name
                            this.UserName = this.GetNextArg();
                            break;

                        case "-P": // Password
                            this.Password = this.GetNextArg();
                            break;

                        case "-E": // Use trusted connection (aka Windows Authentication)
                            this.UseTrustedConnection = true;
                            break;

                        case "-t": // Query timeout
                            string queryTimeoutString = this.GetNextArg();
                            if (queryTimeoutString != null)
                            {
                                int parsedQueryTimeout;
                                bool parseSuccess = int.TryParse(queryTimeoutString, out parsedQueryTimeout);
                                if (parseSuccess)
                                {
                                    this.QueryTimeout = parsedQueryTimeout;
                                }
                                else
                                {
                                    _parseErrors = true;
                                }
                            }

                            break;

                            //case "-pr": // Partial results
                            //    this.ExecutionPolicy = MultiShardExecutionPolicy.PartialResults;
                            //    break;

                            //case "-sn": // $ShardName column
                            //    this.ExecutionOptions |= MultiShardExecutionOptions.IncludeShardNameColumn;
                            //    break;
                    }
                }
            }

            /// <summary>
            /// Returns the next argument, if it exists, and advances the index. Helper method for ParseInternal.
            /// </summary>
            private string GetNextArg()
            {
                string value = null;
                if (_parseIndex < _args.Length)
                {
                    value = _args[_parseIndex];
                }

                _parseIndex++;
                return value;
            }

            /// <summary>
            /// Writes command line usage information to the console.
            /// </summary>
            public void WriteUsage()
            {
                Console.WriteLine(@"
Usage: 

ShardSqlCmd.exe
        -S  server
        -d  shard map manager database
        -sm shard map
        -U  login id
        -P  password
        -E  trusted connection
        -t  query timeout
        -pr PartialResults mode
        -sn include $ShardName column in results

  e.g.  ShardSqlCmd.exe -S myserver -d myshardmapmanagerdb -sm myshardmap -E
        ShardSqlCmd.exe -S myserver -d myshardmapmanagerdb -sm myshardmap -U mylogin -P mypasword
        ShardSqlCmd.exe -S myserver -d myshardmapmanagerdb -sm myshardmap -U mylogin -P mypasword -pr -sn
");
            }

        }
        #endregion

        
    }
}