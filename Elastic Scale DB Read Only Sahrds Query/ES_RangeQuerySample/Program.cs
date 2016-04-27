using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using Microsoft.Azure.SqlDatabase.ElasticScale.Query;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;
using System.Configuration;




namespace MySQLQuery

{
    class Program
    {
        public static string ROServer;

        public static List<int>   CustomerID = new List<int> { 15, 75,152,168, 230 };
        private static CommandLine s_commandLine;
        private static shardListClass shardList; 
        public static  string ShardDBName;


        static void Main()
        {
            s_commandLine = new CommandLine();

            //System.Configuration.ConfigurationManager.ConnectionStrings["MyDBConnectionString"].ConnectionString



            using (var conn = new SqlConnection(System.Configuration.ConfigurationManager.ConnectionStrings["PrimaryServer"].ConnectionString))
            {

                var cmd = conn.CreateCommand();
                cmd.CommandText = @"
                    select partner_server from sys.geo_replication_links
                    group by partner_server;";

                conn.Open();

                var reader = cmd.ExecuteReader();
                reader.Read();

                //Console.WriteLine("{0}", reader.GetString(0));
                ROServer = reader.GetString(0).ToString();
                Console.WriteLine("Failover Partner:", ROServer);

            }

            Console.WriteLine(GetConnectionString());

            // Get Shard Map Manager
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                GetConnectionString(), ShardMapManagerLoadPolicy.Eager);
            Console.WriteLine("Connected to Shard Map Manager");

            // Get Shard Map
            ShardMap map = smm.GetShardMap("CustomerIDShardMap");
            RangeShardMap<int> customerRangeShardMap = smm.GetRangeShardMap<int>("CustomerIDShardMap");

            List<shardListClass> shardList = new List<shardListClass>();


            //Collect the unique DB Names
             foreach (int id in CustomerID)
            {

                var targetShard = customerRangeShardMap.GetMappingForKey(id);
                var shardLocationDB = targetShard.Shard.Location.Database;
                var shardLocationServer = targetShard.Shard.Location.Server;

                //List<shardListClass> list = new List<shardListClass>() { new shardListClass { Server = shardLocationServer, Database = shardLocationDB } };
                //var shardList.Select(x => x.Database == shardLocationDB
                if (!shardList.Any(x => x.Database == shardLocationDB))
                {
                    shardList.Add(new shardListClass {Server = shardLocationServer, Database = shardLocationDB});
                }

            }

            //Begining with the app logic 

                string connectionString = GetCredentialsConnectionString();

                foreach (var s in shardList)
                {
                    if (s != null)
                    {
                        Console.WriteLine("shard: " + s.Database);

                        ShardDBName = s.Database.ToString();
                        SqlConnection ROconn = new SqlConnection(GetGeoConnectionString());

                        System.Data.SqlClient.SqlCommand cmd = ROconn.CreateCommand();
                        cmd.CommandText = "Select * from  [dbo].[Customers] WHERE CustomerID IN  (" + String.Join(",", CustomerID) + ")";


                        ROconn.Open();
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


                        ROconn.Close();                       
                    }               
                }
        }

        public static string GetGeoConnectionString()
        {
            SqlConnectionStringBuilder connStr = new SqlConnectionStringBuilder(GetCredentialsConnectionString());

            //connStr.DataSource = s_commandLine.ServerName;
            //connStr.InitialCatalog = s_commandLine.DatabaseName;

            string geoRServer = ROServer;

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

    public class CommandLine
    {
        // Values that are read from the command line
        public string UserName = "AlexeiK";

        public string Password = "Pass@word1!";

        public string ServerName = "v8r8tefc0a.database.windows.net";

        public string DatabaseName = "ElasticScaleStarterKit_ShardMapManagerDb";

        public string ShardMap = "CustomerIDShardMap";

        public bool UseTrustedConnection = false;



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
                        (this.UserName != null && this.Password != null));
            }
        }


    }

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


    }
}
