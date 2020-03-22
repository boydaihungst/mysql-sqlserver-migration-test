using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;

namespace ValidateMigratedMysqlToSqlServer.DB
{
    class DBMySQLUtils
    {

        private static readonly List<string> listInt = new List<string> { "tinyint", "int", "smallint", "mediumint", "bigint" };
        private static readonly List<string> listDecimal = new List<string> { "float", "double" };
        private static readonly List<string> listDate = new List<string> { "datetime", "date", "timestamp" };
        private static readonly List<string> listStr = new List<string> { "char", "varchar", "binary", "varbiary", "blob", "text", "enum", "set" };

        public static MySqlConnection GetDBConnection()
        {
            MySqlConnection conn = null;
            try
            {
                // Connection String.
                string connString = ConfigurationManager.ConnectionStrings["MySqlContext"].ConnectionString;

                conn = new MySqlConnection(connString);

                conn.Open();
                Console.Title = $"Database: {conn.Database}";
            }
            catch (Exception)
            {
                Console.WriteLine($"Can't open connection to My Sql");
                Console.WriteLine($"Press any key to stop");
                Console.ReadKey();
                Environment.Exit(0);
            }
            return conn;
        }
        public static string GetExportFolder()
        {
            string csvPath = "";
            using (MySqlConnection conn = GetDBConnection())
            {
                string sql = "SELECT @@secure_file_priv";
                using (MySqlCommand cmd = new MySqlCommand(sql, conn))
                {
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                int uploadFolderIndex = reader.GetOrdinal("@@secure_file_priv");
                                string path = reader.GetString(uploadFolderIndex);
                                if (path != null)
                                    csvPath = $@"{path}";
                                else
                                {
                                    csvPath = $@"\temp\migrateFolder\";
                                }
                            }
                        }
                    }
                }
            }
            return csvPath;
        }
        public static Dictionary<string, int> GetListTable()
        {
            Dictionary<string, int> tblNameList = new Dictionary<string, int>();
            try
            {
                using (MySqlConnection conn = GetDBConnection())
                {
                    string sql = $"SELECT table_name as TABLE_NAME FROM information_schema.tables WHERE table_schema = '{conn.Database}'";
                    using (MySqlCommand cmd = new MySqlCommand(sql, conn))
                    {
                        using (DbDataReader reader = cmd.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    int tableIndex = reader.GetOrdinal("TABLE_NAME");
                                    string tableName = reader.GetString(tableIndex);
                                    if (tableName != null)
                                    {
                                        tblNameList.Add(tableName, 0);
                                    }
                                }
                            }
                        }
                    }
                    foreach (var tbl in new List<string>(tblNameList.Keys))
                    {
                        tblNameList[tbl] = CountRecord(conn, tbl);
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
            return tblNameList;
        }
        public static int CountRecord(MySqlConnection conn, string tblName)
        {
            bool isStillConnect = conn.Ping();
            if (!isStillConnect) throw new Exception("Mất kết nối MySql");
            string sql = $"SELECT COUNT(*) as COUNT FROM {tblName}";
            using (MySqlCommand cmd = new MySqlCommand(sql, conn))
            {
                using (DbDataReader reader = cmd.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            int index = reader.GetOrdinal("COUNT");
                            return reader.GetInt32(index);
                        }
                    }
                }
            }
            return 0;
        }

        public static void ExportTableMetadataCsv(string tblName)
        {
            using (MySqlConnection conn = GetDBConnection())
            {
                // Export table metadata
                string sql = $"use {conn.Database};" +
                    $"set session group_concat_max_len = 1000000;\r\n" +
                    $"SET @FOLDER = REPLACE(@@secure_file_priv,'\\\\','\\/');\r\n" +
                    $"SET @PREFIX = '{tblName}_metadata';\r\n" +
                    $"SET @EXT = '.csv';\r\n" +
                    $"SET @ExportTableMetaData = CONCAT(\"\r\n" +
                    $"SELECT COLUMN_NAME, DATA_TYPE\r\n" +
                    $"from INFORMATION_SCHEMA.COLUMNS\r\n" +
                    $"WHERE TABLE_NAME='{tblName}' AND TABLE_SCHEMA = '{conn.Database}'\r\n" +
                    $"INTO OUTFILE '\",@FOLDER,@PREFIX,@EXT,\"'\",\r\n" +
                    $"\"FIELDS ENCLOSED BY '\\\"' TERMINATED BY ';' ESCAPED BY ''\",\r\n" +
                    $"\"LINES TERMINATED BY '\\r\\n'\");\r\n" +
                    $"PREPARE statement FROM @ExportTableMetaData;\r\n" +
                    $"EXECUTE statement;";
                using (MySqlCommand cmd = new MySqlCommand(sql, conn))
                {
                    try
                    {
                        cmd.ExecuteNonQuery();
                    }
                    catch (MySqlException e)
                    {
                        if (e.Number == 1086)
                        {
                            throw new Exception($"File existed! Please remove");
                        }
                        throw new Exception($"Mysql not supported export excel");
                    }
                }
            }
        }
        public static string ExportTableDataCsv(string tblName)
        {
            string excelPath = null;
            using (MySqlConnection conn = GetDBConnection())
            {
                string sql = $"SELECT @@secure_file_priv";
                using (MySqlCommand cmd = new MySqlCommand(sql, conn))
                {
                    excelPath = cmd.ExecuteScalar().ToString();
                    if (excelPath == null)
                    {
                        throw new Exception("Mysql not supported export excel");
                    }
                    sql = $"use {conn.Database};\r\n" +
                        $"set session group_concat_max_len = 1000000;\r\n" +
                        $"SET @FieldList = (SELECT GROUP_CONCAT(CONCAT(\"IFNULL(\",COLUMN_NAME,\", '') AS \", COLUMN_NAME)) as GroupName\r\n" +
                        $"from INFORMATION_SCHEMA.COLUMNS\r\n" +
                        $"WHERE TABLE_NAME = '{tblName}' AND TABLE_SCHEMA = '{conn.Database}');\r\n" +
                        $"SET @FOLDER = REPLACE(@@secure_file_priv,'\\\\','\\/');\r\n" +
                        $"SET @PREFIX = '{tblName}';\r\nSET @EXT    = '.csv';\r\n" +
                        $"SET @ExportTableData = CONCAT(\"\r\n" +
                        $"  SELECT \",@FieldList,\" FROM {tblName}\r\n" +
                        $"  INTO OUTFILE '\",@FOLDER,@PREFIX,@EXT,\r\n" +
                        $"  \" 'FIELDS ENCLOSED BY '\\\"' TERMINATED BY ';' ESCAPED BY ''\",\r\n" +
                        $"  \" LINES TERMINATED BY '\\r\\n'\");\r\n" +
                        $"  PREPARE statement FROM @ExportTableData;\r\n" +
                        $"  EXECUTE statement;";
                    cmd.CommandText = sql;
                    try
                    {
                        cmd.ExecuteNonQuery();
                    }
                    catch (MySqlException e)
                    {
                        if (e.Number == 1086)
                        {
                            throw new Exception($"File existed! Please remove");
                        }
                        throw new Exception("Mysql not supported export excel");
                    }
                }
            }
            return excelPath;
        }

        // get datatype in db
        public static DataTable GetDataType(string tableName)
        {
            DataTable tblData = new DataTable(tableName);
            try
            {
                using (MySqlConnection conn = GetDBConnection())
                {
                    string sql = $"SELECT COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tableName}' AND TABLE_SCHEMA = '{conn.Database}' order BY 1";
                    using (MySqlCommand cmd = new MySqlCommand(sql, conn))
                    {
                        using (DbDataReader reader = cmd.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                tblData.Load(reader);
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
            return tblData;
        }
        public static DataTable DataTableInMySql(string tableName, int page, int pageSize)
        {
            DataTable tblData = new DataTable(tableName);
            try
            {
                using (MySqlConnection conn = GetDBConnection())
                {
                    string sql = $"SELECT *  FROM {tableName} ORDER BY 1" +
                        $" LIMIT {(page - 1) * pageSize}, {pageSize};";
                    using (MySqlCommand cmd = new MySqlCommand(sql, conn))
                    {
                        using (DbDataReader reader = cmd.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                tblData.Load(reader);
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
            return tblData;
        }
    }
}