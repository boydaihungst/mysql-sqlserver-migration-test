using MySql.Data.MySqlClient;
using System.Collections.Generic;
using System.Configuration;
using System.Data.Common;

namespace Db.SqlConn
{
    class DBMySQLUtils
    {

        public static MySqlConnection GetDBConnection()
        {
            // Connection String.
            string connString = ConfigurationManager.ConnectionStrings["MySqlContext"].ConnectionString;

            MySqlConnection conn = new MySqlConnection(connString);

            return conn;
        }
        public static string GetExportFolder(MySqlConnection conn)
        {
            string csvPath = "";
            string sql = "SELECT @@secure_file_priv";
            MySqlCommand cmd = new MySqlCommand(sql, conn);
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
            return csvPath;
        }
        public static List<string> GetListTable(MySqlConnection conn)
        {
            List<string> tblNameList = new List<string>();
            string sql = $"SELECT table_name as TABLE_NAME FROM information_schema.tables WHERE table_schema = '{conn.Database}'";
            MySqlCommand cmd = new MySqlCommand(sql, conn);
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
                            tblNameList.Add(tableName);
                        }
                    }
                }
            }
            return tblNameList;
        }
        public static void ExportCsv(MySqlConnection conn, string tblName)
        {
           string sql = $"use {conn.Database};\r\n" +
                            "set session group_concat_max_len = 1000000;\r\n" +
                            "SET @FieldList = (SELECT GROUP_CONCAT(CONCAT(\"IFNULL(\",COLUMN_NAME,\", '') AS \", COLUMN_NAME)) as GroupName\r\n" +
                            "from INFORMATION_SCHEMA.COLUMNS\r\n" +
                            $"WHERE TABLE_NAME = '{tblName}'\r\n" +
                            "order BY ORDINAL_POSITION);\r\n" +
                            "SET @DataTypeListStr = (SELECT GROUP_CONCAT(CONCAT(\"'\",DATA_TYPE,\"'\")) as DataType\r\n" +
                            "from INFORMATION_SCHEMA.COLUMNS\r\n" +
                            $"WHERE TABLE_NAME = '{tblName}'\r\n" +
                            "order BY ORDINAL_POSITION);\r\n" +
                            "SET @FieldListStr = (SELECT GROUP_CONCAT(CONCAT(\"'\",COLUMN_NAME,\"'\")) as GroupName\r\n" +
                            "from INFORMATION_SCHEMA.COLUMNS\r\n" +
                            $"WHERE TABLE_NAME = '{tblName}'\r\n" +
                            "order BY ORDINAL_POSITION);\r\n" +
                            "SET @FOLDER = REPLACE(@@secure_file_priv,'\\\\','\\/');\r\n" +
                            $"SET @PREFIX = '{tblName}';\r\n" +
                            "SET @EXT    = '.csv';\r\n" +
                            "SET @CMD = CONCAT(\"\r\n" +
                            "SELECT \",@FieldListStr,\"\r\n" +
                            "UNION ALL\r\n" +
                            "SELECT \",@DataTypeListStr,\r\n" +
                            "\" UNION ALL\r\n" +
                            $"SELECT \",@FieldList,\" FROM {tblName} INTO OUTFILE '\",@FOLDER,@PREFIX,@EXT,\r\n" +
                            "                   \" 'FIELDS ENCLOSED BY '\\\"' TERMINATED BY ';' ESCAPED BY ''\",\r\n" +
                            "                   \" LINES TERMINATED BY '\\r\\n'\");\r\n" +
                            "select @CMD;\r\n" +
                            "PREPARE statement FROM @CMD;\r\n" +
                            "EXECUTE statement;\r\n";
            MySqlCommand cmd = new MySqlCommand(sql, conn);
            cmd.ExecuteNonQuery();
        }
    }
}