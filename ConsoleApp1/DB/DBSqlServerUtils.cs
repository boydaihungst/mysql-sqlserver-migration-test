
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;

namespace ValidateMigratedMysqlToSqlServer.DB
{
    class DBSqlServerUtils
    {
        public static SqlConnection GetDBConnection()
        {
            SqlConnection conn = null;
            try
            {
                string connString = ConfigurationManager.ConnectionStrings["SqlServerContext"].ConnectionString;

                conn = new SqlConnection(connString);
                conn.Open();
            }
            catch (Exception)
            {
                Console.WriteLine($"Can't open connection to Sql Server");
                Console.WriteLine($"Press any key to stop");
                Console.ReadKey();
                Environment.Exit(0);
            }
            return conn;
        }
        public static int CountRecord(SqlConnection conn, string tableName)
        {
            bool isStillConnect = conn.State == ConnectionState.Open;
            if (!isStillConnect) throw Exception("Mất kết nối MySql");
            string sql = $"SELECT count(*) FROM [{ConfigurationManager.AppSettings["SqlServerSchema"].ToString()}].[{tableName}]";
            using (SqlCommand cmd = new SqlCommand(sql, conn))
            {
                var rs = (int)cmd.ExecuteScalar();
                return rs;
            }
        }

        private static Exception Exception(string v)
        {
            throw new NotImplementedException();
        }

        public static Dictionary<string, int> GetListTable()
        {
            Dictionary<string, int> tblNameList = new Dictionary<string, int>();
            string schema = ConfigurationManager.AppSettings["SqlServerSchema"].ToString();
            try
            {
                using (SqlConnection conn = GetDBConnection())
                {
                    string sql = $"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA='{schema}'";
                    using (SqlCommand cmd = new SqlCommand(sql, conn))
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
            catch (Exception)
            {

                throw;
            }
            return tblNameList;
        }
        public static DataTable DataTableInSqlServer(string tableName, int page, int pageSize)
        {
            DataTable tblData = new DataTable(tableName);
            string schema = ConfigurationManager.AppSettings["SqlServerSchema"].ToString();
            try
            {
                using (SqlConnection conn = GetDBConnection())
                {
                    string sql = $"SELECT *  FROM [{schema}].[{tableName}]" +
                        $" ORDER BY 1 OFFSET {(page - 1) * pageSize} ROWS FETCH NEXT {pageSize} ROWS ONLY;";
                    using (SqlCommand cmd = new SqlCommand(sql, conn))
                    {
                        using (DbDataReader reader = cmd.ExecuteReader())
                        {
                            tblData.Load(reader);

                        }
                    }
                }
            }
            catch (Exception)
            {

                throw;
            }
            return tblData;
        }
        //public static DataTable GetDataTableFromSqlServer(string tableName, DataTable csvData)
        public static DataTable GetDataTablePaging(string tableName, string[] projectionField, int page, int pageSize)
        {
            for (int i = 0; i < projectionField.Length; i++)
            {
                projectionField[i] = $"[{projectionField[i]}]";
            }
            // Prepare sql statement
            string sql = $"SELECT {string.Join(",", projectionField)} FROM [{ConfigurationManager.AppSettings["SqlServerSchema"].ToString()}].[{tableName}]" +
                $" ORDER BY(SELECT NULL) OFFSET {(page - 1) * pageSize} ROWS FETCH NEXT {pageSize} ROWS ONLY;";
            DataTable dataTable = new DataTable();
            using (SqlConnection conn = GetDBConnection())
            {
                using (SqlCommand cmd = new SqlCommand(sql, conn))
                {
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        dataTable.Load(reader);
                    }
                }
            }
            return dataTable;
        }
        public static DataTable GetDataType(string tableName)
        {
            DataTable tblData = new DataTable(tableName);
            try
            {
                using (SqlConnection conn = GetDBConnection())
                {
                    string sql = $"SELECT COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tableName}'  AND TABLE_SCHEMA = '{conn.Database}' order BY 1";
                    using (SqlCommand cmd = new SqlCommand(sql, conn))
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
