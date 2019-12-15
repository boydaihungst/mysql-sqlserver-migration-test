
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;

namespace ConsoleApp1.DB
{
    class DBSqlServerUtils
    {
        public static SqlConnection GetDBConnection()
        {
            string connString = ConfigurationManager.ConnectionStrings["SqlServerContext"].ConnectionString;

            SqlConnection conn = new SqlConnection(connString);

            return conn;
        }
        public static int CountRowSqlServer(SqlConnection conn, string tableName)
        {
            string sql = $"SELECT count(*) FROM [{ConfigurationManager.AppSettings["SqlServerSchema"].ToString()}].[{tableName}]";
            SqlCommand cmd = new SqlCommand(sql, conn);
            var rs = (int)cmd.ExecuteScalar();
            cmd.Dispose();
            return rs;
        }
        public static DataTable GetDataTableFromSqlServer(SqlConnection conn, string tableName, DataTable csvData)
        {
            // Prepare sql statement
            string sql = $"SELECT *  FROM [{ConfigurationManager.AppSettings["SqlServerSchema"].ToString()}].[{tableName}]";
            if (csvData.Rows.Count > 0)
            {
                sql += " WHERE ";
            }
            var listParams = new List<SqlParameter>();

            for (int j = 0; j < csvData.Rows.Count; j++)
            {
                sql += " ( ";
                for (int i = 0; i < csvData.Columns.Count; i++)
                {
                    string colName = csvData.Columns[i].ToString();
                    sql += $" {colName}";
                    if (csvData.Rows[j][colName] == DBNull.Value)
                    {
                        sql += $" IS NULL";
                    }
                    else
                    {
                        sql += $" = @{colName}{j} ";
                        SqlParameter param = new SqlParameter
                        {
                            ParameterName = $"@{colName}{j}",
                            Value = csvData.Rows[j][colName]
                        };
                        var test = param.Value;
                        listParams.Add(param);
                    }
                    if (i != csvData.Columns.Count - 1)
                    {
                        sql += " AND ";
                    }
                }
                sql += j != csvData.Rows.Count - 1 ? " ) OR " : " ) ";
            }
            SqlCommand cmd = new SqlCommand(sql, conn);
            foreach (var p in listParams)
            {
                cmd.Parameters.Add(p);
            }
            DataTable dataTable = new DataTable();

            // Load sql data to datatable
            using (DbDataReader reader = cmd.ExecuteReader())
            {
                dataTable.Load(reader);
            }
            cmd.Dispose();
            return dataTable;
        }
    }
}
