using ConsoleApp1.DB;
using ConsoleApp1.Model;
using Db.SqlConn;
using Microsoft.VisualBasic.FileIO;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.IO;
using System.Linq;

namespace ConsoleApp1
{
    // TODO: Test case cac list data type
    // Split code
    // Construct lai connect db
    class Program
    {
        private static string csvPath = @"";
        private static List<string> tblNameList = new List<string>();

        private static List<string> listInt = new List<string> { "tinyint", "int", "smallint", "mediumint", "bigint" };
        private static List<string> listDecimal = new List<string> { "float", "double" };
        private static List<string> listDate = new List<string> { "datetime", "date", "timestamp" };
        private static List<string> listStr = new List<string> { "char", "varchar", "binary", "varbiary", "blob", "text", "enum", "set" };

        static void Main(string[] args)
        {
            MySqlConnection conn = null;
            try
            {
                conn = DBMySQLUtils.GetDBConnection();
                Console.Write("Open Connection... ");
                conn.Open();
                Console.WriteLine("OK");
                csvPath = DBMySQLUtils.GetExportFolder(conn);
                // Get list table
                tblNameList = DBMySQLUtils.GetListTable(conn);
                foreach (var tblName in tblNameList)
                {
                    string filePath = $@"{csvPath}{tblName}.csv";
                    RemoveFile(filePath);
                    // Generate csv
                    DBMySQLUtils.ExportCsv(conn, tblName);
                    Console.Write($"Checking table {tblName}... ");
                    bool isDataOk = ValidDataFromCsvAndSqlServer(filePath, tblName);
                    if (!isDataOk) break;
                    Console.WriteLine("OK");
                }

                Console.WriteLine("Done");
            }
            catch (Exception e)
            {
                if (e.GetType() == typeof(InvalidCompareException))
                    Console.WriteLine(e.Message);
                else Console.WriteLine(e);
            }
            finally
            {
                if (conn != null && conn.State != ConnectionState.Closed)
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
            Console.ReadLine();
        }

        private static void RemoveFile(string filePath)
        {
            if (File.Exists(filePath))
            {
                File.Delete(filePath);
            }
        }
        private static void ValidateHeaderField(DataTable csvData, DataTable DBServer, string tableName)
        {
            if (csvData.Columns.Count != DBServer.Columns.Count)
            {
                var msg = "Column size not equal\n";
                throw new InvalidCompareException(msg);
            }
            // Check header name equal
            foreach (DataColumn col in csvData.Columns)
            {
                bool isHeaderNameNotEqual = DBServer.Columns.Contains(col.ToString());
                if (!isHeaderNameNotEqual)
                {
                    var msg = "Error: Column not found\n";
                    msg += string.Format("|{0,5}|{1,5}|{2,5}|\n",
                       "Error Column", col.ToString(), "not found");
                    throw new InvalidCompareException(msg);
                }
            }
        }
        private static void ValidateData(DataTable csvData, DataTable DBServer, string tableName)
        {
            if (csvData.Rows.Count != DBServer.Rows.Count)
            {
                var msg = "Row size not equal\n";
                throw new InvalidCompareException(msg);
            }
            // Check data value
            foreach (DataRow rowCsv in csvData.Rows)
            {
                foreach (DataColumn col in csvData.Columns)
                {
                    bool isDataOk = false;
                    foreach (DataRow rowDb in DBServer.Rows)
                    {
                        if (rowDb[col.ColumnName].ToString() == rowCsv[col.ColumnName].ToString())
                        {
                            isDataOk = true;
                        }
                    }

                    if (!isDataOk)
                    {
                        var errorRow = rowCsv[col.ColumnName].ToString().Substring(0, 10);
                        if (rowCsv[col.ColumnName].ToString().Length > 10)
                        {
                            errorRow += "...";
                        }
                        var msg = "Error: Row not equal\n";
                        msg += string.Format("|{0,5}|{1,5}|{2,5}|\n",
                           "Error Row", errorRow, "not found");
                        throw new InvalidCompareException(msg);
                    }
                }
            }
        }
        private static bool CompareDataTable(DataTable csvData, DataTable DBServer, string tableName)
        {
            ValidateHeaderField(csvData, DBServer, tableName);
            ValidateData(csvData, DBServer, tableName);
            return true;
        }

        private static DataTable GetDataTableFromSqlServer(string tableName, DataTable csvData)
        {
            SqlConnection conn = DBSqlServerUtils.GetDBConnection();
            conn.Open();

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
            conn.Close();
            return dataTable;
        }

        private static bool ValidDataFromCsvAndSqlServer(string csv_file_path, string tableName)
        {
            int bashSize = 10;
            bool csvHasDataRow = false;
            DataTable csvData = new DataTable();

            using (var csvReader = new TextFieldParser(csv_file_path))
            {
                csvReader.SetDelimiters(new string[] { ";" });
                csvReader.HasFieldsEnclosedInQuotes = true;
                //read column names
                string[] colFields = csvReader.ReadFields();
                //read column data type
                string[] colTypes = csvReader.ReadFields();

                for (int i = 0; i < colFields.Length; i++)
                {
                    Type colType = typeof(string);
                    if (listInt.Contains(colTypes[i], StringComparer.OrdinalIgnoreCase))
                    {
                        colType = typeof(int);
                    }
                    else if (listDecimal.Contains(colTypes[i], StringComparer.OrdinalIgnoreCase))
                    {
                        colType = typeof(decimal);
                    }
                    else if (listDate.Contains(colTypes[i], StringComparer.OrdinalIgnoreCase))
                    {
                        colType = typeof(DateTime);
                    }
                    DataColumn datecolumn = new DataColumn(colFields[i], colType)
                    {
                        AllowDBNull = true
                    };
                    csvData.Columns.Add(datecolumn);
                }
                // TODO: split code ra

                while (!csvReader.EndOfData)
                {
                    csvHasDataRow = true;
                    string[] fieldData = csvReader.ReadFields();
                    object[] fieldDataParsed = new object[fieldData.Length];
                    //Making empty value as null
                    for (int i = 0; i < fieldData.Length; i++)
                    {
                        if (fieldData[i] == "")
                        {
                            fieldDataParsed[i] = null;
                        }
                        else if (listInt.Contains(colTypes[i], StringComparer.OrdinalIgnoreCase))
                        {
                            if (int.TryParse(fieldData[i], out int parsed))
                            {
                                fieldDataParsed[i] = parsed;
                            }
                        }
                        else if (listDecimal.Contains(colTypes[i], StringComparer.OrdinalIgnoreCase))
                        {
                            if (decimal.TryParse(fieldData[i], out decimal parsed))
                            {
                                fieldDataParsed[i] = parsed;
                            }
                        }
                        else if (listDate.Contains(colTypes[i], StringComparer.OrdinalIgnoreCase))
                        {
                            if (DateTime.TryParse(fieldData[i], out DateTime parsed))
                            {
                                fieldDataParsed[i] = parsed;
                            }
                        }
                        else if (listStr.Contains(colTypes[i], StringComparer.OrdinalIgnoreCase))
                        {
                            fieldDataParsed[i] = fieldData[i];
                        }
                    }

                    csvData.Rows.Add(fieldDataParsed);
                    // Compare with sql server
                    if (csvData.Rows.Count == bashSize || csvReader.EndOfData)
                    {
                        DataTable DBServer = GetDataTableFromSqlServer(tableName, csvData);
                        bool isDataOk = CompareDataTable(csvData, DBServer, tableName);
                        if (!isDataOk) return false;
                        csvData.Rows.Clear();
                    }

                }
                // Compare with sql server
                if (!csvHasDataRow)
                {
                    DataTable DBServer = GetDataTableFromSqlServer(tableName, csvData);
                    bool isDataOk = CompareDataTable(csvData, DBServer, tableName);
                    if (!isDataOk) return false;
                    csvData.Rows.Clear();
                }
            }
            return true;
        }
    }
}
