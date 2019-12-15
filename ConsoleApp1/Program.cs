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
        private static string inProgressFile = @"";
        private static List<string> tblNameList = new List<string>();
        private static MySqlConnection conn = null;
        private static SqlConnection connSqlSv = null;
        private static List<string> listInt = new List<string> { "tinyint", "int", "smallint", "mediumint", "bigint" };
        private static List<string> listDecimal = new List<string> { "float", "double" };
        private static List<string> listDate = new List<string> { "datetime", "date", "timestamp" };
        private static List<string> listStr = new List<string> { "char", "varchar", "binary", "varbiary", "blob", "text", "enum", "set" };

        static void Main(string[] args)
        {
            try
            {
                conn = DBMySQLUtils.GetDBConnection();
                Console.Write("Open MySql Connection... ");
                conn.Open();
                Console.WriteLine("OK");

                Console.Write("Open Sql Server Connection... ");
                connSqlSv = DBSqlServerUtils.GetDBConnection();
                connSqlSv.Open();
                Console.WriteLine("OK");
                csvPath = DBMySQLUtils.GetExportFolder(conn);
                // Get list table
                tblNameList = DBMySQLUtils.GetListTable(conn);
                foreach (var tblName in tblNameList)
                {
                    inProgressFile = $@"{csvPath}{tblName}.csv";
                    RemoveFile(inProgressFile);
                    // Generate csv
                    DBMySQLUtils.ExportCsv(conn, tblName);
                    Console.Write($"Checking table {tblName}... ");
                    bool isDataOk = ValidDataFromCsvAndSqlServer(inProgressFile, tblName);
                    if (!isDataOk) break;
                    Console.WriteLine("OK");
                    RemoveFile(inProgressFile);
                }
                Console.WriteLine("Everything OK");
            }
            catch (Exception e)
            {
                RemoveFile(inProgressFile);
                Console.WriteLine(e.Message);
            }
            finally
            {
                if (conn != null && conn.State != ConnectionState.Closed)
                {
                    conn.Close();
                    conn.Dispose();
                }
                if (connSqlSv != null && connSqlSv.State != ConnectionState.Closed)
                {
                    connSqlSv.Close();
                    connSqlSv.Dispose();
                }
            }
            Console.ReadLine();
        }

        private static void RemoveFile(string filePath)
        {
            if (File.Exists(filePath))
            {
                FileAttributes fileAttr = File.GetAttributes(filePath);
                if (!fileAttr.HasFlag(FileAttributes.Directory))
                {
                    File.SetAttributes(filePath, FileAttributes.Normal);
                    File.Delete(filePath);
                }
            }
        }
        private static void ValidateHeaderField(DataTable csvData, DataTable DBServer, string tableName)
        {
            if (csvData.Columns.Count != DBServer.Columns.Count)
            {
                var msg = "Error: Column size not equal";
                throw new InvalidCompareException(msg);
            }
            // Check header name equal
            foreach (DataColumn col in csvData.Columns)
            {
                bool isHeaderNameNotEqual = DBServer.Columns.Contains(col.ToString());
                if (!isHeaderNameNotEqual)
                {
                    var msg = $"Error: Invalid Column Name: {col.ToString()}";
                    throw new InvalidCompareException(msg);
                }
            }
        }
        private static void ValidateData(
            DataTable csvData,
            DataTable DBServer,
            string tableName,
            int csvMaxLength,
            int sqlServerMaxLength)
        {
            if (csvMaxLength != sqlServerMaxLength)
            {
                var msg = "Error: Row size not equal";
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
                        var array1 = rowCsv.ItemArray;
                        var array2 = rowDb.ItemArray;
                        if (array1.SequenceEqual(array2))
                        {
                            isDataOk = true;
                        }
                    }

                    if (!isDataOk)
                    {
                        var msg = "Error: Invalid Row data";
                        throw new InvalidCompareException(msg);
                    }
                }
            }
        }
        private static bool CompareDataTable(DataTable csvData,
            DataTable DBServer,
            string tableName,
            int csvMaxLength,
            int sqlServerMaxLength)
        {
            ValidateHeaderField(csvData, DBServer, tableName);
            ValidateData(csvData, DBServer, tableName, csvMaxLength, sqlServerMaxLength);
            return true;
        }

        private static bool ValidDataFromCsvAndSqlServer(string csv_file_path, string tableName)
        {
            int bashSize =  int.Parse(ConfigurationManager.AppSettings["BashSize"]);
            bool csvHasDataRow = false;
            DataTable csvData = new DataTable();

            using (var csvReader = new TextFieldParser(csv_file_path))
            {
                var csvMaxLength = File.ReadAllLines(csv_file_path).Length - 2;
                var sqlServerMaxLength = DBSqlServerUtils.CountRowSqlServer(connSqlSv,tableName);
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
                        DataTable DBServer = DBSqlServerUtils.GetDataTableFromSqlServer(connSqlSv,tableName, csvData);
                        bool isDataOk = CompareDataTable(csvData, DBServer, tableName, csvMaxLength, sqlServerMaxLength);
                        if (!isDataOk) return false;
                        csvData.Rows.Clear();
                    }

                }
                // Compare with sql server
                if (!csvHasDataRow)
                {
                    DataTable DBServer = DBSqlServerUtils.GetDataTableFromSqlServer(connSqlSv,tableName, csvData);
                    bool isDataOk = CompareDataTable(csvData, DBServer, tableName, csvMaxLength, sqlServerMaxLength);
                    if (!isDataOk) return false;
                    csvData.Rows.Clear();
                }
            }
            return true;
        }
    }
}
