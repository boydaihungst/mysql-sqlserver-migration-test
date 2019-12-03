using System;
using System.Data;
using Microsoft.VisualBasic.FileIO;
using ConsoleApp1.Model;
using System.Reflection;
using System.Linq;
using MySql.Data.MySqlClient;
using Db.SqlConn;
using System.IO;
using System.Data.Common;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace ConsoleApp1
{
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
            Console.WriteLine("Getting Connection ...");
            MySqlConnection conn = DBUtils.GetMySqlConnection();

            try
            {
                Console.WriteLine("Openning Connection ...");

                conn.Open();

                Console.WriteLine("Connection successful!");
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
                // Get list table
                sql = "show tables";
                cmd.CommandText = sql;
                using (DbDataReader reader = cmd.ExecuteReader())
                {
                    if (reader.HasRows)
                    {

                        while (reader.Read())
                        {
                            int tableIndex = reader.GetOrdinal("Tables_in_dms");
                            string tableName = reader.GetString(tableIndex);
                            if (tableName != null)
                            {
                                tblNameList.Add(tableName);
                            }
                        }
                    }
                }
                foreach (var item in tblNameList)
                {
                    string filePath = $@"{csvPath}{item}.csv";
                    removeFile(filePath);
                    // Generate csv
                    sql = "use dms;\r\n" +
                            "set session group_concat_max_len = 1000000;\r\n" +
                            "SET @FieldList = (SELECT GROUP_CONCAT(CONCAT(\"IFNULL(\",COLUMN_NAME,\", '') AS \", COLUMN_NAME)) as GroupName\r\n" +
                            "from INFORMATION_SCHEMA.COLUMNS\r\n" +
                            $"WHERE TABLE_NAME = '{item}'\r\n" +
                            "order BY ORDINAL_POSITION);\r\n" +
                            "SET @DataTypeListStr = (SELECT GROUP_CONCAT(CONCAT(\"'\",DATA_TYPE,\"'\")) as DataType\r\n" +
                            "from INFORMATION_SCHEMA.COLUMNS\r\n" +
                            $"WHERE TABLE_NAME = '{item}'\r\n" +
                            "order BY ORDINAL_POSITION);\r\n" +
                            "SET @FieldListStr = (SELECT GROUP_CONCAT(CONCAT(\"'\",COLUMN_NAME,\"'\")) as GroupName\r\n" +
                            "from INFORMATION_SCHEMA.COLUMNS\r\n" +
                            $"WHERE TABLE_NAME = '{item}'\r\n" +
                            "order BY ORDINAL_POSITION);\r\n" +
                            "SET @FOLDER = REPLACE(@@secure_file_priv,'\\\\','\\/');\r\n" +
                            $"SET @PREFIX = '{item}';\r\n" +
                            "SET @EXT    = '.csv';\r\n" +
                            "\r\n" +
                            "SET @CMD = CONCAT(\"\r\n" +
                            "SELECT \",@FieldListStr,\"\r\n" +
                            "UNION ALL\r\n" +
                            "SELECT \",@DataTypeListStr,\r\n" +
                            "\" UNION ALL\r\n" +
                            $"SELECT \",@FieldList,\" FROM {item} INTO OUTFILE '\",@FOLDER,@PREFIX,@EXT,\r\n" +
                            "                   \" 'FIELDS ENCLOSED BY '\\\"' TERMINATED BY ';' ESCAPED BY ''\",\r\n" +
                            "                   \" LINES TERMINATED BY '\\r\\n'\");\r\n" +
                            "\r\n" +
                            "select @CMD;\r\n" +
                            "PREPARE statement FROM @CMD;\r\n" +
                            "\r\n" +
                            "EXECUTE statement;\r\n";
                    cmd.CommandText = sql;
                    cmd.ExecuteNonQuery();
                    Console.WriteLine($"Valid table {item}....");
                    bool isDataOk = ValidDataFromCsvAndSqlServer(filePath, item);
                    if (!isDataOk) break;
                }

                Console.WriteLine("Done");
            }
            catch (Exception e)
            {
                Console.WriteLine("Error: " + e);
            }
            Console.ReadLine();
        }

        private static void removeFile(string filePath)
        {
            if (File.Exists(filePath))
            {
                File.Delete(filePath);
                Console.WriteLine("The file exists. Removed old file");
            }
        }
        private static bool CompareCsvAndSqlServer(DataTable csvData, DataTable DBServer)
        {
            // Console.WriteLine("CSV Rows count:" + csvData.Rows.Count);
            // Console.WriteLine("DBServer Rows count:" + DBServer.Rows.Count);
            // Check header size 
            if (csvData.Columns.Count != DBServer.Columns.Count)
            {
                Console.WriteLine("CSV Headers count:" + csvData.Rows.Count);
                Console.WriteLine("DBServer Headers count:" + DBServer.Rows.Count);
                return false;
            }
            // Check header name equal
            foreach (DataColumn col in csvData.Columns)
            {
                bool isHeaderNameNotEqual = DBServer.Columns.Contains(col.ToString());
                if (!isHeaderNameNotEqual)
                {
                    Console.WriteLine($"[{col.ToString()}] field from CSV not found in DBServer");
                    return false;
                }
            }
            // Check data size
            if (csvData.Rows.Count != DBServer.Rows.Count)
            {
                Console.WriteLine($"CSV Data size = {csvData.Rows.Count}");
                Console.WriteLine($"DBServer Data size = {DBServer.Rows.Count}");
                Console.WriteLine($"CSV Data size not equal DBServer");
                return false;
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
                        Console.WriteLine($"Data of field {col.ColumnName}: {rowCsv[col.ColumnName]} of CSV not equal with DBServer");
                        return false;
                    }
                }
            }
            return true;
        }

        private static DataTable GetDataTableFromSqlServer(string tableName, DataTable csvData)
        {
            SqlConnection conn = DBUtils.GetSqlServerConnection();
            conn.Open();

            // Prepare sql statement
            string sql = $"SELECT *  FROM [dms].[{tableName}]";
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
            Console.WriteLine(sql);
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
                        bool isDataOk = CompareCsvAndSqlServer(csvData, DBServer);
                        if (!isDataOk) return false;
                        csvData.Rows.Clear();
                    }

                }
                // Compare with sql server
                if (!csvHasDataRow)
                {
                    DataTable DBServer = GetDataTableFromSqlServer(tableName, csvData);
                    bool isDataOk = CompareCsvAndSqlServer(csvData, DBServer);
                    if (!isDataOk) return false;
                    csvData.Rows.Clear();
                }
            }
            return true;
        }
    }
}
