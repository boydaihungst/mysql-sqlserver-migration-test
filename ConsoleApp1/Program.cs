using ConsoleApp1.Model;
using Microsoft.VisualBasic.FileIO;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using ValidateMigratedMysqlToSqlServer.DB;

namespace ValidateMigratedMysqlToSqlServer
{
    class Program
    {
        private static List<string> listInt = new List<string> { "tinyint", "int", "smallint", "mediumint", "bigint" };
        private static List<string> listDecimal = new List<string> { "float", "double", "decimal" };
        private static List<string> listDate = new List<string> { "datetime", "date", "timestamp" };
        private static List<string> listStr = new List<string> { "char", "varchar", "binary", "varbiary", "blob", "text", "enum", "set" };
        private static int tableWidth = 100;

        static void Main(string[] args)
        {
            Console.OutputEncoding = Encoding.UTF8;
            MainMenu();
        }
        private static bool ValidateTableHeader(DataTable mySqlDatatable, DataTable sqlServerDatatable, TableDB tbl)
        {
            var isOk = true;
            var differenceheaders = new List<string>();
            foreach (DataColumn col in mySqlDatatable.Columns)
            {
                bool isHeaderNameNotEqual = sqlServerDatatable.Columns.Contains(col.ToString());
                if (!isHeaderNameNotEqual)
                {
                    differenceheaders.Add(col.ToString());
                    isOk = false;
                }
            }
            if (differenceheaders.Any())
            {
                tbl.Problems.Add($"Missing table header: {string.Join(",", differenceheaders.Select(row => row))}");
            }
            return isOk;
        }
        /// <summary>
        /// Validate table data
        /// </summary>
        /// <param name="mySqlDatatable"></param>
        /// <param name="sqlServerDatatable"></param>
        /// <param name="tbl"></param>
        /// <returns></returns>
        private static bool ValidateTableData(DataTable mySqlDatatable, DataTable sqlServerDatatable, TableDB tbl)
        {
            var isOk = true;
            // Compare data type 
            for (int i = 0; i < mySqlDatatable.Columns.Count; i++)
            {
                try
                {
                    var mySqlCellVal = mySqlDatatable.Columns[i];
                    var sqlServerCellVal = sqlServerDatatable.Columns[i];
                    if (mySqlCellVal.DataType != sqlServerCellVal.DataType)
                    {
                        throw new ArrayTypeMismatchException(mySqlDatatable.Columns[i].ColumnName);
                    }
                }
                catch (Exception e)
                {
                    if (e is ArrayTypeMismatchException)
                    {
                        tbl.Problems.Add($"Field {e.Message} type not match");
                    }
                    tbl.Problems = tbl.Problems.Distinct().ToList();
                    isOk = false;
                }
            }
            //foreach (DataRow row in mySqlDatatable.Rows)
            for (int i = 0; i < mySqlDatatable.Rows.Count; i++)
            {
                try
                {
                    foreach (DataColumn col in mySqlDatatable.Columns)
                    {
                        var mySqlCellVal = mySqlDatatable.Rows[i][col.ColumnName];
                        var sqlServerCellVal = sqlServerDatatable.Rows[i][col.ColumnName];
                        if (mySqlCellVal.GetType() != sqlServerCellVal.GetType())
                        {
                            throw new ArrayTypeMismatchException(col.ColumnName);
                        }
                        if (mySqlCellVal.ToString() != sqlServerCellVal.ToString())
                        {
                            throw new InvalidCompareException();
                        }
                    }
                }
                catch (Exception e)
                {
                    if (e is InvalidCompareException)
                    {
                        tbl.Problems.Add($"Data is not matching");
                    }
                    else if (e is ArrayTypeMismatchException)
                    {
                        tbl.Problems.Add($"Field {e.Message} type not match");
                    }
                    tbl.Problems = tbl.Problems.Distinct().ToList();
                    isOk = false;
                }
            }
            return isOk;
        }
        /// <summary>
        /// Compare datatable mysql and tabledata sqlserver 
        /// </summary>
        /// <param name="mySqlDatatable"></param>
        /// <param name="sqlServerDatatable"></param>
        /// <param name="tbl">Table to compare</param>
        /// <returns></returns>
        private static bool CompareTableMySqlAndSqlServer(DataTable mySqlDatatable, DataTable sqlServerDatatable, TableDB tbl)
        {
            var dataset1 = ConvertValueType(mySqlDatatable);
            var dataset2 = ConvertValueType(sqlServerDatatable);
            var isOk = ValidateTableHeader(dataset1, dataset2, tbl);
            if (!isOk) return isOk;
            isOk = ValidateTableData(dataset1, dataset2, tbl);
            return isOk;
        }
        /// <summary>
        /// Read excel file -> compare with sql server. Using paging for each table query
        /// </summary>
        /// <param name="tableDataCsvPath">Csv table data file path</param>
        /// <param name="tableMetadataCsvPath">Csv table metadata file path</param>
        /// <param name="tbl">Table to compare</param>
        /// <returns></returns>
        private static bool ValidDataFromCsvAndSqlServer(string tableDataCsvPath, string tableMetadataCsvPath, TableDB tbl)
        {
            int bashSize = int.Parse(ConfigurationManager.AppSettings["BashSize"]);
            DataTable csvData = new DataTable();
            //read column names
            List<string> colNames = new List<string>();
            //read column data type
            List<string> colDataTypes = new List<string>();
            using (var tblMetadataCsvReader = new TextFieldParser(tableMetadataCsvPath, Encoding.Default, true))
            {
                tblMetadataCsvReader.SetDelimiters(new string[] { ";" });
                tblMetadataCsvReader.HasFieldsEnclosedInQuotes = true;
                tblMetadataCsvReader.TrimWhiteSpace = false;
                while (!tblMetadataCsvReader.EndOfData)
                {
                    List<string> cellData = tblMetadataCsvReader.ReadFields().ToList();
                    if (cellData.Count() < 2)
                    {
                        tbl.Problems.Add($"Table {tbl.TableName} metadata corrupted");
                        return false;
                    }
                    colNames.Add(cellData[0]);
                    colDataTypes.Add(cellData[1]);
                }
            }
            //read row data
            using (var csvReader = new TextFieldParser(tableDataCsvPath, Encoding.Default, true))
            {
                csvReader.SetDelimiters(new string[] { ";" });
                csvReader.HasFieldsEnclosedInQuotes = true;
                csvReader.TrimWhiteSpace = false;
                // Set column datatype
                for (int i = 0; i < colNames.Count(); i++)
                {
                    Type colType = typeof(string);
                    if (listInt.Contains(colDataTypes[i], StringComparer.OrdinalIgnoreCase))
                    {
                        colType = typeof(int);
                    }
                    else if (listDecimal.Contains(colDataTypes[i], StringComparer.OrdinalIgnoreCase))
                    {
                        colType = typeof(decimal);
                    }
                    else if (listDate.Contains(colDataTypes[i], StringComparer.OrdinalIgnoreCase))
                    {
                        colType = typeof(DateTime);
                    }
                    DataColumn datecolumn = new DataColumn(colNames[i], colType)
                    {
                        AllowDBNull = true
                    };
                    csvData.Columns.Add(datecolumn);
                }
                int page = 1;
                int csvRow = 0;
                int sqlServerRow = 0;
                // Set row data
                while (!csvReader.EndOfData)
                {
                    csvRow++;
                    string[] cellData = csvReader.ReadFields();
                    object[] cellDataParsed = new object[cellData.Length];
                    //Making empty value as null
                    for (int i = 0; i < cellData.Length; i++)
                    {
                        if (cellData[i] == "")
                        {
                            cellDataParsed[i] = null;
                        }
                        else if (listInt.Contains(colDataTypes[i], StringComparer.OrdinalIgnoreCase))
                        {
                            if (int.TryParse(cellData[i], out int parsed))
                            {
                                cellDataParsed[i] = parsed;
                            }
                        }
                        else if (listDecimal.Contains(colDataTypes[i], StringComparer.OrdinalIgnoreCase))
                        {
                            if (decimal.TryParse(cellData[i], NumberStyles.Any, new NumberFormatInfo() { NumberDecimalSeparator = "." }, out decimal parsed))
                            {
                                cellDataParsed[i] = parsed;
                            }
                        }
                        else if (listDate.Contains(colDataTypes[i], StringComparer.OrdinalIgnoreCase))
                        {
                            if (DateTime.TryParse(cellData[i], out DateTime parsed))
                            {
                                cellDataParsed[i] = parsed;
                            }
                        }
                        else if (listStr.Contains(colDataTypes[i], StringComparer.OrdinalIgnoreCase))
                        {
                            cellDataParsed[i] = cellData[i];
                        }
                    }
                    csvData.Rows.Add(cellDataParsed);
                    // Compare with sql server
                    if (csvData.Rows.Count == bashSize || csvReader.EndOfData)
                    {
                        DataTable DBServer = DBSqlServerUtils.GetDataTablePaging(tbl.TableName, colNames.ToArray(), page, bashSize);
                        sqlServerRow += DBServer.Rows.Count;
                        CompareTableMySqlAndSqlServer(csvData, DBServer, tbl);
                        csvData.Rows.Clear();
                        page++;
                    }
                }
                if (csvRow > sqlServerRow)
                {
                    tbl.Problems.Add($"Missing {csvRow - sqlServerRow} records");
                    csvData.Rows.Clear();
                    return false;
                }
                else if (csvRow <= 0)
                {
                    DataTable DBServer = DBSqlServerUtils.GetDataTablePaging(tbl.TableName, colNames.ToArray(), page, bashSize);
                    CompareTableMySqlAndSqlServer(csvData, DBServer, tbl);
                    csvData.Rows.Clear();
                }
            }
            return true;
        }
        /// <summary>
        /// Convert to match object type (mysql and sql server)
        /// </summary>
        private static DataTable ConvertValueType(DataTable dataTable)
        {
            List<Type> listInt = new List<Type> { typeof(sbyte), typeof(int), typeof(short), typeof(long) };
            List<Type> listDecimal = new List<Type> { typeof(double), typeof(float), typeof(decimal) };
            List<Type> listDate = new List<Type> { typeof(DateTime) };
            DataTable dtClone = dataTable.Clone();
            for (int i = 0; i < dtClone.Columns.Count; i++)
            {
                Type colType = typeof(string);
                if (listInt.Contains(dtClone.Columns[i].DataType))
                {
                    colType = typeof(long);
                }
                else if (listDecimal.Contains(dtClone.Columns[i].DataType))
                {
                    colType = typeof(decimal);
                }
                else if (listDate.Contains(dtClone.Columns[i].DataType))
                {
                    colType = typeof(DateTime);
                }
                dtClone.Columns[i].DataType = colType;
            }
            foreach (DataRow dr in dataTable.Rows)
            {
                dtClone.ImportRow(dr);
            }
            return dtClone;
        }

        /// <summary>
        /// Compare table to table
        /// </summary>
        private static TableDB ValidationByTable(TableDB tbl)
        {
            string tblName = tbl.TableName;
            // validate header field
            try
            {
                int bashSize = int.Parse(ConfigurationManager.AppSettings["BashSize"]);
                for (int i = 0; i < Math.Ceiling((decimal)tbl.RecordCount / bashSize); i++)
                {
                    var dataTableMySql = DBMySQLUtils.DataTableInMySql(tblName, i + 1, bashSize);
                    var dataTableSqlServer = DBSqlServerUtils.DataTableInSqlServer(tblName, i + 1, bashSize);
                    CompareTableMySqlAndSqlServer(dataTableMySql, dataTableSqlServer, tbl);
                }
            }
            catch (SqlException e)
            {
                if (e.Number == 208)
                {
                    tbl.Problems.Clear();
                    tbl.Problems.Add("Table is not found in Sql Server");
                }
            }
            catch (Exception e)
            {
                //tbl.Problems.Add(e.Message);
            }
            return tbl;
        }

        /// <summary>
        /// Main menu
        /// </summary>
        public static void MainMenu()
        {
            bool isCustomInput = false;
            bool hideMenu = false;
            int subMenu = 0;
            string selected = "m";
            while (!hideMenu)
            {
                isCustomInput = false;
                Console.Clear();
                if (selected == "q") break;
                if (selected == "m")
                {
                    subMenu = 0;
                    Console.WriteLine("1) Lists tables in MySql database");
                    Console.WriteLine("2) Lists tables in Sql Server database");
                    Console.WriteLine("3) Validate all tables in database - using query");
                    Console.WriteLine("4) Validate selection tables in database - using query");
                    Console.WriteLine("5) Export Mysql database to csv file");
                    Console.WriteLine("6) Validate all tables in database - using csv file");
                }
                else if (subMenu == 0 && selected == "1")
                {
                    subMenu = 1;
                    var listTbl = DBMySQLUtils.GetListTable();
                    PrintLine();
                    PrintRow("Lists tables in MySql database");
                    PrintLine();
                    PrintRow("Table name", "Number of records");
                    PrintLine();
                    foreach (var tbl in listTbl)
                    {
                        PrintRow(tbl.Key, tbl.Value.ToString());
                    }
                }
                else if (subMenu == 0 && selected == "2")
                {
                    subMenu = 2;
                    var listTblSqlServer = DBSqlServerUtils.GetListTable();
                    PrintLine();
                    PrintRow("Lists tables in Sql Server database");
                    PrintLine();
                    PrintRow("Table name", "Number of records");
                    PrintLine();
                    foreach (var tbl in listTblSqlServer)
                    {
                        PrintRow(tbl.Key, tbl.Value.ToString());
                    }
                }
                else if (subMenu == 0 && selected == "3")
                {
                    subMenu = 3;
                    var listTblMySql = DBMySQLUtils.GetListTable()
                        .Select(item => new TableDB
                        {
                            TableName = item.Key,
                            RecordCount = item.Value
                        }).ToList();

                    var listTblSqlServer = DBSqlServerUtils.GetListTable();
                    PrintLine();
                    PrintRow("Validation Summary Report");
                    PrintLine();
                    PrintRow("", "MySql", "Sql Server");
                    PrintLine();
                    foreach (var tbl in listTblMySql)
                    {
                        ValidationByTable(tbl);
                    }
                    PrintRow("Number of tables Checked", listTblMySql.Count().ToString(), listTblSqlServer.Count().ToString());
                    PrintLine();
                    var listTblPass = listTblMySql.Where(tbl => tbl.Problems.Count() == 0);
                    var listTblFail = listTblMySql.Where(tbl => !listTblPass.Any(item => item.TableName == tbl.TableName));
                    PrintRow("Table Passed", listTblPass.Count().ToString(), "");
                    PrintLine();
                    PrintRow("Table Failed", listTblFail.Count().ToString(), "");
                    foreach (var tblFailed in listTblFail.ToList())
                    {
                        for (int index = 0; index < tblFailed.Problems.Count(); index++)
                        {
                            var problem = tblFailed.Problems[index];
                            if (index == 0)
                                PrintRow("", tblFailed.TableName, "- " + problem);
                            else
                                PrintRowRight("", "", "- " + problem);
                        }
                    }
                    PrintLine();
                }
                else if (subMenu == 0 && selected == "4")
                {
                    subMenu = 4;
                    var listTbl = DBMySQLUtils.GetListTable();
                    PrintRow("Validate table theo lựa chọn");
                    PrintLine();
                    PrintRow("Danh sách table trong Mysql");
                    PrintLine();
                    PrintRow("#", "table", "Number of Records");
                    PrintLine();
                    for (int index = 0; index < listTbl.Count(); index++)
                    {
                        var tbl = listTbl.ElementAt(index);
                        PrintRow(index.ToString(), tbl.Key, tbl.Value.ToString());
                    }
                    Console.WriteLine("Có 3 cách nhập: ");

                    Console.WriteLine(" - Chọn tất cả table: a hoặc A hoặc All");
                    Console.WriteLine(" - Chọn khoảng table: 1-10 hoặc 1-5 5-10");
                    Console.WriteLine(" - Chọn từng table: 1,2,3 hoặc 1 2 3");
                }
                else if (subMenu == 4)
                {
                    var listTbl = DBMySQLUtils.GetListTable()
                       .Select((item, index) => new TableDB
                       {
                           TableName = item.Key,
                           RecordCount = item.Value,
                           Index = index
                       }).ToList();
                    if (new Regex("(\\d-\\d \\d-\\d)|(\\d-\\d)").IsMatch(selected))
                    {
                        var listSelected = selected.Split(' ').ToList();
                        var selectedTbl = new List<TableDB>();
                        foreach (var item in listSelected)
                        {
                            var selectedTables = item.Split('-').Select(int.Parse).ToList();
                            if (selectedTables[0] < 0 || selectedTables[0] < selectedTables[1] || selectedTables[1] > listTbl.Count())
                            {
                                selected = "4";
                            }
                            selectedTbl.AddRange(listTbl.Where(e => e.Index >= selectedTables[0] && e.Index <= selectedTables[1]).ToList());
                        }
                        listTbl = selectedTbl.Distinct().ToList();
                    }
                    else if (new Regex("(\\d)|(\\d \\d)|(\\d,\\d)").IsMatch(selected))
                    {
                        var selectedTables = selected.Split(' ', ',').Select(int.Parse).Distinct().ToList();
                        if (selectedTables.Any(e => e < 0) || selectedTables.Any(e => e > listTbl.Count()))
                        {
                            selected = "4";
                        }
                        listTbl = listTbl.Where(item => selectedTables.Contains(item.Index)).ToList();
                    }
                    else if (new string[] { "a", "all" }.Contains(selected.ToLower()))
                    {

                    }
                    else
                    {
                        subMenu = 0;
                        selected = "4";
                        continue;
                    }
                    PrintLine();
                    PrintRow("Validation summary report for selection tables");
                    PrintLine();
                    PrintRow("Table", "Checked Records", "Status", "Error Message");
                    PrintLine();
                    foreach (var tbl in listTbl)
                    {
                        ValidationByTable(tbl);
                        if (tbl.Problems.Count() > 0)
                            for (int index = 0; index < tbl.Problems.Count(); index++)
                            {
                                var problem = tbl.Problems[index];
                                if (index == 0)
                                    PrintRow(tbl.TableName, tbl.RecordCount.ToString(), "Failed", problem);
                                else
                                    PrintRowRight("", "", "", problem);
                            }
                        else
                        {
                            PrintRow(tbl.TableName, tbl.RecordCount.ToString(), "Passed", "");
                        }
                        PrintLine();
                    }
                }
                else if (subMenu == 0 && selected == "5")
                {
                    PrintRow("Export Mysql database to csv file");
                    PrintLine();
                    subMenu = 5;
                    var listTbl = DBMySQLUtils.GetListTable()
                       .Select((item, index) => new TableDB
                       {
                           TableName = item.Key,
                           RecordCount = item.Value,
                           Index = index
                       }).ToList();
                    int exportSuccessCount = 0;
                    foreach (var tbl in listTbl)
                    {
                        try
                        {
                            DBMySQLUtils.ExportTableMetadataCsv(tbl.TableName);
                            DBMySQLUtils.ExportTableDataCsv(tbl.TableName);
                            PrintRow($"{tbl.TableName}", "Done");
                            exportSuccessCount++;
                        }
                        catch (Exception e)
                        {
                            PrintRow(tbl.TableName, e.Message);
                        }
                        PrintLine();
                    }
                    if (exportSuccessCount > 0)
                        Console.WriteLine($"{exportSuccessCount} tables exported");
                    else
                        Console.WriteLine($"Export failed.");
                    Console.WriteLine($"File path: {DBMySQLUtils.GetExportFolder()}");
                }
                else if (subMenu == 6)
                {
                    var listTblMySql = DBMySQLUtils.GetListTable()
                     .Select((item, index) => new TableDB
                     {
                         TableName = item.Key,
                         RecordCount = item.Value,
                         Index = index
                     }).ToList();
                    var listTblSqlServer = DBSqlServerUtils.GetListTable();
                    PrintRow("Validation Summary Report");
                    PrintLine();
                    PrintRow("", "MySql", "Sql Server");
                    PrintLine();
                    foreach (var tbl in listTblMySql)
                    {
                        try
                        {
                            var tableDataCsvPath = Path.Combine(selected, $"{tbl.TableName}.csv");
                            var tableMetadataCsvPath = Path.Combine(selected, $"{tbl.TableName}_metadata.csv");
                            ValidDataFromCsvAndSqlServer(tableDataCsvPath, tableMetadataCsvPath, tbl);
                        }
                        catch (Exception e)
                        {
                            if (e is SqlException)
                            {
                                if ((e as SqlException).Number == 208)
                                    tbl.Problems.Add("Table not exist in Sql Server");
                                else if ((e as SqlException).Number == 207)
                                    tbl.Problems.Add(e.Message);
                            }

                            if (e is FileNotFoundException)
                            {
                                tbl.Problems.Add("Cant find csv file");
                            }
                        }
                    }
                    PrintRow("Number of tables Checked", listTblMySql.Count().ToString() + "", listTblMySql.Count().ToString() + "");
                    PrintLine();
                    var listTblPass = listTblMySql.Where(tbl => tbl.Problems.Count() == 0);
                    var listTblFail = listTblMySql.Where(tbl => !listTblPass.Any(item => item.TableName == tbl.TableName));
                    PrintRow("Table Passed", listTblPass.Count().ToString(), "");
                    PrintLine();
                    PrintRow("Table Failed", listTblFail.Count().ToString(), "");
                    foreach (var tblFailed in listTblFail.ToList())
                    {
                        for (int index = 0; index < tblFailed.Problems.Count(); index++)
                        {
                            var problem = tblFailed.Problems[index];
                            if (index == 0)
                                PrintRow("", tblFailed.TableName, "- " + problem);
                            else
                                PrintRowRight("", "", "- " + problem);
                        }
                    }
                    PrintLine();
                }
                else if (subMenu == 0 && selected == "6")
                {
                    isCustomInput = true;
                    string path = "";
                    Console.WriteLine("m) Menu");
                    Console.WriteLine("q) Quit");
                    while (true)
                    {
                        Console.Write("Enter exported csv path (Example: C:/mysql/excel): ");
                        var input = Console.ReadLine().Trim().ToLower();
                        if (input == "m" || input == "q")
                        {
                            selected = input;
                            subMenu = 0;
                            break;
                        }
                        path = Path.GetFullPath(input);
                        if (Directory.Exists(path))
                        {
                            selected = input;
                            subMenu = 6;
                            break;
                        }
                        else
                        {
                            Console.WriteLine("Path is not found");
                        }
                    }
                }
                else
                {
                    subMenu = 0;
                    selected = "m";
                    continue;
                }
                Console.WriteLine("m) Menu");
                Console.WriteLine("q) Quit");
                if (!isCustomInput)
                {
                    Console.Write("\r\nEnter: ");
                    selected = Console.ReadLine().Trim().ToLower();
                }
            }
        }

        /// <summary>
        /// Print line --- command line UI
        /// </summary>
        static void PrintLine()
        {
            Console.WriteLine(new string('-', tableWidth));
        }
        /// <summary>
        /// Print row --- command line UI
        /// </summary>
        static void PrintRow(params string[] columns)
        {
            int width = (tableWidth - columns.Length) / columns.Length;
            string row = "|";

            foreach (string column in columns)
            {
                row += AlignCentre(column, width) + "|";
            }

            Console.WriteLine(row);
        }
        /// <summary>
        /// Print row alight right --- command line UI
        /// </summary>
        static void PrintRowRight(params string[] columns)
        {
            int width = (tableWidth - columns.Length) / columns.Length;
            string row = "|";

            foreach (string column in columns)
            {
                row += AlignRight(column, width) + "|";
            }

            Console.WriteLine(row);
        }
        /// <summary>
        /// Alight center --- command line UI
        /// </summary>
        static string AlignCentre(string text, int width)
        {
            text = text.Length > width ? text.Substring(0, width - 3) + "..." : text;

            if (string.IsNullOrEmpty(text))
            {
                return new string(' ', width);
            }
            else
            {
                return text.PadRight(width - (width - text.Length) / 2).PadLeft(width);
            }
        }
        /// <summary>
        /// Alight right --- command line UI
        /// </summary>
        static string AlignRight(string text, int width)
        {
            text = text.Length > width ? text.Substring(0, width - 3) + "..." : text;

            if (string.IsNullOrEmpty(text))
            {
                return new string(' ', width);
            }
            else
            {
                return text.PadRight(width).PadLeft(0);
            }
        }
    }
}
