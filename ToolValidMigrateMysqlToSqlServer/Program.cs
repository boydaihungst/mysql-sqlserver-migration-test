using System;
using System.Data;
using Microsoft.VisualBasic.FileIO;

namespace ToolValidMigrateMysqlToSqlServer
{
    class Program
    {
        static void Main(string[] args)
        {
            string csv_file_path = @"C:\Users\Administrator\Desktop\test.csv";
            DataTable csvData = GetDataTabletFromCSVFile(csv_file_path);
            Console.WriteLine("Rows count:" + csvData.Rows.Count);
            Console.ReadLine();
        }
        private static DataTable GetDataTabletFromCSVFile(string csv_file_path)
        {
            DataTable csvData = new DataTable();
            try
            {
                using (var csvReader = new TextFieldParser(csv_file_path))
                {
                    csvReader.SetDelimiters(new string[] { "," });
                    csvReader.HasFieldsEnclosedInQuotes = true;
                    //read column names
                    string[] colFields = csvReader.ReadFields();
                    foreach (string column in colFields)
                    {
                        DataColumn datecolumn = new DataColumn(column);
                        datecolumn.AllowDBNull = true;
                        csvData.Columns.Add(datecolumn);
                    }
                    while (!csvReader.EndOfData)
                    {
                        string[] fieldData = csvReader.ReadFields();
                        //Making empty value as null
                        for (int i = 0; i < fieldData.Length; i++)
                        {
                            if (fieldData[i] == "")
                            {
                                fieldData[i] = null;
                            }
                        }
                        csvData.Rows.Add(fieldData);
                    }
                }
            }
            catch (Exception ex)
            {
            }
            return csvData;
        }
}
}
