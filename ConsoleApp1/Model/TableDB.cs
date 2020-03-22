using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp1.Model
{
    class TableDB
    {
        public int Index { get; set; }
        public string TableName { get; set; }
        public int RecordCount { get; set; }
        //public DataTable DataTableMySql { get; set; }
        //public DataTable DataTableSqlServer { get; set; }
        public List<string> Problems { get; set; } = new List<string>();
    }
}
