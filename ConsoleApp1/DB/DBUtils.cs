using ConsoleApp1.DB;
using MySql.Data.MySqlClient;
using System.Data.SqlClient;

namespace Db.SqlConn
{
    class DBUtils
    {
        public static MySqlConnection GetMySqlConnection()
        {
            string host = "127.0.0.1";
            int port = 3306;
            string database = "dms";
            string username = "boydaihungst";
            string password = "Anhhoang123";
            return DBMySQLUtils.GetDBConnection(host, port, database, username, password);
        }
   
        public static SqlConnection GetSqlServerConnection()
        {
            string datasource = @"DESKTOP-DE43FI1";
        
            string database = "dms";
            string username = "";
            string password = "";
            return DBSqlServerUtils.GetDBConnection(datasource, database, username, password);
        }
    }
}