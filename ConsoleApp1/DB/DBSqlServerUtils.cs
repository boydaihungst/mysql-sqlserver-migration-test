
using System.Configuration;
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
    }
}
