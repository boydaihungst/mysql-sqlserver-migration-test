
using System.Data.SqlClient;

namespace ConsoleApp1.DB
{
    class DBSqlServerUtils
    {
        public static SqlConnection
            GetDBConnection(string datasource, string database, string username, string password)
        {
            //
            // Data Source=DESKTOP-DE43FI1;Initial Catalog=dms;Integrated Security=True
            //
            string connString = @"";
            if (!string.IsNullOrEmpty(username) && !string.IsNullOrEmpty(password))
            {
                connString = @"Data Source=" + datasource + ";Initial Catalog="
                        + database + ";Persist Security Info=True;User ID=" + username + ";Password=" + password;
            }
            else
            {
                connString = @"Data Source=" + datasource + ";Initial Catalog="
            + database + ";Integrated Security=True";
            }

            SqlConnection conn = new SqlConnection(connString);

            return conn;
        }
    }
}
