using System;
using MySql.Data.MySqlClient;

namespace Db.SqlConn
{
    class DBMySQLUtils
    {

        public static MySqlConnection
                 GetDBConnection(string host, int port, string database, string username, string password)
        {
            // Connection String.
            String connString = "Server=" + host + ";Database=" + database
                + ";Port=" + port + ";Uid=" + username + ";Pwd=" + password+ ";Allow User Variables=True";

            MySqlConnection conn = new MySqlConnection(connString);

            return conn;
        }

    }
}