package shy.sparkstreaming;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;

/**
 * Created by root on 2016/4/15.
 */
public class ConnectionPool {

    //static connection queue
    private static LinkedList<Connection> connectionQueue;

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建连接
     *
     * @return Connection
     */
    public synchronized static Connection getConnection() {
        try {
            if (connectionQueue == null) {
                connectionQueue = new LinkedList<Connection>();
                for (int i = 0; i < 10; i++) {
                    Connection connection = DriverManager.getConnection("url", "username", "passwd");
                    connectionQueue.push(connection);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return connectionQueue.poll();
    }

    /**
     * 返回一个连接
     */
    public static void returnConnection(Connection connection) {
        connectionQueue.push(connection);
    }
}
