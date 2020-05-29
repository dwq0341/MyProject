package org.jdbc;

import java.sql.*;

public class GetConnUtil {
    final private String url = "jdbc:mysql://localhost:3306/testdb";
    final private String username = "root";
    final private String password = "123456";

    private static GetConnUtil gc = null;

    private GetConnUtil(){}

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static GetConnUtil createInstrea() {
        if (gc == null) {
            synchronized (GetConnUtil.class) {
                if (gc == null) {
                    gc = new GetConnUtil();
                }
            }
        }
        return gc;
    }

    /**
     * 连接mysql
     * @return
     * @throws SQLException
     */
    public Connection getConn() throws SQLException {
        Connection connection = DriverManager.getConnection(url, username, password);
        connection.setAutoCommit(false); //关闭自动提交, 开启事务
        return connection;
    }

    /**
     * 关闭连接
     */
    public void closeAll(PreparedStatement statement, Connection connection) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException sqlException) {
                sqlException.printStackTrace();
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException sqlException) {
                        sqlException.printStackTrace();
                    }
                }
            }
        }
    }
}
