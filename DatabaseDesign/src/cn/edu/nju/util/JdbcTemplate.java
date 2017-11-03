package cn.edu.nju.util;

import java.sql.*;

public class JdbcTemplate {

    // 数据库信息
    private static String DRIVER;
    private static String IP;
    private static int PORT;
    private static String USERNAME;
    private static String PASSWORD;
    private static String DATABASE;
    private static String URL;

    static {
        DRIVER = "com.mysql.cj.jdbc.Driver";
        IP = "localhost";
        PORT = 3306;
        USERNAME = "root";
        PASSWORD = "123456";
        DATABASE = "design1";
        URL = "jdbc:mysql://" + IP + ":" + PORT + "/" + DATABASE + "?useSSL=false";
    }

    public static Connection getConnection() {
        try {
            Class.forName(DRIVER);
            return DriverManager.getConnection(URL, USERNAME, PASSWORD);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void commit(Connection connection) {
        try {
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void beginTx(Connection connection) {
        try {
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void rollback(Connection connection) {
        try {
            connection.rollback();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void release(Statement statement, Connection connection) {
        try {
            statement.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
