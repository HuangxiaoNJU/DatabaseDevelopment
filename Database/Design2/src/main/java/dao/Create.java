package dao;

import util.JdbcTemplate;

import java.sql.Connection;
import java.sql.PreparedStatement;

@SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection", "ConstantConditions"})
public class Create {

    private static Create create;

    private Create() {}

    public static Create getInstance() {
        if (create == null) {
            create = new Create();
        }
        return create;
    }

    private void createTable(String sql) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            connection = JdbcTemplate.getConnection();
            JdbcTemplate.beginTx(connection);
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.execute();
            JdbcTemplate.commit(connection);
        } catch (Exception e) {
            e.printStackTrace();
            JdbcTemplate.rollback(connection);
        } finally {
            JdbcTemplate.releaseStatement(preparedStatement);
            JdbcTemplate.releaseConnection(connection);
        }
    }

    /**
     * 创建user表
     */
    public void createUserTable() {
        String sql =
                "CREATE TABLE IF NOT EXISTS user (" +
                    "id             int(11)     unsigned NOT NULL," +
                    "name           varchar(16) NOT NULL," +
                    "phone_number   varchar(11) NOT NULL," +
                    "balance        double      NOT NULL," +
                    "PRIMARY KEY    (id)" +
                ")";
        createTable(sql);
    }

    /**
     * 创建bike表
     */
    public void createBikeTable() {
        String sql =
                "CREATE TABLE IF NOT EXISTS bike (" +
                    "id     int(11) unsigned NOT NULL," +
                    "PRIMARY KEY    (id)" +
                ")";
        createTable(sql);
    }

    public void createRecordTable() {
        String sql =
                "CREATE TABLE IF NOT EXISTS record (" +
                    "id             int(11)     unsigned NOT NULL auto_increment," +
                    "user_id        int(11)     unsigned NOT NULL," +
                    "bike_id        int(11)     unsigned NOT NULL," +
                    "departure      varchar(16) NOT NULL," +
                    "depart_time    datetime    NOT NULL," +
                    "destination    varchar(16) NOT NULL," +
                    "arrive_time    datetime    NOT NULL," +
                    "cost           double      NOT NULL," +
                    "PRIMARY KEY    (id)" +
                ")";
        createTable(sql);
    }

}
