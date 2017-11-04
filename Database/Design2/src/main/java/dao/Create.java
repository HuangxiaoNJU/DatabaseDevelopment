package dao;

import util.JdbcTemplate;

import java.sql.Connection;
import java.sql.PreparedStatement;

public class Create {

    private static Create create;

    private Create() {}

    public static Create getInstance() {
        if (create == null) {
            create = new Create();
        }
        return create;
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
                ");";
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
     * 创建bike表
     */
    public void createBikeTable() {

    }

}
