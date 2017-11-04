package dao;

import util.JdbcTemplate;

import java.sql.Connection;
import java.sql.Statement;

public class Create {

    /**
     * 创建user表
     */
    public void createUserTable() {
        String sql =
                "CREATE TABLE IF NOT EXISTS student (" +
                    "id             int(11)     unsigned NOT NULL," +
                    "name           varchar(16) NOT NULL," +
                    "phone_number   varchar(11) NOT NULL," +
                    "balance        double      NOT NULL," +
                    "PRIMIAY KEY    (id)" +
                ");";
        Connection connection = null;
        Statement statement = null;
        try {
            connection = JdbcTemplate.getConnection();
            JdbcTemplate.beginTx(connection);
            statement = connection.createStatement();
            statement.execute(sql);
            JdbcTemplate.commit(connection);
        } catch (Exception e) {
            e.printStackTrace();
            JdbcTemplate.rollback(connection);
        } finally {
            JdbcTemplate.release(statement, connection);
        }
    }

    public void createBikeTable() {

    }

}
