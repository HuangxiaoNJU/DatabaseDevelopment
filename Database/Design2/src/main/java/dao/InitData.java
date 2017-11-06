package dao;

import util.JdbcTemplate;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection", "ConstantConditions"})
public class InitData {

    private static final String USER_FILE_NAME = "user.txt";
    private static final String BIKE_FILE_NAME = "bike.txt";
    private static final String RECORD_FILE_NAME = "record.txt";

    /**
     * 初始化共享单车数据
     */
    public void initBikeData() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(BIKE_FILE_NAME);

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try(BufferedReader br = new BufferedReader(new InputStreamReader(stream))) {
            connection = JdbcTemplate.getConnection();
            JdbcTemplate.beginTx(connection);
            preparedStatement = connection.prepareStatement("INSERT INTO bike VALUES(?)");
            String line;
            while ((line = br.readLine()) != null) {
                preparedStatement.setInt(1, Integer.valueOf(line));
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            JdbcTemplate.commit(connection);
        } catch (Exception e) {
            e.printStackTrace();
            JdbcTemplate.rollback(connection);
        } finally {
            JdbcTemplate.releaseStatement(preparedStatement);
            JdbcTemplate.releaseConnection(connection);
        }
    }

    private void prepareUserParam(String line, PreparedStatement statement) throws SQLException {
        String[] info = line.split(";");
        statement.setInt(1, Integer.valueOf(info[0]));
        statement.setString(2, info[1]);
        statement.setString(3, info[2]);
        statement.setDouble(4, Double.valueOf(info[3]));
    }

    /**
     * 初始化用户数据
     */
    public void initUserData() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(USER_FILE_NAME);

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(stream))) {
            connection = JdbcTemplate.getConnection();
            JdbcTemplate.beginTx(connection);
            preparedStatement = connection.prepareStatement(
                    "INSERT INTO user(id, name, phone_number, balance) VALUES (?,?,?,?)");

            String line;
            int i = 0;
            while ((line = br.readLine()) != null) {
                prepareUserParam(line, preparedStatement);
                preparedStatement.addBatch();
                i++;
                if (i % 10000 == 0) {
                    preparedStatement.executeBatch();
                    preparedStatement.clearBatch();
                }
            }
            preparedStatement.executeBatch();
            JdbcTemplate.commit(connection);
        } catch (Exception e) {
            e.printStackTrace();
            JdbcTemplate.rollback(connection);
        } finally {
            JdbcTemplate.releaseStatement(preparedStatement);
            JdbcTemplate.releaseConnection(connection);
        }
    }

    private void prepareRecordParam(String line, PreparedStatement statement) {
        String[] info = line.split()
    }

    /**
     * 初始化记录数据
     */
    public void initRecordData() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(RECORD_FILE_NAME);

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try(BufferedReader br = new BufferedReader(new InputStreamReader(stream))) {
            // TODO
            String line;
            while ((line = br.readLine()) != null) {
                String[] info = line.split(";");
            }
        } catch (Exception e) {
            e.printStackTrace();
            JdbcTemplate.rollback(connection);
        } finally {
            JdbcTemplate.releaseStatement(preparedStatement);
            JdbcTemplate.releaseConnection(connection);
        }
    }

    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis();
        new InitData().initBikeData();
        System.out.println("插入时间：" + (System.currentTimeMillis() - start) + "ms");
    }

}
