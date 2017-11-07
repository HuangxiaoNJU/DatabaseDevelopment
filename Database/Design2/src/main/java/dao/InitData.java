package dao;

import util.JdbcTemplate;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Set;

@SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection", "ConstantConditions"})
public class InitData {

    private static InitData initData;

    private InitData() {}

    public static InitData getInstance() {
        if (initData == null) {
            initData = new InitData();
        }
        return initData;
    }

    private static final String USER_FILE_NAME = "user.txt";
    private static final String BIKE_FILE_NAME = "bike.txt";
    private static final String RECORD_FILE_NAME = "record.txt";

    private static final int BATCH_SIZE = 10000;

    private static final DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy/MM/dd-HH:mm:ss");
    private static final DateTimeFormatter STANDARD_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

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
        statement.addBatch();
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
                i++;
                if (i % BATCH_SIZE == 0) {
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

    private Set<Integer> balanceNotEnoughUserIdSet = new HashSet<>();

    private void prepareRecordParam(String line, PreparedStatement insertRecordStmt, PreparedStatement selectBalanceStmt, PreparedStatement updateUserStmt)
            throws SQLException, ParseException {
        String[] info = line.split(";");
        int userId = Integer.valueOf(info[0]);
        if (balanceNotEnoughUserIdSet.contains(userId)) {
            return;
        }

        LocalDateTime departTime = LocalDateTime.parse(info[3], DATE_TIME_FORMAT);
        LocalDateTime arriveTime = LocalDateTime.parse(info[5], DATE_TIME_FORMAT);
        // 计算骑车费用
        double cost = Double.min(Duration.between(departTime, arriveTime).toMinutes() / 30 + 1.0, 4.0);

        // 计算骑车后用户余额
        selectBalanceStmt.setInt(1, userId);
        ResultSet resultSet = selectBalanceStmt.executeQuery();
        double newBalance;
        if (resultSet.next()) {
            newBalance = resultSet.getDouble("balance") - cost;
        } else {
            System.out.println(userId + "\t不存在");
            throw new SQLException();
        }

        if (newBalance <= 0) {
            balanceNotEnoughUserIdSet.add(userId);
            System.out.println(userId + "\t余额不足");
        }
        // 更新用户余额
        updateUserStmt.setDouble(1, newBalance);
        updateUserStmt.setInt(2, userId);
        updateUserStmt.addBatch();

        // 插入骑车记录
        insertRecordStmt.setInt(1, Integer.valueOf(info[0]));
        insertRecordStmt.setInt(2, Integer.valueOf(info[1]));
        insertRecordStmt.setString(3, info[2]);
        insertRecordStmt.setString(4, departTime.format(STANDARD_FORMAT));
        insertRecordStmt.setString(5, info[4]);
        insertRecordStmt.setString(6, arriveTime.format(STANDARD_FORMAT));
        insertRecordStmt.setDouble(7, cost);
        insertRecordStmt.addBatch();
    }

    /**
     * 初始化记录数据
     */
    public void initRecordData() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(RECORD_FILE_NAME);

        Connection connection = null;
        PreparedStatement insertRecordStmt = null;
        PreparedStatement updateUserStmt = null;
        PreparedStatement selectBalanceStmt = null;
        try(BufferedReader br = new BufferedReader(new InputStreamReader(stream))) {
            connection = JdbcTemplate.getConnection();
            JdbcTemplate.beginTx(connection);
            insertRecordStmt = connection.prepareStatement(
                    "INSERT INTO record" +
                         "(user_id, bike_id, departure, depart_time, destination, arrive_time, cost) " +
                         "VALUES (?,?,?,?,?,?,?)");
            updateUserStmt = connection.prepareStatement(
                    "UPDATE user SET balance=? WHERE id=?");
            selectBalanceStmt = connection.prepareStatement(
                    "SELECT balance FROM user WHERE id=?");
            int i = 0;
            String line;
            while ((line = br.readLine()) != null) {
                i++;
                prepareRecordParam(line, insertRecordStmt, selectBalanceStmt, updateUserStmt);
                if (i % BATCH_SIZE == 0) {
                    insertRecordStmt.executeBatch();
                    insertRecordStmt.clearBatch();
                    updateUserStmt.executeBatch();
                    updateUserStmt.clearBatch();
                }
            }
            insertRecordStmt.executeBatch();
            updateUserStmt.executeBatch();
            JdbcTemplate.commit(connection);
        } catch (Exception e) {
            e.printStackTrace();
            JdbcTemplate.rollback(connection);
        } finally {
            JdbcTemplate.releaseStatement(insertRecordStmt);
            JdbcTemplate.releaseStatement(updateUserStmt);
            JdbcTemplate.releaseStatement(selectBalanceStmt);
            JdbcTemplate.releaseConnection(connection);
        }
    }

    /**
     * 添加用户住址
     */
    public void addUserHome() {
        Connection connection = null;
        PreparedStatement selectStmt = null;
        PreparedStatement updateStmt = null;
        try {
            connection = JdbcTemplate.getConnection();
            JdbcTemplate.beginTx(connection);
            updateStmt = connection.prepareStatement(
                    "UPDATE user SET home=? WHERE id=?");
            selectStmt = connection.prepareStatement(
                    "SELECT user_id, destination FROM " +
                            "(SELECT user_id, destination, count(*) as times " +
                            "FROM record " +
                            "WHERE date_format(arrive_time, '%H') between 18 and 24 " +
                            "GROUP BY user_id, destination " +
                            "order by times desc) t " +
                         "group by user_id");
            ResultSet resultSet = selectStmt.executeQuery();
            while (resultSet.next()) {
                int userId = resultSet.getInt("user_id");
                String home = resultSet.getString("destination");
                updateStmt.setString(1, home);
                updateStmt.setInt(2, userId);
                updateStmt.addBatch();
            }
            updateStmt.executeBatch();
            JdbcTemplate.commit(connection);
        } catch (Exception e) {
            e.printStackTrace();
            JdbcTemplate.rollback(connection);
        } finally {
            JdbcTemplate.releaseStatement(selectStmt);
            JdbcTemplate.releaseStatement(updateStmt);
            JdbcTemplate.releaseConnection(connection);
        }
    }

    /**
     * 添加repair数据
     */
    public void addBikeRepair() {
        Connection connection = null;
        PreparedStatement insertStmt = null;
        PreparedStatement selectStmt = null;
        try {
            connection = JdbcTemplate.getConnection();
            JdbcTemplate.beginTx(connection);
            selectStmt = connection.prepareStatement(
                    "select r.bike_id, r.destination from " +
                            "(select bike_id, max(arrive_time) as last_time " +
                            "from record " +
                            "group by bike_id, date_format(arrive_time, '%Y-%m') " +
                            "having sum(minute(arrive_time-depart_time)) > 200 * 60) t, record r " +
                          "where r.bike_id = t.bike_id and t.last_time = r.arrive_time");
            insertStmt = connection.prepareStatement(
                    "INSERT INTO repair(bike_id, last_location) VALUES(?,?)");
            ResultSet resultSet = selectStmt.executeQuery();
            while (resultSet.next()) {
                insertStmt.setInt(1, resultSet.getInt("bike_id"));
                insertStmt.setString(2, resultSet.getString("destination"));
                insertStmt.addBatch();
            }
            insertStmt.executeBatch();
            JdbcTemplate.commit(connection);
        } catch (Exception e) {
            e.printStackTrace();
            JdbcTemplate.rollback(connection);
        } finally {
            JdbcTemplate.releaseStatement(selectStmt);
            JdbcTemplate.releaseStatement(insertStmt);
            JdbcTemplate.releaseConnection(connection);
        }
    }

}
