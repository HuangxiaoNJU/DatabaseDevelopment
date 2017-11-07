package dao;

import util.JdbcTemplate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection", "ConstantConditions"})
public class Solve {

    private static Solve solve;

    public static Solve getInstance() {
        if (solve == null) {
            solve = new Solve();
        }
        return solve;
    }

    private Solve() {}

    /**
     * 查询王小星同学所在宿舍楼的所有院系
     */
    public void search() {
        Connection connection = JdbcTemplate.getConnection();
        PreparedStatement stmt = null;
        try {
            stmt = connection.prepareStatement(
                    "select distinct(department) " +
                    "from student s " +
                    "where dormitory_name = (select dormitory_name from student where name=?)");
            stmt.setString(1, "王小星");
            ResultSet resultSet = stmt.executeQuery();
            System.out.println("王小星同学所在宿舍楼所有院系：");
            while (resultSet.next()) {
                System.out.println("\t" + resultSet.getString("department"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JdbcTemplate.releaseStatement(stmt);
            JdbcTemplate.releaseConnection(connection);
        }
    }

    /**
     * 修改陶园1舍住宿费为1200元
     */
    public void modifyCost() {
        Connection connection = JdbcTemplate.getConnection();
        PreparedStatement stmt = null;
        try {
            stmt = connection.prepareStatement(
                    "update dormitory set standard_cost = ? where dormitory_name = ?");
            stmt.setInt(1, 1200);
            stmt.setString(2, "陶园1舍");
            stmt.execute();
            System.out.println("陶园一舍住宿费修改完成");
        } catch (SQLException e) {
            e.printStackTrace();
            JdbcTemplate.rollback(connection);
        } finally {
            JdbcTemplate.releaseStatement(stmt);
            JdbcTemplate.releaseConnection(connection);
        }
    }

    /**
     * 软件学院男女研究生互换宿舍楼
     */
    public void exchangeDormitory() {
        Connection connection = JdbcTemplate.getConnection();
        PreparedStatement stmt = null;
        try {
            stmt = connection.prepareStatement(
                    "SELECT DISTINCT (dormitory_name) FROM student WHERE department=? AND gender=?");
            stmt.setString(1, "软件学院");
            stmt.setString(2, "男");
            ResultSet resultSet = stmt.executeQuery();
            resultSet.next();
            String maleDormitory = resultSet.getString("dormitory_name");
            stmt.setString(1, "软件学院");
            stmt.setString(2, "女");
            resultSet = stmt.executeQuery();
            resultSet.next();
            String femaleDormitory = resultSet.getString("dormitory_name");

            stmt = connection.prepareStatement("UPDATE student SET dormitory_name=? WHERE department=? AND gender=?");
            stmt.setString(1, femaleDormitory);
            stmt.setString(2, "软件学院");
            stmt.setString(3, "男");
            stmt.addBatch();
            stmt.setString(1, maleDormitory);
            stmt.setString(2, "软件学院");
            stmt.setString(3, "女");
            stmt.addBatch();
            stmt.executeBatch();

        } catch (SQLException e) {
            e.printStackTrace();
            JdbcTemplate.rollback(connection);
        } finally {
            JdbcTemplate.releaseStatement(stmt);
            JdbcTemplate.releaseConnection(connection);
        }
    }

}
