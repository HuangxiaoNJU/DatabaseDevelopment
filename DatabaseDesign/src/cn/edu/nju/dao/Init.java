package cn.edu.nju.dao;

import cn.edu.nju.util.JdbcTemplate;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class Init {

    private static final String ALLOCATION_FILE_PATH = "data/分配方案.xls";
    private static final String PHONE_NUMBER_FILE_PATH = "data/电话.txt";

    /**
     * 建表
     */
    public void createTable() {
        String createStudentSQL =
                "CREATE TABLE IF NOT EXISTS student (" +
                    "student_number varchar(16) NOT NULL," +
                    "name           varchar(16) NOT NULL," +
                    "gender         varchar(1)  NOT NULL," +
                    "department     varchar(32) NOT NULL," +
                    "dormitory_name varchar(8)  NOT NULL," +
                    "PRIMARY KEY    (student_number)" +
                ");";
        String createDormitorySQL =
                "CREATE TABLE IF NOT EXISTS dormitory (" +
                    "dormitory_name varchar(8)  NOT NULL," +
                    "campus         varchar(4)  NOT NULL," +
                    "phone_number   varchar(8)  NOT NULL," +
                    "standard_cost  int(11)     NOT NULL," +
                    "PRIMARY KEY    (dormitory_name)" +
                ");";

        Connection connection = null;
        Statement statement = null;
        try {
            connection = JdbcTemplate.getConnection();
            JdbcTemplate.beginTx(connection);
            statement = connection.createStatement();
            statement.execute(createStudentSQL);
            statement.execute(createDormitorySQL);
            JdbcTemplate.commit(connection);
        } catch (Exception e) {
            e.printStackTrace();
            JdbcTemplate.rollback(connection);
        } finally {
            JdbcTemplate.release(statement, connection);
        }
    }

    /**
     * 获取excel表中每行学生信息写入数据库的SQL语句
     * @param row   row
     * @return      sql
     */
    private String getInsertStudentSQLByRow(Row row) {
        String sql = "INSERT INTO student VALUES('";
        sql += row.getCell(1).getStringCellValue() + "','";
        sql += row.getCell(2).getStringCellValue() + "','";
        sql += row.getCell(3).getStringCellValue() + "','";
        sql += row.getCell(0).getStringCellValue() + "','";
        sql += row.getCell(5).getStringCellValue() + "');";
        return sql;
    }

    /**
     * 获取宿舍信息写入数据库的SQL语句
     * @return      sql
     */
    private String getInsertDormitorySQL(Row row, String phoneNumber) {
        String sql = "INSERT INTO dormitory VALUES('";
        sql += row.getCell(5).getStringCellValue() + "','";
        sql += row.getCell(4).getStringCellValue() + "','";
        sql += phoneNumber + "','";
        sql += row.getCell(6).getNumericCellValue() + "');";
        return sql;
    }

    private Map<String, String> getDormitoryPhoneNumber() throws IOException {
        Map<String, String> res = new HashMap<>();
        BufferedReader br = new BufferedReader(new FileReader(PHONE_NUMBER_FILE_PATH));
        String line;
        while ((line = br.readLine()) != null) {
            String[] info = line.split(";");
            if (info.length > 1) {
                res.put(info[0], info[1]);
            }
        }
        return res;
    }

    /**
     * 初始化数据
     */
    private void initData() throws IOException, SQLException {
        Long start = System.currentTimeMillis();
        // 获取宿舍楼对应电话
        Map<String, String> dormitoryPhoneNumber = getDormitoryPhoneNumber();
        FileInputStream stream = new FileInputStream(ALLOCATION_FILE_PATH);
        Workbook wb = new HSSFWorkbook(stream);
        Sheet sheet = wb.getSheetAt(0);
        Iterator<Row> rowIterator = sheet.rowIterator();
        // 剔除第一行
        rowIterator.next();
        Set<String> dormitorySet = new HashSet<>();
        Connection connection = null;
        Statement statement = null;
        try {
            connection = JdbcTemplate.getConnection();
            JdbcTemplate.beginTx(connection);
            statement = connection.createStatement();
            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                statement.addBatch(getInsertStudentSQLByRow(row));
                String dormitoryName = row.getCell(5).getStringCellValue();
                if (!dormitorySet.contains(dormitoryName)) {
                    statement.addBatch(getInsertDormitorySQL(row, dormitoryPhoneNumber.get(dormitoryName)));
                    dormitorySet.add(dormitoryName);
                }
            }
            statement.executeBatch();
            statement.clearBatch();
            JdbcTemplate.commit(connection);
        } catch (Exception e) {
            e.printStackTrace();
            JdbcTemplate.rollback(connection);
        } finally {
            JdbcTemplate.release(statement, connection);
        }

        System.out.println(System.currentTimeMillis() - start);
        stream.close();
    }

    public static void main(String[] args) throws IOException {
        Init init = new Init();
        init.createTable();
        try {
            init.initData();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
