package dao;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import util.JdbcTemplate;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

@SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection", "ConstantConditions"})
public class Init {

    private static Init init;

    public static Init getInstance() {
        if (init == null) {
            init = new Init();
        }
        return init;
    }

    private Init() {}

    private static final String ALLOCATION_FILE_NAME = "分配方案.xls";
    private static final String PHONE_NUMBER_FILE_NAME = "电话.txt";

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
            JdbcTemplate.releaseStatement(statement);
            JdbcTemplate.releaseConnection(connection);
        }
    }

    /**
     * 插入学生数据
     * PreparedStatement设置参数
     */
    private void prepareStudentParam(Row row, PreparedStatement statement) throws SQLException {
        statement.setString(1, row.getCell(1).getStringCellValue());
        statement.setString(2, row.getCell(2).getStringCellValue());
        statement.setString(3, row.getCell(3).getStringCellValue());
        statement.setString(4, row.getCell(0).getStringCellValue());
        statement.setString(5, row.getCell(5).getStringCellValue());
    }

    /**
     * 插入宿舍数据
     * PreparedStatement设置参数
     */
    private void preparedDormitoryParam(Row row, String phoneNumber, PreparedStatement statement) throws SQLException {
        statement.setString(1, row.getCell(5).getStringCellValue());
        statement.setString(2, row.getCell(4).getStringCellValue());
        statement.setString(3, String.valueOf(row.getCell(6).getNumericCellValue()));
        statement.setString(4, phoneNumber);
    }

    private Map<String, String> getDormitoryPhoneNumber() throws IOException {
        Map<String, String> res = new HashMap<>();
        BufferedReader br = new BufferedReader(
                new InputStreamReader(getClass().getClassLoader().getResourceAsStream(PHONE_NUMBER_FILE_NAME)));
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
    public void initData() throws IOException {
        // 获取宿舍楼对应电话
        Map<String, String> dormitoryPhoneNumber = getDormitoryPhoneNumber();
        InputStream stream = getClass().getClassLoader().getResourceAsStream(ALLOCATION_FILE_NAME);
        Workbook wb = new HSSFWorkbook(stream);
        Sheet sheet = wb.getSheetAt(0);
        Iterator<Row> rowIterator = sheet.rowIterator();
        // 剔除第一行
        rowIterator.next();
        Set<String> dormitorySet = new HashSet<>();
        Connection connection = null;
        PreparedStatement studentStatement = null;
        PreparedStatement dormitoryStatement = null;
        try {
            connection = JdbcTemplate.getConnection();
            JdbcTemplate.beginTx(connection);
            studentStatement = connection.prepareStatement(
                    "INSERT INTO student(student_number, name, gender, department, dormitory_name) VALUES (?,?,?,?,?)");
            dormitoryStatement = connection.prepareStatement(
                    "INSERT INTO dormitory(dormitory_name, campus, standard_cost, phone_number) VALUES (?,?,?,?)");
            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                prepareStudentParam(row, studentStatement);
                studentStatement.addBatch();
                String dormitoryName = row.getCell(5).getStringCellValue();
                if (!dormitorySet.contains(dormitoryName)) {
                    preparedDormitoryParam(row, dormitoryPhoneNumber.get(dormitoryName), dormitoryStatement);
                    dormitoryStatement.addBatch();
                    dormitorySet.add(dormitoryName);
                }
            }
            studentStatement.executeBatch();
            dormitoryStatement.executeBatch();
            JdbcTemplate.commit(connection);
        } catch (Exception e) {
            e.printStackTrace();
            JdbcTemplate.rollback(connection);
        } finally {
            JdbcTemplate.releaseStatement(studentStatement);
            JdbcTemplate.releaseStatement(dormitoryStatement);
            JdbcTemplate.releaseConnection(connection);
            stream.close();
        }
    }


}
