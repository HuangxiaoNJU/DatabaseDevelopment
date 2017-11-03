package database;

import cn.edu.nju.util.JdbcTemplate;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class DatabaseConnectTest {

    @Test
    public void testConnect() {
        assertNotNull(JdbcTemplate.getConnection());
    }

}
