package brave.extension;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.sql.DataSource;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(DatabaseExtension.class)
public class DatabaseExtensionTest {

    static DataSource talaAcp;
    static HikariDataSource talariaSeller;

    @BeforeAll
    static void setupDatabase() {
        HikariConfig talaAcpConfig = new HikariConfig();
        talaAcpConfig.setJdbcUrl("jdbc:h2:mem:tala_acp;MODE=MYSQL;DB_CLOSE_DELAY=-1");
        talaAcpConfig.setUsername("un");
        talaAcpConfig.setPassword("pw");
        talaAcp = new HikariDataSource(talaAcpConfig);


        HikariConfig talariaSellerConfig = new HikariConfig();
        talariaSellerConfig.setJdbcUrl("jdbc:h2:mem:talaria_seller;MODE=MYSQL;DB_CLOSE_DELAY=-1");
        talariaSellerConfig.setUsername("un");
        talariaSellerConfig.setPassword("pw");
        talariaSeller = new HikariDataSource(talariaSellerConfig);
    }

    @AfterAll
    static void destroyDatabase() {

    }

    @Test
    void databaseLoading_shouldBeOk(DatabaseStub datasourceStub) throws SQLException {
        datasourceStub.load("tala_acp", "stubs/databaseLoading/tala_acp");
        datasourceStub.load("talaria_seller", "stubs/databaseLoading/talaria_seller");

        try (Connection connection = talaAcp.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM tala_acp.users")) {
                ResultSet resultSet = statement.executeQuery();
                assertTrue(resultSet.next());
                long userId = resultSet.getLong("id");
                assertEquals(20412L, userId);
            }
        }

        try (Connection connection = talariaSeller.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM talaria_seller.seller")) {
                ResultSet resultSet = statement.executeQuery();
                assertTrue(resultSet.next());
                long userId = resultSet.getLong("id");
                assertEquals(22440L, userId);
            }
        }

        datasourceStub.unload("tala_acp", "stubs/databaseLoading/tala_acp");
        datasourceStub.unload("talaria_seller", "stubs/databaseLoading/talaria_seller");

        try (Connection connection = talaAcp.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM tala_acp.users")) {
                ResultSet resultSet = statement.executeQuery();
                assertFalse(resultSet.next());
            }
        }

        try (Connection connection = talariaSeller.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM talaria_seller.seller")) {
                ResultSet resultSet = statement.executeQuery();
                assertFalse(resultSet.next());
            }
        }
    }

}
