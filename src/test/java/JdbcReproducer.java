import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Tuple;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;


@ExtendWith(VertxExtension.class)
public class JdbcReproducer {
    protected static final String INSERT = "INSERT INTO vegetables (name, amount) VALUES (?, ?)";
    protected static final String SELECT = "SELECT * FROM vegetables WHERE rowid = ?";
    private static final String CREATE_TABLE_QUERY = "CREATE TABLE vegetables (\n" +
        "  id        NUMBER GENERATED ALWAYS as IDENTITY(START with 1 INCREMENT by 1),\n" +
        "  name      VARCHAR2(40) NOT NULL,\n" +
        "  amount    INT,\n" +
        "  CONSTRAINT vegetables_pk PRIMARY KEY (id))";
    private static final JDBCClient jdbcClient = JDBCClient.create(Vertx.vertx(), connDetailsFromEnv());
    private static final JDBCPool jdbcPool = JDBCPool.pool(Vertx.vertx(), connDetailsFromEnv());

    private static JsonObject connDetailsFromEnv() {
        return new JsonObject()
            .put("provider_class", io.vertx.ext.jdbc.spi.impl.AgroalCPDataSourceProvider.class.getCanonicalName())
            .put("jdbcUrl", System.getenv("DB_JDBC_URL"))
            .put("driverClassName", System.getenv("DB_DRIVER_CLASS"))
            .put("principal", System.getenv("DB_USERNAME"))
            .put("credential", System.getenv("DB_PASSWORD"));
    }

    @BeforeAll
    public static void initDb(VertxTestContext context) {
        jdbcClient.getConnection(sqlConnectionAsyncResult -> {
            if (sqlConnectionAsyncResult.succeeded()) {
                SQLConnection sqlConnection = sqlConnectionAsyncResult.result();
                sqlConnection.execute(CREATE_TABLE_QUERY, voidAsyncResult -> {
                    if (voidAsyncResult.failed()) {
                        voidAsyncResult.cause().printStackTrace();
                    }
                    context.completeNow();
                    sqlConnection.close();
                });
            } else {
                context.failNow(sqlConnectionAsyncResult.cause());
            }
        });
    }

    @AfterAll
    public static void delete(VertxTestContext testContext) {
        jdbcClient.getConnection(sqlConnectionAsyncResult -> {
            if (sqlConnectionAsyncResult.succeeded()) {
                SQLConnection sqlConnection = sqlConnectionAsyncResult.result();
                sqlConnection.execute("DROP TABLE vegetables", voidAsyncResult -> {
                    if (voidAsyncResult.succeeded()) {
                        testContext.completeNow();
                    } else {
                        testContext.failNow(voidAsyncResult.cause());
                    }
                });
            } else {
                testContext.failNow(sqlConnectionAsyncResult.cause());
            }
        });
    }

    @Test
    public void ojdbcClientRowIdTest(VertxTestContext testContext) {
        jdbcClient.getConnection(sqlConnectionAsyncResult -> {
            if (sqlConnectionAsyncResult.succeeded()) {
                SQLConnection connection = sqlConnectionAsyncResult.result();
                connection.updateWithParams(INSERT, new JsonArray().add("pickle").add("5"), updateResultAsyncResult -> {
                    if (updateResultAsyncResult.failed()) {
                        testContext.failNow(updateResultAsyncResult.cause());
                    }
                    assertThat(updateResultAsyncResult.result()).isNotNull();
                    JsonArray rowId = updateResultAsyncResult.result().getKeys();
                    connection.queryWithParams(SELECT, new JsonArray().add(rowId), resultSetAsyncResult -> {
                        if (resultSetAsyncResult.succeeded()) {
                            assertThat(resultSetAsyncResult.result().getRows().size()).isEqualTo(1);
                            JsonObject result = resultSetAsyncResult.result().getRows().get(0);
                            assertThat(result.getString("name")).isEqualTo("pickle");
                            assertThat(result.getInteger("ammount")).isEqualTo(5);
                            assertThat(result.getString("id")).isNotNull();
                            testContext.completeNow();
                        } else {
                            testContext.failNow(resultSetAsyncResult.cause());
                        }
                    });
                });
            } else {
                testContext.failNow(sqlConnectionAsyncResult.cause());
            }
        });
    }

    @Test
    public void ojdbcPoolRowIdTest(VertxTestContext testContext) {
        jdbcPool.getConnection(sqlConnectionAsyncResult -> {
            if (sqlConnectionAsyncResult.succeeded()) {
                SqlConnection connection = sqlConnectionAsyncResult.result();
                connection.preparedQuery(INSERT).execute(Tuple.of("pickle", 5))
                    .onSuccess(rows -> {
                    Row lastInsertId = rows.property(JDBCPool.GENERATED_KEYS);
                    try {
                        byte[] newId = lastInsertId.get(byte[].class, 0);
                        connection.preparedQuery(SELECT).execute(Tuple.of(newId))
                            .onSuccess(rows1 -> {
                                for (Row row : rows1) {

                                }
                                testContext.completeNow();
                            }).onFailure(throwable -> testContext.failNow(throwable));
                    } catch (Exception exception) {
                        testContext.failNow(exception);
                    }
                }).onFailure(throwable -> testContext.failNow(throwable));
            }
        });
    }


}
