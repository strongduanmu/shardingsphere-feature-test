package com.strongduanmu.feature.sharding.schema;

import com.strongduanmu.datasource.jdbc.SchemaFeatureType;
import com.strongduanmu.datasource.jdbc.YamlDataSourceFactory;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Sharding schema based SQL statement test.
 */
@Slf4j
public class ShardingSchemaBasedSQLStatementTest {
    
    private Connection connection;
    
    @Before
    public void setUp() throws SQLException, IOException {
        DataSource dataSource = YamlDataSourceFactory.newInstance(SchemaFeatureType.SHARDING_DATABASES_AND_TABLES);
        connection = dataSource.getConnection();
    }
    
    @Test
    public void testSchema() throws SQLException {
        executeSchema("DROP SCHEMA IF EXISTS test_schema_1 CASCADE;");
        executeSchema("CREATE SCHEMA test_schema_1;");
        executeSchema("ALTER SCHEMA test_schema_1 RENAME TO test_schema_2;");
        executeSchema("ALTER SCHEMA test_schema_2 RENAME TO test_schema_1;");
        executeSchema("DROP SCHEMA IF EXISTS test_schema_1 CASCADE;");
    }
    
    @Test
    public void testTable() throws SQLException {
//        创建表测试
//        executeTable("DROP TABLE IF EXISTS t_order;", false);
//        executeTable("CREATE TABLE t_order(order_id BIGINT, user_id INT, content VARCHAR);", false);
//        executeTable("INSERT INTO t_order VALUES(1, 1, 'TEST1');", false);
//        executeTable("INSERT INTO t_order(user_id, content) VALUES(1, 'TEST1');", false);
//        executeTable("SELECT * FROM t_order;", true);
//        
//        executeTable("DROP TABLE IF EXISTS t_order_item;", false);
//        executeTable("CREATE TABLE t_order_item(order_item_id BIGINT, order_id BIGINT, user_id INT, content VARCHAR);", false);
//        executeTable("INSERT INTO t_order_item VALUES(1, 1, 1, 'TEST1');", false);
//        executeTable("INSERT INTO t_order_item(order_id, user_id, content) VALUES(1, 1, 'TEST1');", false);
//        executeTable("SELECT * FROM t_order_item;", true);
//    
//        executeTable("DROP TABLE IF EXISTS t_single;", false);
//        executeTable("CREATE TABLE t_single(single_id BIGINT, content VARCHAR);", false);
//        executeTable("INSERT INTO t_single VALUES(1, 'TEST1');", false);
//        executeTable("INSERT INTO t_single(single_id, content) VALUES (2, 'TEST2');", false);
//        executeTable("SELECT * FROM t_single;", true);
//    
//        executeSchema("DROP SCHEMA IF EXISTS test_schema_1 CASCADE;");
//        executeSchema("CREATE SCHEMA test_schema_1;");
//    
//        executeTable("DROP TABLE IF EXISTS test_schema_1.t_order;", false);
//        executeTable("CREATE TABLE test_schema_1.t_order(order_id BIGINT, user_id INT, content VARCHAR, create_time timestamp);", false);
//        executeTable("INSERT INTO test_schema_1.t_order VALUES(1, 1, 'TEST1', NOW());", false);
//        executeTable("INSERT INTO test_schema_1.t_order(user_id, content, create_time) VALUES(1, 'TEST1', NOW());", false);
//        executeTable("SELECT * FROM test_schema_1.t_order;", true);
//    
//        executeTable("DROP TABLE IF EXISTS test_schema_1.t_order_item;", false);
//        executeTable("CREATE TABLE test_schema_1.t_order_item(order_item_id BIGINT, order_id BIGINT, user_id INT, content VARCHAR, create_time timestamp);", false);
//        executeTable("INSERT INTO test_schema_1.t_order_item VALUES(1, 1, 1, 'TEST1', NOW());", false);
//        executeTable("INSERT INTO test_schema_1.t_order_item(order_id, user_id, content, create_time) VALUES(1, 1, 'TEST1', NOW());", false);
//        executeTable("SELECT * FROM test_schema_1.t_order_item;", true);
//    
//        executeTable("DROP TABLE IF EXISTS test_schema_1.t_single;", false);
//        executeTable("CREATE TABLE test_schema_1.t_single(single_id BIGINT, content VARCHAR);", false);
//        executeTable("INSERT INTO test_schema_1.t_single VALUES(1, 'TEST1');", false);
//        executeTable("INSERT INTO test_schema_1.t_single(single_id, content) VALUES (2, 'TEST2');", false);
//        executeTable("SELECT * FROM test_schema_1.t_single;", true);
    
//        修改表测试
//        executeTable("ALTER TABLE t_order ADD COLUMN status varchar(30);", false);
//        executeTable("SELECT * FROM t_order;", true);
//    
//        executeTable("ALTER TABLE test_schema_1.t_order ADD COLUMN status varchar(30);", false);
//        executeTable("SELECT * FROM test_schema_1.t_order;", true);
//    
//        executeTable("ALTER TABLE t_order DROP COLUMN status;", false);
//        executeTable("SELECT * FROM t_order;", true);
//    
//        executeTable("ALTER TABLE test_schema_1.t_order DROP COLUMN status;", false);
//        executeTable("SELECT * FROM test_schema_1.t_order;", true);
        
//        清空表测试
//        executeTable("TRUNCATE TABLE t_order;", false);
//        executeTable("SELECT * FROM t_order;", true);
//    
//        executeTable("TRUNCATE TABLE test_schema_1.t_order;", false);
//        executeTable("SELECT * FROM test_schema_1.t_order;", true);
//    
//        executeTable("DROP TABLE t_order;", false);
//        executeTable("DROP TABLE test_schema_1.t_order;", false);
        
//        创建索引测试
//        executeTable("CREATE INDEX ON t_order(order_id);", false);
//        executeTable("CREATE INDEX ON test_schema_1.t_order(order_id);", false);
//        
//        executeTable("CREATE INDEX t_order_id_idx ON t_order (order_id);", false);
//        executeTable("CREATE INDEX t_order_id_idx ON test_schema_1.t_order (order_id);", false);
    
//        executeTable("ALTER INDEX t_order_id_idx RENAME TO t_order_idx;", false);
//        executeTable("ALTER INDEX t_order_idx RENAME TO t_order_id_idx;", false);
//    
//        executeTable("ALTER INDEX test_schema_1.t_order_id_idx RENAME TO t_order_idx;", false);
//        executeTable("ALTER INDEX test_schema_1.t_order_idx RENAME TO t_order_id_idx;", false);
    
//        executeTable("DROP INDEX t_order_id_idx;", false);
//        executeTable("DROP INDEX test_schema_1.t_order_id_idx;", false);
    
//        executeTable("TRUNCATE TABLE t_order;", false);
//        executeTable("INSERT INTO t_order VALUES(1, 1, 'TEST1');", false);
//        executeTable("INSERT INTO t_order(user_id, content) VALUES(1, 'TEST1');", false);
//        executeTable("SELECT * FROM t_order;", true);
    
//        executeTable("TRUNCATE TABLE t_order_item;", false);
//        executeTable("INSERT INTO t_order_item VALUES(1, 1, 1, 'TEST1');", false);
//        executeTable("INSERT INTO t_order_item(order_id, user_id, content) VALUES(1, 1, 'TEST1');", false);
//        executeTable("SELECT * FROM t_order_item;", true);
    
//        executeTable("TRUNCATE TABLE test_schema_1.t_order;", false);
//        executeTable("INSERT INTO test_schema_1.t_order VALUES(1, 1, 'TEST1', NOW());", false);
//        executeTable("INSERT INTO test_schema_1.t_order(user_id, content, create_time) VALUES(1, 'TEST1', NOW());", false);
//        executeTable("SELECT * FROM test_schema_1.t_order;", true);
    
//        executeTable("TRUNCATE TABLE test_schema_1.t_order_item;", false);
//        executeTable("INSERT INTO test_schema_1.t_order_item VALUES(1, 1, 1, 'TEST1', NOW());", false);
//        executeTable("INSERT INTO test_schema_1.t_order_item(order_id, user_id, content, create_time) VALUES(1, 1, 'TEST1', NOW());", false);
//        executeTable("SELECT * FROM test_schema_1.t_order_item;", true);
    
//        executeTable("UPDATE t_order SET content = 'TEST11' WHERE order_id = 1;", false);
//        executeTable("SELECT * FROM t_order WHERE order_id = 1;", true);
//    
//        executeTable("UPDATE t_order_item SET content = 'TEST11' WHERE order_item_id = 1;", false);
//        executeTable("SELECT * FROM t_order_item WHERE order_item_id = 1;", true);
//    
//        executeTable("UPDATE test_schema_1.t_order SET content = 'TEST11', create_time = NOW() WHERE order_id = 1;", false);
//        executeTable("SELECT * FROM test_schema_1.t_order WHERE order_id = 1;", true);
//    
//        executeTable("UPDATE test_schema_1.t_order_item SET content = 'TEST11', create_time = NOW() WHERE order_item_id = 1;", false);
//        executeTable("SELECT * FROM test_schema_1.t_order_item WHERE order_item_id = 1;", true);
    
//        executeTable("DELETE FROM t_order WHERE order_id = 1;", false);
//        executeTable("SELECT * FROM t_order;", true);
//    
//        executeTable("DELETE FROM t_order_item WHERE order_item_id = 1;", false);
//        executeTable("SELECT * FROM t_order_item;", true);
//    
//        executeTable("DELETE FROM test_schema_1.t_order WHERE order_id = 1;", false);
//        executeTable("SELECT * FROM test_schema_1.t_order;", true);
//    
//        executeTable("DELETE FROM test_schema_1.t_order_item WHERE order_item_id = 1;", false);
//        executeTable("SELECT * FROM test_schema_1.t_order_item;", true);
    
        executeTable("SELECT * FROM t_order WHERE user_id = 1 AND order_id = 1;", true);
        executeTable("SELECT * FROM t_order_item WHERE user_id = 1 AND order_id = 1;", true);
    
        executeTable("SELECT * FROM test_schema_1.t_order WHERE user_id = 1 AND order_id = 1;", true);
        executeTable("SELECT * FROM test_schema_1.t_order_item WHERE user_id = 1 AND order_id = 1;", true);
    
        executeTable("SELECT * FROM t_order o INNER JOIN t_order_item i ON o.user_id = i.user_id AND o.order_id = i.order_id;", true);
        executeTable("SELECT * FROM test_schema_1.t_order o INNER JOIN test_schema_1.t_order_item i ON o.user_id = i.user_id AND o.order_id = i.order_id;", true);
    
//        executeTable("SELECT * FROM t_order o INNER JOIN test_schema_1.t_order_item i ON o.user_id = i.user_id AND o.order_id = i.order_id;", true);
        executeTable("SELECT * FROM t_single s1 INNER JOIN test_schema_1.t_single s2 ON s1.single_id = s2.single_id;", true);
    }
    
    private void executeTable(final String sql, final boolean isQuery) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.execute();
            if (!isQuery) {
                return;
            }
            ResultSet resultSet = preparedStatement.getResultSet();
            ResultSetMetaData metaData = resultSet.getMetaData();
            while (resultSet.next()) {
                StringBuilder builder = new StringBuilder();
                for (int index = 0; index < metaData.getColumnCount(); index++) {
                    builder.append(metaData.getColumnLabel(index + 1)).append(": ").append(resultSet.getString(index + 1)).append(", ");
                }
                System.out.println(builder);
            }
        }
    }
    
    private void executeSchema(final String sql) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.execute();
            ResultSet schemas = connection.getMetaData().getSchemas();
            ResultSetMetaData metaData = schemas.getMetaData();
            while (schemas.next()) {
                StringBuilder builder = new StringBuilder();
                for (int index = 0; index < metaData.getColumnCount(); index++) {
                    builder.append(metaData.getColumnLabel(index + 1)).append(": ").append(schemas.getString(index + 1)).append(", ");
                }
                System.out.println(builder);
            }   
        }
    }
    
    @After
    public void cleanUp() throws SQLException {
        connection.close();
    }
}
