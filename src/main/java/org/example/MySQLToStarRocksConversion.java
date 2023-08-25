package org.example;

import java.sql.*;
import java.util.*;

/**
 * Author:Jude
 * Date:2023-07-20 10:44
 */
public class MySQLToStarRocksConversion {
    // MySQL和StarRocks的数据类型映射关系
    private static final Map<String, String> dataTypeMapping = new HashMap<>();

    static {
        dataTypeMapping.put("int", "INT");
        dataTypeMapping.put("tinyint", "TINYINT");
        dataTypeMapping.put("smallint", "SMALLINT");
        dataTypeMapping.put("bigint", "BIGINT");
        dataTypeMapping.put("float", "FLOAT");
        dataTypeMapping.put("double", "DOUBLE,");
        dataTypeMapping.put("decimal", "DECIMAL,");
        dataTypeMapping.put("varchar", "STRING");
        dataTypeMapping.put("datetime", "DATETIME");
        dataTypeMapping.put("date", "DATE");
    }

    public static void main(String[] args) throws SQLException {

        String url = "jdbc:mysql://localhost:3306/test?characterEncoding=utf-8&useSSL=false&&allowPublicKeyRetrieval=true&serverTimezone=UTC";
        String username = "jude";
        String password = "12345";

        Connection conn = DriverManager.getConnection(url, username, password);

        String tableName = "t_pipeline_base_info";

        // 获取表结构元数据
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet columnsResultSet = metaData.getColumns(null, null, tableName, null);


        StringBuilder createTableSql = new StringBuilder("CREATE TABLE ");
        createTableSql.append(tableName).append(" (");

        while (columnsResultSet.next()) {
            String columnName = columnsResultSet.getString("COLUMN_NAME");
            String typeName = columnsResultSet.getString("TYPE_NAME").toLowerCase();

            createTableSql.append(columnName)
                    .append(" ")
                    .append(dataTypeMapping.get(typeName))
                    .append(", ");
        }

        // 去掉最后一个逗号和空格
        createTableSql.setLength(createTableSql.length() - 2);
        createTableSql.append(");");

        System.out.println("StarRocks Create Table SQL: " + createTableSql.toString());

        columnsResultSet.close();
        conn.close();
    }
}
