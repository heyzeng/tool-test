package org.example;

import java.sql.*;


public class MySQLToClickHouseConverter {
    public static void main(String[] args) {

        String mysqlUrl = "jdbc:mysql://localhost:3306/test?characterEncoding=utf-8&useSSL=false&&allowPublicKeyRetrieval=true&serverTimezone=UTC";
        String mysqlUsername = "jude";
        String mysqlPassword = "12345";
        //todo : 表名，库名，主键索引变为传参
        String param1 = null;
        String param2 = null;

        try {
            // 加载MySQL驱动程序
            Class.forName("com.mysql.cj.jdbc.Driver");
            // 连接MySQL数据库
            Connection mysqlConnection = DriverManager.getConnection(mysqlUrl, mysqlUsername, mysqlPassword);
            DatabaseMetaData mysqlMetaData = mysqlConnection.getMetaData();

            // 获取MySQL表的元数据信息
            ResultSet mysqlTables = mysqlMetaData.getTables(null, null, null, new String[]{"TABLE"});

            while (mysqlTables.next()) {
                String tableName = mysqlTables.getString("TABLE_NAME");

                // 获取MySQL表的列的元数据信息
                ResultSet mysqlColumns = mysqlMetaData.getColumns(null, null, tableName, null);
                StringBuilder clickhouseColumns = new StringBuilder();

                while (mysqlColumns.next()) {
                    String columnName = mysqlColumns.getString("COLUMN_NAME");
                    String columnType = mysqlColumns.getString("TYPE_NAME");
                    int columnSize = mysqlColumns.getInt("COLUMN_SIZE");

                    // 根据MySQL列的类型和大小生成ClickHouse列的类型
                    String clickhouseType = generateClickHouseType(columnType, columnSize);

                    if (clickhouseColumns.length() > 0) {
                        clickhouseColumns.append(", ");
                    }
                    clickhouseColumns.append(columnName).append(" ").append(clickhouseType);
                }

                // 创建分布式ClickHouse表
                String createDistributedTable = "CREATE TABLE " + tableName
                        + " on cluster ck_00_1repl " + " (" + clickhouseColumns.toString() + ")"
                        + " ENGINE = Distributed('ck_00_1repl', 'realtime_db', "
                        + "'" + "loc_" + tableName + "', " + "hiveHash(Fuid));";
                System.out.println(createDistributedTable);

                // 创建clickhouse本地表
                String createLocalTable = "CREATE TABLE " + "loc_" + tableName
                        + " on cluster ck_00_1repl " + " (" + clickhouseColumns.toString() + ")"
                        + " ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/"
                        + "loc_" + tableName + "'" + ", '{replica}')"
                        + " PARTITION BY toYYYYMMDD(Fmodify_time)"
                        + " ORDER BY (Fuid, Fmodify_time)"
                        + " TTL Fmodify_time + toIntervalDay(7)"
                        + " SETTINGS index_granularity = 8192;";
                System.out.println(createLocalTable);
            }

            // 关闭连接
            mysqlConnection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    // mysql数据类型转换为clickhouse数据类型
    private static String generateClickHouseType(String mysqlType, int mysqlSize) {
        // 根据MySQL列的类型和大小生成ClickHouse列的类型
        if (mysqlType.equalsIgnoreCase("varchar")) {
            return "String";
        } else if (mysqlType.equalsIgnoreCase("tinyint")) {
            return "Int8";
        } else if (mysqlType.equalsIgnoreCase("int")) {
            return "Int32";
        } else if (mysqlType.equalsIgnoreCase("bigint")) {
            return "Int64";
        } else if (mysqlType.equalsIgnoreCase("decimal")) {
            return "Decimal";
        } else if (mysqlType.equalsIgnoreCase("datetime")) {
            return "DateTime";
        } else if (mysqlType.equalsIgnoreCase("double")) {
            return "Float64";
        } else if (mysqlType.equalsIgnoreCase("float")) {
            return "float32";
        } else if (mysqlType.equalsIgnoreCase("timestamp")) {
            return "DateTime";
        } else if (mysqlType.equalsIgnoreCase("text")) {
            return "String";
        } else if (mysqlType.equalsIgnoreCase("longtext")) {
            return "String";
        } else {
            return "String";
        }
    }
}