package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TestMysqlJDBC {
    public static void main(String[] args) {
        // 定义数据库连接信息
        String url = "jdbc:mysql://localhost:3306/guigu-auth";
        String username = "jude";
        String password = "12345";

        // 定义SQL查询语句
        String query = "SELECT * FROM sys_dept";

        try {
            // 加载MySQL驱动程序
            Class.forName("com.mysql.cj.jdbc.Driver");

            // 连接到MySQL数据库
            Connection connection = DriverManager.getConnection(url, username, password);

            // 创建Statement对象
            Statement statement = connection.createStatement();

            // 执行查询语句
            ResultSet resultSet = statement.executeQuery(query);

            // 遍历结果集并输出数据
            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String name = resultSet.getString("name");
                int age = resultSet.getInt("sort_value");

                System.out.println("ID: " + id + ", Name: " + name + ", sort_value: " + age);
            }

            // 关闭连接
            resultSet.close();
            statement.close();
            connection.close();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }
}

