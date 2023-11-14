package org.example;

import java.sql.*;

public class ProductJdbc {

    public static void main(String[] args) {
        String url = "jdbc:postgresql://localhost:5432/new_db";
        String username = "postgres";
        String password = "hr";

        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            String sql = "WITH RankedProducts AS (SELECT p.*, "
                    + "RANK() OVER (PARTITION BY p.productCategoryId ORDER BY p.salesCount DESC, p.productCreateDate DESC) AS rnk "
                    + "FROM products p "
                    + "JOIN categories c ON p.productCategoryId = c.id "
                    + "WHERE p.productCreateDate > '2020-01-01') "
                    + "SELECT id, productCategoryId, productName, productCreateDate, salesCount "
                    + "FROM RankedProducts "
                    + "WHERE rnk = 1";

            try (PreparedStatement preparedStatement = connection.prepareStatement(sql);
                 ResultSet resultSet = preparedStatement.executeQuery()) {

                while (resultSet.next()) {
                    int id = resultSet.getInt("id");
                    int productCategoryId = resultSet.getInt("productCategoryId");
                    String productName = resultSet.getString("productName");
                    String productCreateDate = resultSet.getString("productCreateDate");
                    int salesCount = resultSet.getInt("salesCount");

                    System.out.println("id: " + id + ", productCategoryId: " + productCategoryId +
                            ", productName: " + productName + ", productCreateDate: " +
                            productCreateDate + ", salesCount: " + salesCount);
                }
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

}
