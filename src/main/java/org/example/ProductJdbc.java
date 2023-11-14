package org.example;

import java.sql.*;

public class ProductJdbc {

    public static void main(String[] args) {
        String url = "jdbc:postgresql://localhost:5432/new_db";
        String username = "postgres";
        String password = "hr";

        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            String sql = "WITH RankedProducts AS (SELECT p.*, "
                    + "RANK() OVER (PARTITION BY p.product_category_id "
                    + "ORDER BY p.sales_count DESC, p.product_create_date DESC) AS rnk "
                    + "FROM products p "
                    + "JOIN categories c ON p.product_category_id = c.id "
                    + "WHERE p.product_create_date > '2020-01-01') "
                    + "SELECT rp.id, rp.product_category_id, "
                    + "(SELECT count(*) FROM products p "
                    + "WHERE p.product_category_id = rp.product_category_id "
                    + "GROUP BY p.product_category_id) AS product_count, "
                    + "rp.product_name, rp.product_create_date, rp.sales_count "
                    + "FROM RankedProducts rp "
                    + "WHERE rnk = 1";

            try (PreparedStatement preparedStatement = connection.prepareStatement(sql);
                 ResultSet resultSet = preparedStatement.executeQuery()) {

                System.out.printf("%-4s %-19s %-14s %-13s %-19s %-12s%n",
                        "Id", "ProductCategoryId", "ProductCount", "ProductName", "ProductCreateDate", "SalesCount");
                while (resultSet.next()) {
                    int id = resultSet.getInt("id");
                    int productCategoryId = resultSet.getInt("product_category_id");
                    int productCount = resultSet.getInt("product_count");
                    String productName = resultSet.getString("product_name");
                    String productCreateDate = resultSet.getString("product_create_date");
                    int salesCount = resultSet.getInt("sales_count");

                    System.out.printf("%-4d %-19d %-14d %-13s %-19s %-12d%n",
                            id, productCategoryId, productCount, productName, productCreateDate, salesCount);
                }
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

}
