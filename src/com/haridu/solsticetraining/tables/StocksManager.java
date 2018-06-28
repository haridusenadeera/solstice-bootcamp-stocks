package com.haridu.solsticetraining.tables;

import java.sql.*;

import com.haridu.solsticetraining.DBType;
import com.haridu.solsticetraining.DBUtil;
import com.haridu.solsticetraining.beans.Stock;


public class StocksManager {

    public static boolean insert(Stock stock) throws Exception {

        String sql = "INSERT into stocks (symbol, price, volume, date) " +
                "VALUES (?, ?, ?, ?)";

        ResultSet keys = null;

        try (
                Connection conn = DBUtil.getConnection(DBType.MYSQL);
                PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
        ) {

            stmt.setString(1, stock.getSymbol());
            stmt.setDouble(2, stock.getPrice());
            stmt.setInt(3, stock.getVolume());
            stmt.setDate(4, stock.getDate());

            stmt.executeUpdate();

        } catch (SQLException e) {
            System.err.println(e);
            return false;
        }
        return true;
    }

    public void insertJSONDate() {

    }
}
