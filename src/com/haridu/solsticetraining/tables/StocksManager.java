package com.haridu.solsticetraining.tables;

import java.sql.*;

import com.haridu.solsticetraining.DBType;
import com.haridu.solsticetraining.DBUtil;
import com.haridu.solsticetraining.beans.Stock;
import com.haridu.solsticetraining.beans.StockSummary;


public class StocksManager {

    /**
     *
     * @param stock
     * @return
     * @throws Exception
     */
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

    /**
     *
     * @param symbol
     * @param date
     * @return
     * @throws SQLException
     */
    public static StockSummary getSummary(String symbol, Date date) throws SQLException {

        String sql = "SELECT MAX(price), MIN(price), SUM(volume) FROM stocks WHERE date = ? AND symbol = ?";
        ResultSet rs = null;

        // Object representing summarized data
        StockSummary summary = new StockSummary();
        summary.setSymbol(symbol);
        summary.setDate(date);

        try (
                Connection conn = DBUtil.getConnection(DBType.MYSQL);
                PreparedStatement stmt = conn.prepareStatement(sql);
        ){
            stmt.setDate(1, date);
            stmt.setString(2, symbol);
            rs = stmt.executeQuery();

            if (rs.next()) {

                summary.setHighestPrice(rs.getDouble(1));
                summary.setLowestPrice(rs.getDouble(2));
                summary.setTotalVolume(rs.getInt(3));
            } else {
                System.out.println("Error in retrieving query results");
            }

            return summary;

        } catch (SQLException e) {
            System.err.println(e);
            return null;
        } finally {
            if (rs != null){
                rs.close();
            }
        }
    }
}
