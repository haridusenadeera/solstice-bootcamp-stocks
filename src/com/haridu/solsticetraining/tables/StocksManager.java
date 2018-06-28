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

        String sqlHighestPrice = "SELECT MAX(price) FROM stocks WHERE date = ? AND symbol = ?";
        ResultSet resultHighestPrice = null;

        String sqlLowestPrice = "SELECT MIN(price) FROM stocks WHERE date = ? AND symbol = ?";
        ResultSet resultLowestPrice = null;


        String sqlTotalVolume = "SELECT SUM(volume) FROM stocks WHERE date = ? AND symbol = ?";
        ResultSet resultTotalVolume = null;

        // Object representing summarized data
        StockSummary summary = new StockSummary();
        summary.setSymbol(symbol);
        summary.setDate(date);

        try (
                Connection conn = DBUtil.getConnection(DBType.MYSQL);

                // statement for each sql query
                PreparedStatement stmtHighestPrice = conn.prepareStatement(sqlHighestPrice);
                PreparedStatement stmtLowestPrice = conn.prepareStatement(sqlLowestPrice);
                PreparedStatement stmtTotalVolume = conn.prepareStatement(sqlTotalVolume);
        ){
            stmtHighestPrice.setDate(1, date);
            stmtHighestPrice.setString(2, symbol);
            resultHighestPrice = stmtHighestPrice.executeQuery();

            stmtLowestPrice.setDate(1, date);
            stmtLowestPrice.setString(2, symbol);
            resultLowestPrice = stmtLowestPrice.executeQuery();

            stmtTotalVolume.setDate(1, date);
            stmtTotalVolume.setString(2, symbol);
            resultTotalVolume = stmtTotalVolume.executeQuery();

            if (resultHighestPrice.next()) {

                summary.setHighestPrice(resultHighestPrice.getDouble(1));
            } else {
                System.out.println("Error in retrieving highest price");
            }

            if (resultLowestPrice.next()) {

                summary.setLowestPrice(resultLowestPrice.getDouble(1));
            } else {
                System.out.println("Error in retrieving lowest price");
            }

            if (resultTotalVolume.next()) {

                summary.setTotalVolume(resultTotalVolume.getInt(1));
            } else {
                System.out.println("Error in retrieving total volume");
            }

            return summary;

        } catch (SQLException e) {
            System.err.println(e);
            return null;
        } finally {
            if (resultHighestPrice != null){
                resultHighestPrice.close();
            }
            if (resultLowestPrice != null){
                resultLowestPrice.close();
            }
            if (resultTotalVolume != null){
                resultTotalVolume.close();
            }
        }
    }
}
