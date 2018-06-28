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

    public static double highestPrice(String symbol, Date date) throws SQLException {

        String sql = "SELECT MAX(price) FROM stocks WHERE date = ? AND symbol = ?";
        ResultSet rs = null;

        try (
                Connection conn = DBUtil.getConnection(DBType.MYSQL);
                PreparedStatement stmt = conn.prepareStatement(sql);
        ){
            stmt.setDate(1, date);
            stmt.setString(2, symbol);
            rs = stmt.executeQuery();

            if (rs.next()) {
                return rs.getDouble(1);
            } else {
                return -1;
            }

        } catch (SQLException e) {
            System.err.println(e);
            return -1;
        } finally {
            if (rs != null) {
                rs.close();
            }
        }
    }

    public static double lowestPrice(String symbol, Date date) throws SQLException {

        String sql = "SELECT MIN(price) FROM stocks WHERE date = ? AND symbol = ?";
        ResultSet rs = null;

        try (
                Connection conn = DBUtil.getConnection(DBType.MYSQL);
                PreparedStatement stmt = conn.prepareStatement(sql);
        ){
            stmt.setDate(1, date);
            stmt.setString(2, symbol);
            rs = stmt.executeQuery();

            if (rs.next()) {
                return rs.getDouble(1);
            } else {
                return -1;
            }

        } catch (SQLException e) {
            System.err.println(e);
            return -1;
        } finally {
            if (rs != null) {
                rs.close();
            }
        }
    }

    public static int totalVolume(String symbol, Date date) throws SQLException {

        String sql = "SELECT SUM(volume) FROM stocks WHERE date = ? AND symbol = ?";
        ResultSet rs = null;

        try (
                Connection conn = DBUtil.getConnection(DBType.MYSQL);
                PreparedStatement stmt = conn.prepareStatement(sql);
        ){
            stmt.setDate(1, date);
            stmt.setString(2, symbol);
            rs = stmt.executeQuery();

            if (rs.next()) {
                return rs.getInt(1);
            } else {
                return -1;
            }

        } catch (SQLException e) {
            System.err.println(e);
            return -1;
        } finally {
            if (rs != null) {
                rs.close();
            }
        }
    }
}
