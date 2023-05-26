package org.dstu.db;

import org.dstu.util.CsvReader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;

public class DbWorker {
    public static void populateFromFile(String fileName) {
        List<String[]> strings = CsvReader.readCsvFile(fileName, ";");
        Connection conn = DbConnection.getConnection();
        try {
            Statement cleaner = conn.createStatement();
            System.out.println(cleaner.executeUpdate("DELETE FROM square"));
            System.out.println(cleaner.executeUpdate("DELETE FROM rectangle"));
            PreparedStatement squareSt = conn.prepareStatement(
                    "INSERT INTO square (ugol, colorline, colorback, lengt, wight) " +
                            "VALUES (?, ?, ?, ?, ?)");
            PreparedStatement rectangleSt = conn.prepareStatement(
                    "INSERT INTO rectangle (ugol, colorline, colorback, lengt, wight) " +
                            "VALUES (?, ?, ?, ?, ?)");

            for (String[] line: strings) {
                if (Objects.equals(line[3], line[4])) {
                    squareSt.setInt(1, Integer.parseInt(line[0]));
                    squareSt.setString(2, line[1]);
                    squareSt.setString(3, line[2]);
                    squareSt.setInt(4, Integer.parseInt(line[3]));
                    squareSt.setInt(5, Integer.parseInt(line[4]));
                    squareSt.addBatch();
                } else {
                    rectangleSt.setInt(1, Integer.parseInt(line[0]));
                    rectangleSt.setString(2, line[1]);
                    rectangleSt.setString(3, line[2]);
                    rectangleSt.setInt(4, Integer.parseInt(line[3]));
                    rectangleSt.setInt(5, Integer.parseInt(line[4]));
                    rectangleSt.addBatch();
                }
            }
            int[] stRes = squareSt.executeBatch();
            int[] rectangleRes = rectangleSt.executeBatch();
            for (int num: stRes) {
                System.out.println(num);
            }

            for (int num: rectangleRes) {
                System.out.println(num);
            }
            cleaner.close();
            squareSt.close();
            rectangleSt.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public static void demoQuery() {
        Connection conn = DbConnection.getConnection();
        try {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery("SELECT * FROM square WHERE colorline = 'красный'");
            while (rs.next()) {
                System.out.print(rs.getString("colorline"));
                System.out.print(" ");
                System.out.print(rs.getString("colorback"));
                System.out.print(" ");
                System.out.print(rs.getString("lengt"));
                System.out.print(" ");
                System.out.println(rs.getString("wight"));
            }
            rs.close();
            st.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public static void dirtyReadDemo() {
        Runnable first = () -> {
            Connection conn1 = DbConnection.getNewConnection();
            if (conn1 != null) {
                try {
                    conn1.setAutoCommit(false);
                    conn1.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                    Statement upd = conn1.createStatement();
                    upd.executeUpdate("UPDATE square SET colorline='фиолетовый' WHERE lengt > 10");
                    Thread.sleep(2000);
                    conn1.rollback();
                    upd.close();
                    Statement st = conn1.createStatement();
                    System.out.println("In the first thread:");
                    ResultSet rs = st.executeQuery("SELECT * FROM square");
                    while (rs.next()) {
                        System.out.println(rs.getString("colorline"));
                    }
                    st.close();
                    rs.close();
                    conn1.close();
                } catch (SQLException | InterruptedException throwables) {
                    throwables.printStackTrace();
                }
            }
        };

        Runnable second = () -> {
            Connection conn2 = DbConnection.getNewConnection();
            if (conn2 != null) {
                try {
                    Thread.sleep(500);
                    conn2.setAutoCommit(false);
                    conn2.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                    Statement st = conn2.createStatement();
                    ResultSet rs = st.executeQuery("SELECT * FROM square");
                    while (rs.next()) {
                        System.out.println(rs.getString("colorline"));
                    }
                    rs.close();
                    st.close();
                    conn2.close();
                } catch (SQLException | InterruptedException throwables) {
                    throwables.printStackTrace();
                }
            }
        };
        Thread th1 = new Thread(first);
        Thread th2 = new Thread(second);
        th1.start();
        th2.start();
    }
}

