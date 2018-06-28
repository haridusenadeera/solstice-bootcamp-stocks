package com.haridu.solsticetraining;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.haridu.solsticetraining.beans.Stock;
import com.haridu.solsticetraining.tables.StocksManager;
import sun.java2d.pipe.SpanShapeRenderer;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.sql.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) throws Exception {

        /*
        InputStream source = Main.class.getResourceAsStream("week1-stocks.json");
        addToDatabase(source);
        */

        Scanner sc = new Scanner(System.in);

        System.out.print("Please enter the symbol of the stock (i.e MSFT): ");
        String symbol = sc.nextLine();
        System.out.print("Please enter the date you want a summary of (i.e 2018-06-22): ");
        String date = sc.nextLine();


        double highestPrice = StocksManager.highestPrice(symbol, Date.valueOf(date));
        double lowestPrice = StocksManager.lowestPrice(symbol, Date.valueOf(date));
        int totalVolume = StocksManager.totalVolume(symbol, Date.valueOf(date));


        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        System.out.println("~~~~~~~~~~SUMMARY~~~~~~~~~~~~~~~~");
        System.out.println("Highest price for the day: " + highestPrice);
        System.out.println("Lowest price for the day: " + lowestPrice);
        System.out.println("Total volume for the day: " + totalVolume);

    }

    public static void addToDatabase(InputStream source ) throws Exception {

        ObjectMapper mapper = new ObjectMapper();

        Stock[] stockData = null;

        try {
            stockData = mapper.readValue(source, Stock[].class);
            System.out.println("Reading from input stream completed");
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (Stock data:stockData){
            boolean result = StocksManager.insert(data);
            if(!result){
                System.out.println(data + " Row not added");
            }
        }
        System.out.println("Process completed!");
    }
}
