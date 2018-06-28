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

public class Main {

    public static void main(String[] args) throws Exception {

        InputStream source = Main.class.getResourceAsStream("week1-stocks.json");
        addToDatabase(source);

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
