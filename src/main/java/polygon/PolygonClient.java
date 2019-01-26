package polygon;

import common.OHLCV;
import com.google.gson.Gson;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import polygon.models.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class PolygonClient {
    static String baseUrl = "https://api.polygon.io/v1";
    static String apiKey = "INSERT_KEY_HERE";
    static Gson gson = new Gson(); // Thread safe
    final static Logger logger = LogManager.getLogger(PolygonClient.class);

    private static String doRequest(String uri) {
        StringBuffer content;
        try {
            for (int i = 0; i < 3; i++) {
                URL url = new URL(uri);
                HttpURLConnection con = (HttpURLConnection) url.openConnection();
                con.setRequestMethod("GET");
                con.setDoOutput(true);

                BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
                String inputLine;
                content = new StringBuffer();
                while ((inputLine = in.readLine()) != null) {
                    content.append(inputLine);
                }
                in.close();
                con.disconnect();

                int code = con.getResponseCode();
                if (code == 200)
                    return content.toString();
            }

        } catch (MalformedURLException e) {
            logger.error(e);
        } catch (ProtocolException e) {
            logger.error(e);
        } catch (IOException e) {
            logger.error(e);
        }

        logger.error("Retries exceeded for request {}", uri);
        return null;
    }

    public static List<Trade> getTradesForSymbol(String day, String symbol) {
        List<Trade> trades = new ArrayList<>();

        long offset = 0;
        while(true) {
            String url = String.format("%s/historic/trades/%s/%s?apiKey=%s&limit=10000&offset=%d",
                    baseUrl, symbol, day, apiKey, offset);
            // logger.debug(url);
            String content = doRequest(url);
            TradeResponse r = gson.fromJson(content, TradeResponse.class);
            if (r.ticks == null) // Last page
                return trades;
            trades.addAll(r.ticks);
            offset = r.ticks.get(r.ticks.size() - 1).t;
        }
    }

    public static List<OHLCV> getHistAggForSymbol(String day, String symbol, String resolution) {
        int limit = 50000;
        if (resolution.compareTo("day") == 0)
            limit = 2;
        String url = String.format("%s/historic/agg/%s/%s?apiKey=%s&limit=%d&from=%s",
                baseUrl, resolution, symbol, apiKey, limit, day);
//        logger.debug(url);
        String content = doRequest(url);
        AggResponse r = gson.fromJson(content, AggResponse.class);
        return r.ticks;
    }

    public static List<String> getSymbols() {
        List<String> symbols = new ArrayList<>();

        int perPage = 10000;
        for (int page = 1;; page++) {
            String url = String.format("%s/meta/symbols?apiKey=%s&sort=symbol&perpage=%d&page=%d",
                    baseUrl, apiKey, perPage, page);
            String content = doRequest(url);
            SymbolResponse r = gson.fromJson(content, SymbolResponse.class);
            for (Symbol s : r.symbols) {
                symbols.add(s.symbol);
            }

            if (r.symbols.size() < perPage) // Last page
                return symbols;
        }
    }
}
