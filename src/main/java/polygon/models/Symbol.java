package polygon.models;

public class Symbol {
    public String symbol;
    public String name;
    public String type;
    public String updated;
    public Boolean isOTC;
    public int primaryExchange;
    public String exchSym;
    public Boolean active;
    public String url;

    public String toString() {
        return String.format("%s (%s) %s %b", symbol, exchSym, type, active);
    }
}
