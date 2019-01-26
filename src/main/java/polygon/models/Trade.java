package polygon.models;

public class Trade {
    public long t; // Time
    public double p; // Price
    public int s; // Size
    public String e; // Exchange
    public int c1; // Conditions
    public int c2;
    public int c3;
    public int c4;

    public boolean hasFlag(int flag) {
        if (c1 == flag || c2 == flag || c3 == flag || c4 == flag)
            return true;

        return false;
    }

    public boolean isExcluded() {
        return hasFlag(37);
    }

    public String toString() {
        return String.format("%d - %d shares @ %.3f", t, s, p);
    }
}
