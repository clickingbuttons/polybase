package polygon.models;

import com.google.gson.annotations.SerializedName;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class Trade implements Serializable {
    @SerializedName("t")
    public long timeMillis;
    @SerializedName("p")
    public double price;
    @SerializedName("s")
    public int size;
    @SerializedName("e")
    public String exchange;
    @SerializedName("c1")
    public int condition1;
    @SerializedName("c2")
    public int condition2;
    @SerializedName("c3")
    public int condition3;
    @SerializedName("c4")
    public int condition4;

    public boolean hasFlag(int flag) {
        if (condition1 == flag || condition2 == flag || condition3 == flag || condition4 == flag)
            return true;

        return false;
    }

    // https://www.ctaplan.com/publicdocs/ctaplan/notifications/trader-update/CTS_BINARY_OUTPUT_SPECIFICATION.pdf
    // PAGE 61
    public boolean isUneligibleOpen() {
        return  hasFlag(TradeCondition.AveragePrice.condition) ||
                hasFlag(TradeCondition.CashTrade.condition) ||
                hasFlag(TradeCondition.PriceVariation.condition) ||
                hasFlag(TradeCondition.OddLot.condition) ||
                hasFlag(TradeCondition.MarketCenterOfficialOpen.condition) || // Debatable
                hasFlag(TradeCondition.MarketCenterOfficialClose.condition) ||
                hasFlag(TradeCondition.NextDay.condition) ||
                hasFlag(TradeCondition.Seller.condition) ||
                hasFlag(TradeCondition.Contingent.condition) ||
                hasFlag(TradeCondition.ContingentQualified.condition) ||
                hasFlag(TradeCondition.CorrectedConsolidatedClosePrice.condition);
    }

    public boolean isUneligibleClose() {
        return  hasFlag(TradeCondition.AveragePrice.condition) ||
                hasFlag(TradeCondition.CashTrade.condition) ||
                hasFlag(TradeCondition.PriceVariation.condition) ||
                hasFlag(TradeCondition.OddLot.condition) ||
                hasFlag(TradeCondition.NextDay.condition) ||
                hasFlag(TradeCondition.MarketCenterOfficialOpen.condition) ||
                hasFlag(TradeCondition.MarketCenterOfficialClose.condition) || // Debatable
                hasFlag(TradeCondition.Seller.condition) ||
                hasFlag(TradeCondition.Contingent.condition) ||
                hasFlag(TradeCondition.ContingentQualified.condition);
    }

    public boolean isUneligibleHighLow() {
        return  hasFlag(TradeCondition.AveragePrice.condition) ||
                hasFlag(TradeCondition.CashTrade.condition) ||
                hasFlag(TradeCondition.PriceVariation.condition) ||
                hasFlag(TradeCondition.OddLot.condition) ||
                hasFlag(TradeCondition.NextDay.condition) ||
                hasFlag(TradeCondition.MarketCenterOfficialOpen.condition) || // Debatable
                hasFlag(TradeCondition.MarketCenterOfficialClose.condition) || // Debatable
                hasFlag(TradeCondition.Seller.condition) ||
                hasFlag(TradeCondition.Contingent.condition) ||
                hasFlag(TradeCondition.ContingentQualified.condition) ||
                hasFlag(TradeCondition.CorrectedConsolidatedClosePrice.condition); // Debatable
    }

    public int encodeConditions() {
        // https://www.ctaplan.com/publicdocs/ctaplan/notifications/trader-update/CTS_BINARY_OUTPUT_SPECIFICATION.pdf
        // Page 33: Sale Condition 4 Char [ ]
        return condition1 | (condition2 << 8) | (condition3 << 16) | (condition4 << 24);
    }

    public void decodeConditions(int encoded) {
        condition1 = encoded & 0x000000FF;
        condition2 = (encoded & 0x0000FF00) >> 8;
        condition3 = (encoded & 0x00FF0000) >> 16;
        condition4 = (encoded & 0xFF000000) >> 24;
    }

    private void writeObject(ObjectOutputStream oos) throws IOException {
        oos.writeLong(timeMillis);
        oos.writeDouble(price);
        oos.writeInt(size);
        oos.writeInt(Integer.decode(exchange));
        oos.writeInt(encodeConditions());
    }

    private void readObject(ObjectInputStream ois) throws IOException {
        timeMillis = ois.readLong();
        price = ois.readDouble();
        size = ois.readInt();
        exchange = Integer.toString(ois.readInt());
        decodeConditions(ois.readInt());
    }

    public String toString() {
        return String.format("%d - %d @ %.3f", timeMillis, size, price);
    }
}
