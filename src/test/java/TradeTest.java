import polygon.models.Trade;
import polygon.models.TradeCondition;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class TradeTest {
    @Test
    public void testEncoding() {
        Trade t1 = new Trade();
        t1.condition1 = TradeCondition.Errored.condition;
        t1.condition2 = TradeCondition.Summary.condition;
        t1.condition3 = TradeCondition.OOB.condition;
        t1.condition4 = TradeCondition.AsOfCancel.condition;

        int encoded = t1.encodeConditions();

        Trade t2 = new Trade();
        t2.decodeConditions(encoded);

        assertEquals(t1.condition1, t2.condition1);
        assertEquals(t1.condition2, t2.condition2);
        assertEquals(t1.condition3, t2.condition3);
        assertEquals(t1.condition4, t2.condition4);
    }
}
