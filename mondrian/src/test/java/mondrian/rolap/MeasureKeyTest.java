package mondrian.rolap;

import org.junit.jupiter.api.Test;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.*;

class MeasureKeyTest {
    @Test
    void emptyResetSignatureEqualsBareMeasureId() {
        MeasureKey a = MeasureKey.of("[Measures].[АКБ]");
        MeasureKey b = MeasureKey.of("[Measures].[АКБ]", Set.of());
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void differentResetSignaturesNotEqual() {
        MeasureKey a = MeasureKey.of("[Measures].[АКБ]", Set.of("[Продукт.Бренд]"));
        MeasureKey b = MeasureKey.of("[Measures].[АКБ]", Set.of("[Продукт.СКЮ]"));
        assertNotEquals(a, b);
    }

    @Test
    void resetSignatureIsOrderInsensitive() {
        MeasureKey a = MeasureKey.of("[M]", Set.of("[A]", "[B]"));
        MeasureKey b = MeasureKey.of("[M]", Set.of("[B]", "[A]"));
        assertEquals(a, b);
    }

    @Test
    void toStringHasMeasureIdAndResetParts() {
        MeasureKey k = MeasureKey.of("[Measures].[АКБ]", Set.of("[Продукт.Бренд]"));
        String s = k.toString();
        assertTrue(s.contains("[Measures].[АКБ]"));
        assertTrue(s.contains("[Продукт.Бренд]"));
    }
}
