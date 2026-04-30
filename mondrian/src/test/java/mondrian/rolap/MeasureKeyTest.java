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

    @Test
    void equalsHandlesNullAndOtherType() {
        MeasureKey k = MeasureKey.of("[m]");
        assertNotEquals(null, k);
        assertNotEquals("[m]", k);
        assertEquals(k, k); // reflexive
    }

    @Test
    void ofRejectsNullMeasureId() {
        assertThrows(NullPointerException.class,
            () -> MeasureKey.of(null));
        assertThrows(NullPointerException.class,
            () -> MeasureKey.of(null, java.util.Set.of()));
    }

    @Test
    void ofRejectsNullResetSignatureElements() {
        java.util.Set<String> bad = new java.util.HashSet<>();
        bad.add("[A]");
        bad.add(null);
        assertThrows(NullPointerException.class,
            () -> MeasureKey.of("[m]", bad));
    }

    @Test
    void mutatingInputSetDoesNotAffectKey() {
        java.util.Set<String> input =
            new java.util.HashSet<>(java.util.Set.of("[A]"));
        MeasureKey k = MeasureKey.of("[m]", input);
        int hashBefore = k.hashCode();
        input.add("[B]");
        assertEquals(hashBefore, k.hashCode(),
            "hashCode must not change when input set is mutated");
        assertEquals(java.util.Set.of("[A]"), k.resetSignature());
    }

    @Test
    void preservesUniqueNameWhitespaceAndCase() {
        MeasureKey a = MeasureKey.of("[ Бренд ]", java.util.Set.of("[Code Box]"));
        assertTrue(a.measureId().contains(" "),
            "measureId whitespace must not be normalized");
        assertTrue(a.resetSignature().iterator().next().contains(" "),
            "resetSignature elements must not be normalized");
    }
}
