/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.xmla;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The {@code <Value>} of an MDDataSet cell is typed from the Java class of
 * {@code Cell.getValue()} alone — {@code MDDataSet.writeCell} builds its
 * {@link XmlaHandler.ValueInfo} with a {@code null} datatype hint, so the
 * runtime class decides both {@code xsi:type} and the lexical form.
 *
 * <p>These cases pin that mapping for the classes a cell can carry, which is
 * what makes {@code NativeCellValueTypeTest}'s conversions load-bearing: a
 * measure whose cells reach XMLA as {@code Double} is {@code xsd:double}
 * with Java's {@code Double.toString} spelling, and the same number as the
 * raw JDBC {@code BigDecimal} or {@code BigInteger} behind it is a different
 * XMLA type with a different spelling.
 */
public class XmlaCellValueTypeTest {

    /** A stored measure: {@code sum} over a scaled DECIMAL column. */
    @Test
    public void storedMeasureAsDoubleIsXsdDouble() {
        assertSerializesAs(
            "xsd:double", "6.801459915E7", 68014599.15d);
    }

    /**
     * The same number as the driver's own {@code BigDecimal}: a different
     * type and spelling for the very same cell. This is what the NQE
     * prefetch used to publish, so one measure came back as
     * {@code xsd:double} from the segment cache and {@code xsd:decimal}
     * from a prefetched read.
     */
    @Test
    public void theSameStoredMeasureAsBigDecimalIsXsdDecimal() {
        assertSerializesAs(
            "xsd:decimal", "68014599.15", new BigDecimal("68014599.15"));
    }

    /**
     * A distinct-count measure. The segment path stores it as a double
     * (its ClickHouse {@code UInt64} column maps to
     * {@code SqlStatement.Type.OBJECT}, and the segment loader coerces a
     * numeric OBJECT to double), so it is an {@code xsd:double} whose
     * trailing {@code .0} is normalized away.
     */
    @Test
    public void distinctCountMeasureAsDoubleIsXsdDouble() {
        assertSerializesAs("xsd:double", "20", 20d);
    }

    /** The same count as the driver's {@code BigInteger}: {@code xsd:int}. */
    @Test
    public void theSameDistinctCountAsBigIntegerIsXsdInt() {
        assertSerializesAs("xsd:int", "20", BigInteger.valueOf(20));
    }

    /**
     * A native-SQL template measure. Its {@code val} column is read with
     * {@code getDouble} on both the rollup and non-rollup readers, so it is
     * an {@code xsd:double} either way.
     */
    @Test
    public void nativeTemplateMeasureIsXsdDouble() {
        assertSerializesAs(
            "xsd:double", "94464.72104166668", 94464.72104166668d);
    }

    /**
     * A calculated measure over stored ones. An alias formula hands back
     * the stored cell's own object, and an arithmetic formula produces a
     * {@code double}; both are {@code xsd:double} once the stored value is
     * a {@code Double}.
     */
    @Test
    public void calcMeasureOverStoredValuesIsXsdDouble() {
        final double stored = 68014599.15d;
        // alias: [Measures].[Sales with VAT] = [Measures].[Sales]
        assertSerializesAs("xsd:double", "6.801459915E7", stored);
        // arithmetic: sales / (customers * months)
        assertSerializesAs(
            "xsd:double", "94464.72104166668", stored / (20 * 36));
    }

    /**
     * Applies the same two steps {@code MDDataSet.writeCell} applies: a
     * hint-free {@link XmlaHandler.ValueInfo}, then
     * {@link XmlaUtil#normalizeNumericString} for a decimal value.
     */
    private void assertSerializesAs(
        String expectedType, String expectedText, Object value)
    {
        final XmlaHandler.ValueInfo info =
            new XmlaHandler.ValueInfo(null, value);
        final String text = info.isDecimal
            ? XmlaUtil.normalizeNumericString(info.value.toString())
            : info.value.toString();
        assertEquals(expectedType, info.valueType, "xsi:type");
        assertEquals(expectedText, text, "serialized value");
    }
}
