/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/

package mondrian.olap;

import mondrian.olap.fun.BuiltinFunTable;
import mondrian.parser.JavaccParserValidatorImpl;
import org.junit.jupiter.api.Test;

import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression tests for StrToMember escaping inside archaic formula strings:
 * WITH MEMBER ... AS 'StrToMember("...")...'
 */
public class StrToMemberEscapingParserTest {
    private static final BuiltinFunTable FUN_TABLE = BuiltinFunTable.instance();

    private static final String ESCAPED_QUOTES_MDX =
        "WITH MEMBER [Продукт].[XL_PT0] AS "
            + "'strtomember(\"[Продукт].[Объединенные кондитеры]\").UniqueName'\n"
            + "MEMBER [Продукт].[XL_PT1] AS "
            + "'strtomember(\"[Продукт].[ООО \"\"Мон''дэлис Русь\"\"]\").UniqueName'\n"
            + "MEMBER [Продукт].[XL_PT2] AS "
            + "'strtomember(\"[Продукт].[ООО Марс]\").UniqueName'\n"
            + "SELECT {[Продукт].[XL_PT0],[Продукт].[XL_PT1],[Продукт].[XL_PT2]} ON 0\n"
            + "FROM [Кондитерка] CELL PROPERTIES VALUE";

    @Test
    public void testJavaccParserParsesEscapedApostropheInArchaicFormula() throws Exception {
        CaptureFactory factory = new CaptureFactory();
        QueryPart queryPart = new JavaccParserValidatorImpl(factory).parseInternal(
            null,
            ESCAPED_QUOTES_MDX,
            false,
            FUN_TABLE,
            false);

        assertNull(queryPart);
        assertParsedAsExpression(factory);
    }

    private static void assertParsedAsExpression(CaptureFactory factory) {
        assertNotNull(factory.formulas, "Expected formulas captured from WITH MEMBER");
        assertTrue(factory.formulas.length >= 2, "Expected at least 2 formulas");

        final Exp escapedMemberExpression = factory.formulas[1].getExpression();
        assertNotNull(escapedMemberExpression, "Escaped StrToMember formula must have expression");
        assertFalse(
            escapedMemberExpression instanceof Literal,
            "Escaped formula must be parsed as MDX expression, not as string literal");

        final String unparsed = Util.unparse(escapedMemberExpression);
        assertTrue(
            unparsed.toLowerCase(Locale.ROOT).contains("strtomember"),
            "Expected StrToMember call in parsed expression, got: " + unparsed);
        assertTrue(
            unparsed.contains("Мон'дэлис"),
            "Expected escaped apostrophe content in parsed expression, got: " + unparsed);
    }

    private static class CaptureFactory extends DefaultQueryPartFactory {
        private Formula[] formulas;

        @Override
        public Query makeQuery(
            mondrian.server.Statement statement,
            Formula[] formulae,
            QueryAxis[] axes,
            Subcube subcube,
            Exp slicer,
            QueryPart[] cellProps,
            boolean strictValidation) {
            this.formulas = formulae;
            return null;
        }
    }
}
