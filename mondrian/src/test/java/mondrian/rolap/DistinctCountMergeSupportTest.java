/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap;

import junit.framework.TestCase;
import mondrian.olap.MondrianProperties;
import mondrian.spi.Dialect;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DistinctCountMergeSupportTest extends TestCase {
    private static final String PROP_FUNCTION =
        DistinctCountMergeSupport.PROP_DISTINCT_MERGE_FUNCTION;
    private static final String PROP_MODE =
        DistinctCountMergeSupport.PROP_DISTINCT_MERGE_MODE;
    private static final String PROP_FUNCTION_MAP =
        DistinctCountMergeSupport.PROP_DISTINCT_MERGE_FUNCTION_MAP;

    public void testAutoModeEnabledWhenDialectSupports() {
        final MondrianProperties properties = MondrianProperties.instance();
        final String prevFunction = properties.getProperty(PROP_FUNCTION);
        final String prevMode = properties.getProperty(PROP_MODE);
        final String prevMap = properties.getProperty(PROP_FUNCTION_MAP);
        properties.setProperty(PROP_FUNCTION, "uniqCombinedMerge");
        properties.setProperty(PROP_MODE, "auto");
        try {
            final Dialect dialect = dialect(true);
            assertTrue(DistinctCountMergeSupport.isEnabledForDialect(dialect));
            assertEquals(
                "uniqCombinedMerge",
                DistinctCountMergeSupport.getMergeFunctionForDialect(dialect));
        } finally {
            restoreProperty(properties, PROP_FUNCTION, prevFunction);
            restoreProperty(properties, PROP_MODE, prevMode);
            restoreProperty(properties, PROP_FUNCTION_MAP, prevMap);
        }
    }

    public void testAutoModeDisabledWhenDialectDoesNotSupport() {
        final MondrianProperties properties = MondrianProperties.instance();
        final String prevFunction = properties.getProperty(PROP_FUNCTION);
        final String prevMode = properties.getProperty(PROP_MODE);
        final String prevMap = properties.getProperty(PROP_FUNCTION_MAP);
        properties.setProperty(PROP_FUNCTION, "uniqCombinedMerge");
        properties.setProperty(PROP_MODE, "auto");
        try {
            final Dialect dialect = dialect(false);
            assertFalse(DistinctCountMergeSupport.isEnabledForDialect(dialect));
            assertNull(DistinctCountMergeSupport.getMergeFunctionForDialect(dialect));
        } finally {
            restoreProperty(properties, PROP_FUNCTION, prevFunction);
            restoreProperty(properties, PROP_MODE, prevMode);
            restoreProperty(properties, PROP_FUNCTION_MAP, prevMap);
        }
    }

    public void testOnModeForcesEnableWithoutDialectSupport() {
        final MondrianProperties properties = MondrianProperties.instance();
        final String prevFunction = properties.getProperty(PROP_FUNCTION);
        final String prevMode = properties.getProperty(PROP_MODE);
        final String prevMap = properties.getProperty(PROP_FUNCTION_MAP);
        properties.setProperty(PROP_FUNCTION, "uniqCombinedMerge");
        properties.setProperty(PROP_MODE, "on");
        try {
            final Dialect dialect = dialect(false);
            assertTrue(DistinctCountMergeSupport.isEnabledForDialect(dialect));
            assertEquals(
                "uniqCombinedMerge",
                DistinctCountMergeSupport.getMergeFunctionForDialect(dialect));
        } finally {
            restoreProperty(properties, PROP_FUNCTION, prevFunction);
            restoreProperty(properties, PROP_MODE, prevMode);
            restoreProperty(properties, PROP_FUNCTION_MAP, prevMap);
        }
    }

    public void testOffModeDisablesEvenWhenDialectSupports() {
        final MondrianProperties properties = MondrianProperties.instance();
        final String prevFunction = properties.getProperty(PROP_FUNCTION);
        final String prevMode = properties.getProperty(PROP_MODE);
        final String prevMap = properties.getProperty(PROP_FUNCTION_MAP);
        properties.setProperty(PROP_FUNCTION, "uniqCombinedMerge");
        properties.setProperty(PROP_MODE, "off");
        try {
            final Dialect dialect = dialect(true);
            assertFalse(DistinctCountMergeSupport.isEnabledForDialect(dialect));
            assertNull(DistinctCountMergeSupport.getMergeFunctionForDialect(dialect));
        } finally {
            restoreProperty(properties, PROP_FUNCTION, prevFunction);
            restoreProperty(properties, PROP_MODE, prevMode);
            restoreProperty(properties, PROP_FUNCTION_MAP, prevMap);
        }
    }

    public void testInvalidModeFallsBackToAuto() {
        final MondrianProperties properties = MondrianProperties.instance();
        final String prevFunction = properties.getProperty(PROP_FUNCTION);
        final String prevMode = properties.getProperty(PROP_MODE);
        final String prevMap = properties.getProperty(PROP_FUNCTION_MAP);
        properties.setProperty(PROP_FUNCTION, "uniqCombinedMerge");
        properties.setProperty(PROP_MODE, "not-a-mode");
        try {
            final Dialect dialect = dialect(false);
            assertEquals(
                DistinctCountMergeSupport.Mode.AUTO,
                DistinctCountMergeSupport.getConfiguredMode());
            assertFalse(DistinctCountMergeSupport.isEnabledForDialect(dialect));
        } finally {
            restoreProperty(properties, PROP_FUNCTION, prevFunction);
            restoreProperty(properties, PROP_MODE, prevMode);
            restoreProperty(properties, PROP_FUNCTION_MAP, prevMap);
        }
    }

    public void testMissingFunctionDisablesEveryMode() {
        final MondrianProperties properties = MondrianProperties.instance();
        final String prevFunction = properties.getProperty(PROP_FUNCTION);
        final String prevMode = properties.getProperty(PROP_MODE);
        final String prevMap = properties.getProperty(PROP_FUNCTION_MAP);
        properties.remove(PROP_FUNCTION);
        properties.setProperty(PROP_MODE, "on");
        try {
            assertFalse(DistinctCountMergeSupport.isMergeFunctionConfigured());
            assertNull(
                DistinctCountMergeSupport.getMergeFunctionForDialect(
                    dialect(true)));
        } finally {
            restoreProperty(properties, PROP_FUNCTION, prevFunction);
            restoreProperty(properties, PROP_MODE, prevMode);
            restoreProperty(properties, PROP_FUNCTION_MAP, prevMap);
        }
    }

    public void testMeasureOverrideWinsOverGlobalFunctionInAutoMode() {
        final MondrianProperties properties = MondrianProperties.instance();
        final String prevFunction = properties.getProperty(PROP_FUNCTION);
        final String prevMode = properties.getProperty(PROP_MODE);
        final String prevMap = properties.getProperty(PROP_FUNCTION_MAP);
        properties.setProperty(PROP_FUNCTION, "uniqMerge");
        properties.setProperty(PROP_MODE, "auto");
        properties.setProperty(
            PROP_FUNCTION_MAP,
            "AKB=uniqCombinedMerge;SKU=uniqExactMerge");
        try {
            final Dialect dialect =
                dialectForFunctions("uniqCombinedMerge", "uniqExactMerge");
            assertEquals(
                "uniqCombinedMerge",
                DistinctCountMergeSupport.getMergeFunctionForDialect(
                    dialect,
                    "AKB"));
            assertEquals(
                "uniqExactMerge",
                DistinctCountMergeSupport.getMergeFunctionForDialect(
                    dialect,
                    "SKU"));
            assertNull(
                DistinctCountMergeSupport.getMergeFunctionForDialect(
                    dialect,
                    "UNMAPPED"));
        } finally {
            restoreProperty(properties, PROP_FUNCTION, prevFunction);
            restoreProperty(properties, PROP_MODE, prevMode);
            restoreProperty(properties, PROP_FUNCTION_MAP, prevMap);
        }
    }

    public void testMeasureOverrideFallsBackToGlobalWhenMissing() {
        final MondrianProperties properties = MondrianProperties.instance();
        final String prevFunction = properties.getProperty(PROP_FUNCTION);
        final String prevMode = properties.getProperty(PROP_MODE);
        final String prevMap = properties.getProperty(PROP_FUNCTION_MAP);
        properties.setProperty(PROP_FUNCTION, "uniqCombinedMerge");
        properties.setProperty(PROP_MODE, "auto");
        properties.setProperty(PROP_FUNCTION_MAP, "AKB=uniqExactMerge");
        try {
            final Dialect dialect =
                dialectForFunctions("uniqCombinedMerge", "uniqExactMerge");
            assertEquals(
                "uniqCombinedMerge",
                DistinctCountMergeSupport.getMergeFunctionForDialect(
                    dialect,
                    "UNMAPPED"));
        } finally {
            restoreProperty(properties, PROP_FUNCTION, prevFunction);
            restoreProperty(properties, PROP_MODE, prevMode);
            restoreProperty(properties, PROP_FUNCTION_MAP, prevMap);
        }
    }

    public void testInvalidMeasureMapEntryDoesNotBreakGlobalFallback() {
        final MondrianProperties properties = MondrianProperties.instance();
        final String prevFunction = properties.getProperty(PROP_FUNCTION);
        final String prevMode = properties.getProperty(PROP_MODE);
        final String prevMap = properties.getProperty(PROP_FUNCTION_MAP);
        properties.setProperty(PROP_FUNCTION, "uniqCombinedMerge");
        properties.setProperty(PROP_MODE, "auto");
        properties.setProperty(PROP_FUNCTION_MAP, "broken-entry-without-equals");
        try {
            final Dialect dialect = dialectForFunctions("uniqCombinedMerge");
            assertEquals(
                "uniqCombinedMerge",
                DistinctCountMergeSupport.getMergeFunctionForDialect(
                    dialect,
                    "AKB"));
        } finally {
            restoreProperty(properties, PROP_FUNCTION, prevFunction);
            restoreProperty(properties, PROP_MODE, prevMode);
            restoreProperty(properties, PROP_FUNCTION_MAP, prevMap);
        }
    }

    private Dialect dialect(boolean supportsMergeFunction) {
        final Dialect dialect = mock(Dialect.class);
        when(dialect.supportsDistinctCountMergeFunction(anyString()))
            .thenReturn(supportsMergeFunction);
        return dialect;
    }

    private Dialect dialectForFunctions(String... supportedFunctions) {
        final Dialect dialect = mock(Dialect.class);
        final Set<String> supported =
            new HashSet<String>(Arrays.asList(supportedFunctions));
        when(dialect.supportsDistinctCountMergeFunction(anyString()))
            .thenAnswer(invocation ->
                supported.contains(invocation.getArgument(0, String.class)));
        return dialect;
    }

    private void restoreProperty(
        MondrianProperties properties,
        String path,
        String value)
    {
        if (value == null) {
            properties.remove(path);
        } else {
            properties.setProperty(path, value);
        }
    }
}
