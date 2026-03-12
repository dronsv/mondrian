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

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DistinctCountMergeSupportTest extends TestCase {
    private static final String PROP_FUNCTION =
        DistinctCountMergeSupport.PROP_DISTINCT_MERGE_FUNCTION;
    private static final String PROP_MODE =
        DistinctCountMergeSupport.PROP_DISTINCT_MERGE_MODE;

    public void testAutoModeEnabledWhenDialectSupports() {
        final MondrianProperties properties = MondrianProperties.instance();
        final String prevFunction = properties.getProperty(PROP_FUNCTION);
        final String prevMode = properties.getProperty(PROP_MODE);
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
        }
    }

    public void testAutoModeDisabledWhenDialectDoesNotSupport() {
        final MondrianProperties properties = MondrianProperties.instance();
        final String prevFunction = properties.getProperty(PROP_FUNCTION);
        final String prevMode = properties.getProperty(PROP_MODE);
        properties.setProperty(PROP_FUNCTION, "uniqCombinedMerge");
        properties.setProperty(PROP_MODE, "auto");
        try {
            final Dialect dialect = dialect(false);
            assertFalse(DistinctCountMergeSupport.isEnabledForDialect(dialect));
            assertNull(DistinctCountMergeSupport.getMergeFunctionForDialect(dialect));
        } finally {
            restoreProperty(properties, PROP_FUNCTION, prevFunction);
            restoreProperty(properties, PROP_MODE, prevMode);
        }
    }

    public void testOnModeForcesEnableWithoutDialectSupport() {
        final MondrianProperties properties = MondrianProperties.instance();
        final String prevFunction = properties.getProperty(PROP_FUNCTION);
        final String prevMode = properties.getProperty(PROP_MODE);
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
        }
    }

    public void testOffModeDisablesEvenWhenDialectSupports() {
        final MondrianProperties properties = MondrianProperties.instance();
        final String prevFunction = properties.getProperty(PROP_FUNCTION);
        final String prevMode = properties.getProperty(PROP_MODE);
        properties.setProperty(PROP_FUNCTION, "uniqCombinedMerge");
        properties.setProperty(PROP_MODE, "off");
        try {
            final Dialect dialect = dialect(true);
            assertFalse(DistinctCountMergeSupport.isEnabledForDialect(dialect));
            assertNull(DistinctCountMergeSupport.getMergeFunctionForDialect(dialect));
        } finally {
            restoreProperty(properties, PROP_FUNCTION, prevFunction);
            restoreProperty(properties, PROP_MODE, prevMode);
        }
    }

    public void testInvalidModeFallsBackToAuto() {
        final MondrianProperties properties = MondrianProperties.instance();
        final String prevFunction = properties.getProperty(PROP_FUNCTION);
        final String prevMode = properties.getProperty(PROP_MODE);
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
        }
    }

    public void testMissingFunctionDisablesEveryMode() {
        final MondrianProperties properties = MondrianProperties.instance();
        final String prevFunction = properties.getProperty(PROP_FUNCTION);
        final String prevMode = properties.getProperty(PROP_MODE);
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
        }
    }

    private Dialect dialect(boolean supportsMergeFunction) {
        final Dialect dialect = mock(Dialect.class);
        when(dialect.supportsDistinctCountMergeFunction(anyString()))
            .thenReturn(supportsMergeFunction);
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

