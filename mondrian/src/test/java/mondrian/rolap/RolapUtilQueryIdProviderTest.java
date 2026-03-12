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

import java.util.concurrent.atomic.AtomicInteger;

public class RolapUtilQueryIdProviderTest extends TestCase {

    @Override
    protected void tearDown() throws Exception {
        RolapUtil.clearThreadQueryIdProvider();
        super.tearDown();
    }

    public void testThreadLocalQueryIdProviderYieldsSequence() {
        final AtomicInteger sequence = new AtomicInteger(0);
        RolapUtil.setThreadQueryIdProvider(
            new RolapUtil.QueryIdProvider() {
                public String nextQueryId() {
                    return "q-" + sequence.incrementAndGet();
                }
            });
        assertEquals("q-1", RolapUtil.nextQueryId());
        assertEquals("q-2", RolapUtil.nextQueryId());
    }

    public void testClearThreadLocalProviderReturnsNull() {
        RolapUtil.setThreadQueryIdProvider(
            new RolapUtil.QueryIdProvider() {
                public String nextQueryId() {
                    return "q-1";
                }
            });
        assertEquals("q-1", RolapUtil.nextQueryId());
        RolapUtil.clearThreadQueryIdProvider();
        assertNull(RolapUtil.nextQueryId());
    }
}
