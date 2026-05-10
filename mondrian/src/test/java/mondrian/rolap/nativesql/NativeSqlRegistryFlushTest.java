/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap.nativesql;

import mondrian.rolap.CacheControlImpl;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 * Regression test for the production bug where
 * {@link NativeSqlRegistry#GLOBAL_SUCCESS} and
 * {@link NativeSqlRegistry#FINGERPRINT_KIND_INDEX} grew unboundedly
 * because nothing in production ever called
 * {@code NativeSqlCalc.clearCache()}.
 *
 * <p>The fix routes schema-flush invalidation through
 * {@link CacheControlImpl#flushSchemaCache()} (and its sibling
 * {@code flushSchema(...)} overloads) via the shared
 * {@code clearResultReuseCache()} helper. This test seeds both
 * static caches with synthetic entries, invokes
 * {@code flushSchemaCache()} on a {@link CacheControlImpl} constructed
 * with a {@code null} {@code RolapConnection} (the null-safe code path),
 * and asserts both caches are emptied.
 *
 * <p>Reflection is used to (a) inject the seed entries — the maps and
 * their key type are intentionally private — and (b) read back the
 * post-flush sizes. This keeps production encapsulation intact: no
 * field, key type, or test-only setter is added to
 * {@link NativeSqlRegistry}.
 */
public class NativeSqlRegistryFlushTest {

    @BeforeEach public void setUp() {
        // Start each test with a clean global state so the seed/flush
        // assertions reflect only what this test wrote.
        NativeSqlRegistry.clearGlobalCache();
    }

    @AfterEach public void tearDown() {
        // Be a polite static-cache citizen: don't leak state to the
        // next test class.
        NativeSqlRegistry.clearGlobalCache();
    }

    @Test public void flushSchemaCacheEmptiesNativeSqlRegistryStatics()
        throws Exception
    {
        // -- arrange: seed both static caches via reflection -------------
        DataSource ds = mock(DataSource.class);
        NativeSqlFingerprint fp = NativeSqlFingerprint.of(
            "SELECT 1", Collections.emptyList(), ds, "flush-test");

        Map<Object, NativeSqlLookupResult> globalSuccess =
            globalSuccessMap();
        Map<NativeSqlFingerprint, NativeSqlWorkKind> fingerprintKindIndex =
            fingerprintKindIndexMap();

        Object cacheKey = newCacheKey(fp, NativeSqlWorkKind.SCALAR);
        globalSuccess.put(cacheKey, NativeSqlLookupResult.success("seed"));
        fingerprintKindIndex.put(fp, NativeSqlWorkKind.SCALAR);

        assertEquals(1, globalSuccess.size(),
            "precondition: GLOBAL_SUCCESS should hold the seed entry");
        assertEquals(1, fingerprintKindIndex.size(),
            "precondition: FINGERPRINT_KIND_INDEX should hold the seed entry");

        // -- act: flush via the public CacheControl API ------------------
        // null connection is safe: flushSchemaCache() and the
        // clearResultReuseCache() helper both null-check the connection
        // before dereferencing it. The path we care about
        // (NativeSqlCalc.clearCache → NativeSqlRegistry.clearGlobalCache)
        // does not depend on the connection at all.
        CacheControlImpl cacheControl = new CacheControlImpl(null);
        cacheControl.flushSchemaCache();

        // -- assert: both static caches are empty ------------------------
        assertTrue(globalSuccess.isEmpty(),
            "flushSchemaCache must clear GLOBAL_SUCCESS, but it still "
            + "holds " + globalSuccess.size() + " entries");
        assertTrue(fingerprintKindIndex.isEmpty(),
            "flushSchemaCache must clear FINGERPRINT_KIND_INDEX, but it "
            + "still holds " + fingerprintKindIndex.size() + " entries");
    }

    // ---------------------------------------------------------------------
    // reflection helpers — keep encapsulation; production fields stay
    // private. If NativeSqlRegistry is later refactored to expose a
    // package-private test seam these helpers can be deleted in one shot.
    // ---------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private static Map<Object, NativeSqlLookupResult> globalSuccessMap()
        throws Exception
    {
        Field f = NativeSqlRegistry.class.getDeclaredField("GLOBAL_SUCCESS");
        f.setAccessible(true);
        return (Map<Object, NativeSqlLookupResult>) f.get(null);
    }

    @SuppressWarnings("unchecked")
    private static Map<NativeSqlFingerprint, NativeSqlWorkKind>
        fingerprintKindIndexMap() throws Exception
    {
        Field f = NativeSqlRegistry.class
            .getDeclaredField("FINGERPRINT_KIND_INDEX");
        f.setAccessible(true);
        return (Map<NativeSqlFingerprint, NativeSqlWorkKind>) f.get(null);
    }

    /**
     * Builds an instance of the private {@code NativeSqlRegistry.CacheKey}
     * record via reflection. We use the {@code SCALAR} bucket which the
     * registry maps to its private {@code Bucket.CELL_SCALAR} discriminator
     * — but for this test the only requirement is that the key is a valid
     * {@code GLOBAL_SUCCESS} entry; flush is bucket-agnostic.
     */
    private static Object newCacheKey(
        NativeSqlFingerprint fp, NativeSqlWorkKind kind) throws Exception
    {
        Class<?> cacheKeyClass =
            Class.forName("mondrian.rolap.nativesql.NativeSqlRegistry$CacheKey");
        Class<?> bucketClass =
            Class.forName("mondrian.rolap.nativesql.NativeSqlRegistry$Bucket");
        java.lang.reflect.Method forKind =
            bucketClass.getDeclaredMethod("forKind", NativeSqlWorkKind.class);
        forKind.setAccessible(true);
        Object bucket = forKind.invoke(null, kind);
        Constructor<?> ctor =
            cacheKeyClass.getDeclaredConstructor(
                NativeSqlFingerprint.class, bucketClass);
        ctor.setAccessible(true);
        return ctor.newInstance(fp, bucket);
    }
}
