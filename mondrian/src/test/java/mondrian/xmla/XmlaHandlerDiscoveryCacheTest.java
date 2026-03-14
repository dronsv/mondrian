/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.xmla;

import junit.framework.TestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class XmlaHandlerDiscoveryCacheTest extends TestCase {

    public void testBuildDiscoveryCacheKeyIgnoresMapOrderAndGroupOrder() {
        final XmlaRequest left = discoverRequest(
            "MDSCHEMA_CUBES",
            "analyst",
            mapOf(
                "Catalog", "Sales",
                "LocaleIdentifier", "1033"),
            restrictionMap(
                "CUBE_NAME", Arrays.asList("Sales Cube"),
                "CATALOG_NAME", Arrays.asList("Sales")),
            new String[] {"group-b", "group-a"});
        final XmlaRequest right = discoverRequest(
            "MDSCHEMA_CUBES",
            "analyst",
            mapOf(
                "LocaleIdentifier", "1033",
                "Catalog", "Sales"),
            restrictionMap(
                "CATALOG_NAME", Arrays.asList("Sales"),
                "CUBE_NAME", Arrays.asList("Sales Cube")),
            new String[] {"group-a", "group-b"});

        assertEquals(
            XmlaHandler.buildDiscoveryCacheKey(left),
            XmlaHandler.buildDiscoveryCacheKey(right));
    }

    public void testBuildDiscoveryCacheKeyIgnoresContentAndFormatProperties() {
        final XmlaRequest left = discoverRequest(
            "MDSCHEMA_DIMENSIONS",
            "analyst",
            mapOf(
                "Catalog", "Sales",
                "Content", "Data",
                "Format", "Tabular"),
            Collections.<String, Object>emptyMap(),
            new String[0]);
        final XmlaRequest right = discoverRequest(
            "MDSCHEMA_DIMENSIONS",
            "analyst",
            mapOf(
                "Catalog", "Sales",
                "Content", "SchemaData",
                "Format", "Tabular"),
            Collections.<String, Object>emptyMap(),
            new String[0]);

        assertEquals(
            XmlaHandler.buildDiscoveryCacheKey(left),
            XmlaHandler.buildDiscoveryCacheKey(right));
    }

    public void testBuildDiscoveryCacheKeyChangesWithRole() {
        final XmlaRequest left =
            discoverRequest("MDSCHEMA_LEVELS", "analyst", Collections.<String, String>emptyMap(),
                Collections.<String, Object>emptyMap(), new String[0]);
        when(left.getRoleName()).thenReturn("role-a");
        final XmlaRequest right =
            discoverRequest("MDSCHEMA_LEVELS", "analyst", Collections.<String, String>emptyMap(),
                Collections.<String, Object>emptyMap(), new String[0]);
        when(right.getRoleName()).thenReturn("role-b");

        assertFalse(
            XmlaHandler.buildDiscoveryCacheKey(left)
                .equals(XmlaHandler.buildDiscoveryCacheKey(right)));
    }

    public void testDiscoveryRowsetCacheHonorsTtlAndLru() {
        final XmlaHandler.DiscoveryRowsetCache cache =
            new XmlaHandler.DiscoveryRowsetCache();
        cache.put("a", Collections.singletonList(new Rowset.Row()), 1_000L, 1, 10_000, 10);
        cache.put("b", Collections.singletonList(new Rowset.Row()), 1_001L, 1, 10_000, 10);

        assertNull(cache.tryAcquire("a", 1_001L, 1, 10_000));
        assertNotNull(cache.tryAcquire("b", 1_001L, 1, 10_000));
        assertNull(cache.tryAcquire("b", 12_500L, 1, 10_000));
    }

    public void testIsDiscoveryCacheableMatchesMetadataAllowList() {
        assertTrue(XmlaHandler.isDiscoveryCacheable(RowsetDefinition.MDSCHEMA_CUBES));
        assertTrue(XmlaHandler.isDiscoveryCacheable(RowsetDefinition.MDSCHEMA_MEMBERS));
        assertFalse(XmlaHandler.isDiscoveryCacheable(RowsetDefinition.DISCOVER_XML_METADATA));
        assertFalse(XmlaHandler.isDiscoveryCacheable(RowsetDefinition.DISCOVER_CSDL_METADATA));
    }

    private XmlaRequest discoverRequest(
        String requestType,
        String authenticatedUser,
        Map<String, String> properties,
        Map<String, Object> restrictions,
        String[] groups ) {
        final XmlaRequest request = mock(XmlaRequest.class);
        when(request.getMethod()).thenReturn(org.olap4j.metadata.XmlaConstants.Method.DISCOVER);
        when(request.getRequestType()).thenReturn(requestType);
        when(request.getRoleName()).thenReturn("analyst-role");
        when(request.getUsername()).thenReturn("excel-user");
        when(request.getAuthenticatedUser()).thenReturn(authenticatedUser);
        when(request.getAuthenticatedUserGroups()).thenReturn(groups);
        when(request.getProperties()).thenReturn(properties);
        when(request.getRestrictions()).thenReturn(restrictions);
        return request;
    }

    private Map<String, String> mapOf(String... pairs) {
        final Map<String, String> map = new HashMap<String, String>();
        for (int i = 0; i < pairs.length; i += 2) {
            map.put(pairs[i], pairs[i + 1]);
        }
        return map;
    }

    private Map<String, Object> restrictionMap(Object... pairs) {
        final Map<String, Object> map = new HashMap<String, Object>();
        for (int i = 0; i < pairs.length; i += 2) {
            map.put((String) pairs[i], pairs[i + 1]);
        }
        return map;
    }
}
