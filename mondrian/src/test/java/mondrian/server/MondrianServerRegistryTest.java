package mondrian.server;

import junit.framework.TestCase;

public class MondrianServerRegistryTest extends TestCase {

    public void testStandaloneRegistryDoesNotRequireModulesPath() {
        final String previousModulesPath = MondrianServerImpl.modulesPath;
        try {
            MondrianServerImpl.modulesPath = null;
            MondrianServerRegistry registry = new MondrianServerRegistry();
            assertNotNull(registry);
            assertNotNull(registry.serverForId(null));
        } finally {
            MondrianServerImpl.modulesPath = previousModulesPath;
        }
    }
}
