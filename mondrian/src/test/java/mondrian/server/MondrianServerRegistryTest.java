package mondrian.server;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class MondrianServerRegistryTest {

    @Test public void testStandaloneRegistryDoesNotRequireModulesPath() {
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
