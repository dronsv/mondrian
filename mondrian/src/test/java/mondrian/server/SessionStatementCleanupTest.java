package mondrian.server;

import mondrian.olap.MondrianServer;
import mondrian.olap.Util;
import mondrian.rolap.RolapConnection;
import mondrian.spi.CatalogLocator;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SessionStatementCleanupTest {

    @Test public void closeInternalRemovesOnlyStatementsForSession()
        throws Exception
    {
        final MondrianServerImpl server = new MondrianServerImpl(
            new MondrianServerRegistry(),
            mock(Repository.class),
            mock(CatalogLocator.class));
        final String sessionId = "session-cleanup-"
            + UUID.randomUUID().toString();
        final String otherSessionId = sessionId + "-other";

        try {
            Session.create(sessionId);
            Session.create(otherSessionId);

            server.addStatement(statement(101L, server, sessionId));
            server.addStatement(statement(102L, server, sessionId));
            server.addStatement(statement(201L, server, otherSessionId));

            assertEquals(2, server.getStatements(sessionId).size());
            assertEquals(1, server.getStatements(otherSessionId).size());

            Session.closeInternal(sessionId);

            assertEquals(0, server.getStatements(sessionId).size());
            assertEquals(1, server.getStatements(otherSessionId).size());
        } finally {
            Session.closeInternal(sessionId);
            Session.closeInternal(otherSessionId);
            MondrianServerImpl.getServers().remove(server);
        }
    }

    private static Statement statement(
        long id,
        MondrianServer server,
        String sessionId)
    {
        final Statement statement = mock(Statement.class);
        final RolapConnection connection = mock(RolapConnection.class);
        final Util.PropertyList connectInfo = new Util.PropertyList();
        connectInfo.put("sessionId", sessionId);

        when(connection.getConnectInfo()).thenReturn(connectInfo);
        when(connection.getServer()).thenReturn(server);
        when(connection.getId()).thenReturn((int) id);
        when(statement.getId()).thenReturn(id);
        when(statement.getMondrianConnection()).thenReturn(connection);
        return statement;
    }
}
