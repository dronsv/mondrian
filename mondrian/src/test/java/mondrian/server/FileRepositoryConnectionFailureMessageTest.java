/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026
// All Rights Reserved.
*/
package mondrian.server;

import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLHandshakeException;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression for dronsv/mondrian#16 — XMLA discovery's "No suitable
 * connection found" fault must preserve datasource / catalog / last
 * failure context so deployment-time connection issues are
 * distinguishable from registration issues.
 */
public class FileRepositoryConnectionFailureMessageTest {

    @Test public void
    testFormatNoConnectionMessage_includesDatasourceAndCatalog() {
        String msg = FileRepository.formatNoConnectionMessage(
            "FoodMartDS",
            "FoodMartCatalog",
            new SSLHandshakeException("PKIX path building failed"));
        assertTrue(msg.contains("FoodMartDS"), msg);
        assertTrue(msg.contains("FoodMartCatalog"), msg);
    }

    @Test public void
    testFormatNoConnectionMessage_includesLastFailureClassAndMessage() {
        String msg = FileRepository.formatNoConnectionMessage(
            "DS",
            "Cat",
            new SSLHandshakeException("PKIX path building failed"));
        assertTrue(msg.contains("SSLHandshakeException"), msg);
        assertTrue(msg.contains("PKIX path building failed"), msg);
    }

    @Test public void
    testFormatNoConnectionMessage_handlesNullFailure() {
        String msg = FileRepository.formatNoConnectionMessage(
            "DS", "Cat", null);
        assertTrue(msg.contains("No suitable connection found"), msg);
        assertTrue(msg.contains("DS"), msg);
        assertFalse(msg.contains("null"), msg);
    }

    @Test public void
    testFormatNoConnectionMessage_handlesNullCatalogName() {
        String msg = FileRepository.formatNoConnectionMessage(
            "DS",
            null,
            new SQLException("connection refused"));
        assertTrue(msg.contains("DS"), msg);
        assertTrue(msg.contains("SQLException"), msg);
        assertFalse(msg.contains("null"), msg);
    }

    @Test public void
    testFormatNoConnectionMessage_handlesNullDatasourceName() {
        String msg = FileRepository.formatNoConnectionMessage(
            null,
            "Cat",
            new SQLException("connection refused"));
        assertTrue(msg.contains("Cat"), msg);
        assertFalse(msg.contains("null"), msg);
    }

    @Test public void
    testFormatNoConnectionMessage_redactsJdbcPasswordInFailureMessage() {
        String msg = FileRepository.formatNoConnectionMessage(
            "DS",
            "Cat",
            new SQLException(
                "auth failed for jdbc:clickhouse://h:8443?"
                + "JdbcUser=root;JdbcPassword=hunter2;ssl=true"));
        assertFalse(msg.contains("hunter2"), msg);
        assertTrue(msg.contains("JdbcPassword=***"), msg);
    }

    @Test public void
    testFormatNoConnectionMessage_redactsJdbcUserInFailureMessage() {
        String msg = FileRepository.formatNoConnectionMessage(
            "DS",
            "Cat",
            new SQLException(
                "auth failed for JdbcUser=admin@corp;"
                + "JdbcPassword=hunter2;ssl=true"));
        assertFalse(msg.contains("admin@corp"), msg);
        assertTrue(msg.contains("JdbcUser=***"), msg);
    }

    @Test public void
    testRedactConnectionCredentials_password() {
        String src = "Jdbc=jdbc:ch://h:8443;JdbcUser=root;JdbcPassword=s3cret;ssl=true";
        String redacted = FileRepository.redactConnectionCredentials(src);
        assertFalse(redacted.contains("s3cret"), redacted);
        assertTrue(redacted.contains("JdbcPassword=***"), redacted);
        assertTrue(redacted.contains("ssl=true"), redacted);
    }

    @Test public void
    testRedactConnectionCredentials_userAndPassword() {
        String src = "JdbcUser=alice;JdbcPassword=p4ss";
        String redacted = FileRepository.redactConnectionCredentials(src);
        assertFalse(redacted.contains("alice"), redacted);
        assertFalse(redacted.contains("p4ss"), redacted);
        assertEquals("JdbcUser=***;JdbcPassword=***", redacted);
    }

    @Test public void
    testRedactConnectionCredentials_nullPassthrough() {
        assertEquals(null, FileRepository.redactConnectionCredentials(null));
    }

    @Test public void
    testRedactConnectionCredentials_noCredentialsPassThrough() {
        String src = "Jdbc=jdbc:ch://host:8443;ssl=true";
        assertEquals(src, FileRepository.redactConnectionCredentials(src));
    }
}
