/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap;

import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import mondrian.olap.MondrianProperties;
import mondrian.olap.Util;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

class NativeSqlConfigurationDiagnosticsTest {
    @ParameterizedTest
    @ValueSource(strings = {"scalar", "enabled", "fallbackMdx", "rollupAxes", "fallbackOnMissingRowKey"})
    void malformedBooleanWarnsAtSchemaLoadWithoutLoggingItsValue(String name)
        throws Exception
    {
        try (Capture capture = new Capture()) {
            load(Map.of("nativeSql." + name, "SELECT sensitive_canary FROM private_table"));
            assertTrue(capture.messages.stream().anyMatch(s ->
                s.contains("nativeSql." + name) && s.contains("boolean")), capture.messages.toString());
            assertTrue(capture.messages.stream().noneMatch(s ->
                s.contains("sensitive_canary") || s.contains("private_table")));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "fallbackTemplate", "template.0", "template.01", "template.other",
        "template.", "template.-1", "template.+1", "template.2147483648", "enabeld"
    })
    void unknownAnnotationWarnsAtSchemaLoad(String suffix) throws Exception {
        try (Capture capture = new Capture()) {
            load(Map.of("nativeSql." + suffix, "SELECT sensitive_canary"));
            assertTrue(capture.messages.stream().anyMatch(s ->
                s.contains("nativeSql." + suffix) && s.contains("unknown")), capture.messages.toString());
            assertTrue(capture.messages.stream().noneMatch(s -> s.contains("sensitive_canary")));
        }
    }

    @Test void acceptedNamesAndCaseInsensitiveBooleansDoNotWarn() throws Exception {
        Map<String, String> annotations = new LinkedHashMap<>();
        annotations.put("nativeSql.enabled", " true ");
        annotations.put("nativeSql.template", "SELECT 1 AS val");
        annotations.put("nativeSql.template.1", "SELECT 2 AS val");
        annotations.put("nativeSql.scalar", " FALSE ");
        annotations.put("nativeSql.rollupAxes", "false");
        annotations.put("nativeSql.fallbackMdx", "True");
        annotations.put("nativeSql.fallbackOnMissingRowKey", "false");
        annotations.put("nativeSql.maxAxes", "2");
        annotations.put("nativeSql.variables", "x=1");
        annotations.put("nativeSql.relationAlias", "f");
        annotations.put("application.note", "unrelated annotation");
        try (Capture capture = new Capture()) {
            load(annotations);
            assertEquals(List.of(), capture.messages);
        }
    }

    @Test void repeatedConfigurationReadsDoNotRepeatSchemaWarnings() throws Exception {
        try (Capture capture = new Capture()) {
            load(Map.of("nativeSql.fallbackTemplate", "SELECT 1 AS val"));
            assertEquals(1, capture.messages.stream().filter(s ->
                s.contains("nativeSql.fallbackTemplate") && s.contains("unknown")).count(),
                capture.messages.toString());
        }
    }

    @Test void obsoleteExpanderKeyWarnsWithoutEnablingTheFeature() {
        String obsolete = "mondrian.expander.ExpandNonNative";
        MondrianProperties properties = MondrianProperties.instance();
        String previousSystem = System.getProperty(obsolete);
        String previousProperty = properties.getProperty(obsolete);
        boolean previousExpand = properties.ExpandNonNative.get();
        try (Capture capture = new Capture()) {
            properties.ExpandNonNative.set(false);
            System.setProperty(obsolete, "true");
            properties.populate();
            assertFalse(properties.ExpandNonNative.get());
            assertTrue(capture.messages.stream().anyMatch(s ->
                s.contains(obsolete) && s.contains("mondrian.native.ExpandNonNative")),
                capture.messages.toString());
        } finally {
            if (previousSystem == null) System.clearProperty(obsolete);
            else System.setProperty(obsolete, previousSystem);
            if (previousProperty == null) properties.remove(obsolete);
            else properties.setProperty(obsolete, previousProperty);
            properties.ExpandNonNative.set(previousExpand);
        }
    }

    private static void load(Map<String, String> annotations) throws Exception {
        String jdbc = "jdbc:h2:mem:native_config_" + UUID.randomUUID().toString().replace("-", "")
            + ";DATABASE_TO_UPPER=false";
        try (java.sql.Connection db = DriverManager.getConnection(jdbc, "sa", "");
             java.sql.Statement statement = db.createStatement())
        {
            statement.execute("CREATE TABLE fact (qty INT)");
            StringBuilder xml = new StringBuilder();
            annotations.forEach((name, value) -> xml.append("<Annotation name=\"")
                .append(name).append("\"><![CDATA[").append(value).append("]]></Annotation>"));
            Util.PropertyList props = Util.parseConnectString("Provider=mondrian;JdbcPassword=;");
            props.put("JdbcUser", "sa");
            props.put("JdbcDrivers", "org.h2.Driver");
            props.put("Jdbc", jdbc);
            props.put("CatalogContent", """
                <Schema name="NativeDiagnostics"><Cube name="Sales"><Table name="fact"/>
                  <Measure name="Quantity" column="qty" aggregator="sum"/>
                  <CalculatedMember name="Configured" dimension="Measures">
                    <Annotations>%s</Annotations><Formula>1</Formula>
                  </CalculatedMember>
                </Cube></Schema>
                """.formatted(xml));
            mondrian.olap.Connection connection = mondrian.olap.DriverManager.getConnection(props, null);
            try {
                var query = connection.parseQuery("SELECT {[Measures].[Configured]} ON COLUMNS FROM [Sales]");
                for (var member : query.getMeasuresMembers()) {
                    if (member instanceof RolapMember rolap) {
                        for (int i = 0; i < 3; i++) NativeSqlConfig.findNativeSqlMember(rolap);
                    }
                }
            } finally {
                connection.close();
            }
        }
    }

    private static final class Capture implements AutoCloseable {
        final List<String> messages = new ArrayList<>();
        private final LoggerContext context = (LoggerContext) LogManager.getContext(false);
        private final Map<String, LoggerConfig> previous = new LinkedHashMap<>();
        private final AbstractAppender appender = new AbstractAppender(
            "nativeDiagnostics", null, null, true, Property.EMPTY_ARRAY)
        {
            @Override public void append(LogEvent event) {
                messages.add(event.getMessage().getFormattedMessage());
            }
        };
        Capture() {
            appender.start();
            for (String name : List.of(NativeSqlConfig.class.getName(), MondrianProperties.class.getName())) {
                previous.put(name, context.getConfiguration().getLoggers().get(name));
                LoggerConfig logger = new LoggerConfig(name, org.apache.logging.log4j.Level.WARN, false);
                logger.addAppender(appender, org.apache.logging.log4j.Level.WARN, null);
                context.getConfiguration().removeLogger(name);
                context.getConfiguration().addLogger(name, logger);
            }
            context.updateLoggers();
        }
        @Override public void close() {
            previous.forEach((name, logger) -> {
                context.getConfiguration().removeLogger(name);
                if (logger != null) context.getConfiguration().addLogger(name, logger);
            });
            context.updateLoggers();
            appender.stop();
        }
    }
}
