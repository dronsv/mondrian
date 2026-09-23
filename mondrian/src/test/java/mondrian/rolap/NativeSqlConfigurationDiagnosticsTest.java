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

    @ParameterizedTest
    @ValueSource(strings = {"missing", "blank"})
    void templateAfterGapWarnsWithoutChangingTheParsedChain(String gap)
        throws Exception
    {
        Map<String, String> annotations = new LinkedHashMap<>();
        annotations.put("nativeSql.enabled", "true");
        annotations.put("nativeSql.template", "SELECT 1 AS val");
        annotations.put("nativeSql.template.1", "SELECT 2 AS val");
        if (gap.equals("blank")) {
            annotations.put("nativeSql.template.2", " ");
        }
        annotations.put("nativeSql.template.3", "SELECT sensitive_canary AS val");
        try (Capture capture = new Capture()) {
            var definition = load(annotations);
            assertEquals(List.of("SELECT 1 AS val", "SELECT 2 AS val"),
                definition.getTemplates());
            assertEquals(1, capture.messages.stream().filter(message ->
                message.contains("nativeSql.template.3")
                    && message.contains("ignored")).count(),
                capture.messages.toString());
            assertTrue(capture.messages.stream().noneMatch(message ->
                message.contains("sensitive_canary")));
        }
    }

    @Test void numberedTemplatesWithoutPrimaryWarnThatTheyAreIgnored()
        throws Exception
    {
        try (Capture capture = new Capture()) {
            assertNull(load(Map.of(
                "nativeSql.enabled", "true",
                "nativeSql.template.1", "SELECT sensitive_canary AS val")));
            assertTrue(capture.messages.stream().anyMatch(message ->
                message.contains("nativeSql.template.1")
                    && message.contains("ignored")), capture.messages.toString());
            assertTrue(capture.messages.stream().noneMatch(message ->
                message.contains("sensitive_canary")));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"not-a-number", "1.5", "2147483648", " "})
    void malformedMaxAxesWarnsButRetainsTheRuntimeDefault(String value)
        throws Exception
    {
        try (Capture capture = new Capture()) {
            var definition = load(Map.of(
                "nativeSql.enabled", "true",
                "nativeSql.template", "SELECT 1 AS val",
                "nativeSql.maxAxes", value));
            assertEquals(10, definition.getMaxAxes());
            assertEquals(1, capture.messages.stream().filter(message ->
                message.contains("nativeSql.maxAxes")
                    && message.contains("integer")).count(),
                capture.messages.toString());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"template", "scalar"})
    void duplicateNativeAnnotationWarnsWhileTheLastStillWins(String name)
        throws Exception
    {
        boolean scalar = name.equals("scalar");
        try (Capture capture = new Capture()) {
            var definition = load(List.of(
                Map.entry("nativeSql.enabled", "true"),
                Map.entry("nativeSql." + name, "SELECT sensitive_canary AS val"),
                Map.entry(scalar ? "nativeSql.template" : "nativeSql.template.1",
                    "SELECT 2 AS val"),
                Map.entry("nativeSql." + name, scalar ? "false" : "SELECT 3 AS val")));
            assertEquals(1, capture.count("'nativeSql." + name + "'", "2 times"),
                capture.messages.toString());
            if (scalar) {
                assertFalse(definition.isScalar());
            } else {
                assertEquals(List.of("SELECT 3 AS val", "SELECT 2 AS val"),
                    definition.getTemplates());
            }
            capture.assertValueFree();
        }
    }

    @Test void duplicateAnnotationOutsideNativeSqlAlsoWarns() throws Exception {
        try (Capture capture = new Capture()) {
            load(List.of(
                Map.entry("application.note", "sensitive_canary"),
                Map.entry("application.note", "second")));
            assertEquals(1, capture.count("'application.note'", "2 times"),
                capture.messages.toString());
            capture.assertValueFree();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "NativeSql.scalar=nativeSql.scalar",
        "nativesql.template.1=nativeSql.template.1",
        "NATIVESQL.ENABLED=nativeSql.enabled",
        "nativeSql.Scalar=nativeSql.scalar"
    })
    void wrongCaseNameWarnsWithTheSupportedName(String names) throws Exception {
        String name = names.substring(0, names.indexOf('='));
        String supported = names.substring(names.indexOf('=') + 1);
        try (Capture capture = new Capture()) {
            load(Map.of(
                "nativeSql.enabled", "true",
                "nativeSql.template", "SELECT 1 AS val",
                name, "SELECT sensitive_canary"));
            assertEquals(1, capture.count("'" + name + "'", "'" + supported + "'"),
                capture.messages.toString());
            capture.assertValueFree();
        }
    }

    @Test void wrongCasePrefixWithoutASupportedNameStillWarns() throws Exception {
        try (Capture capture = new Capture()) {
            load(Map.of("NativeSQL.fallbackTemplate", "SELECT sensitive_canary"));
            assertEquals(1, capture.count("'NativeSQL.fallbackTemplate'", "unknown"),
                capture.messages.toString());
            capture.assertValueFree();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "<Cube name=\"Sales\"><Annotations>%s</Annotations><Table name=\"fact\"/>"
            + "<Measure name=\"Quantity\" column=\"qty\" aggregator=\"sum\"/></Cube>",
        "<Cube name=\"Sales\"><Table name=\"fact\"/>"
            + "<Measure name=\"Quantity\" column=\"qty\" aggregator=\"sum\">"
            + "<Annotations>%s</Annotations></Measure></Cube>",
        "<Cube name=\"Sales\"><Table name=\"fact\"/>"
            + "<Dimension name=\"Qty\"><Annotations>%s</Annotations>"
            + "<Hierarchy hasAll=\"true\"><Level name=\"Qty\" column=\"qty\"/>"
            + "</Hierarchy></Dimension>"
            + "<Measure name=\"Quantity\" column=\"qty\" aggregator=\"sum\"/></Cube>",
        "<Cube name=\"Sales\"><Table name=\"fact\"/>"
            + "<Dimension name=\"Qty\"><Hierarchy hasAll=\"true\">"
            + "<Level name=\"Qty\" column=\"qty\"><Annotations>%s</Annotations></Level>"
            + "</Hierarchy></Dimension>"
            + "<Measure name=\"Quantity\" column=\"qty\" aggregator=\"sum\"/></Cube>",
        "<Cube name=\"Sales\"><Table name=\"fact\"/>"
            + "<Measure name=\"Quantity\" column=\"qty\" aggregator=\"sum\"/>"
            + "<NamedSet name=\"All measures\"><Annotations>%s</Annotations>"
            + "<Formula>{[Measures].[Quantity]}</Formula></NamedSet></Cube>",
        "<Cube name=\"Sales\"><Table name=\"fact\"/>"
            + "<Measure name=\"Quantity\" column=\"qty\" aggregator=\"sum\"/></Cube>"
            + "<VirtualCube name=\"Everything\"><Annotations>%s</Annotations>"
            + "<VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Quantity]\"/>"
            + "</VirtualCube>"
    })
    void nativeAnnotationOnAnElementThatNeverReadsItWarns(String cubes)
        throws Exception
    {
        String annotations =
            "<Annotation name=\"nativeSql.enabled\">true</Annotation>"
            + "<Annotation name=\"NativeSql.template\"><![CDATA[SELECT sensitive_canary]]></Annotation>";
        try (Capture capture = new Capture()) {
            loadSchema(cubes.formatted(annotations));
            assertEquals(1, capture.count("'nativeSql.enabled'", "ignored"),
                capture.messages.toString());
            assertEquals(1, capture.count("'NativeSql.template'", "ignored"),
                capture.messages.toString());
            capture.assertValueFree();
        }
    }

    @Test void nativeAnnotationsWithoutEnabledWarnThatTheyAreIgnored()
        throws Exception
    {
        try (Capture capture = new Capture()) {
            assertNull(load(Map.of(
                "nativeSql.template", "SELECT sensitive_canary AS val",
                "nativeSql.scalar", "true")));
            assertEquals(1, capture.count("'nativeSql.enabled'", "missing"),
                capture.messages.toString());
            capture.assertValueFree();
        }
    }

    @Test void explicitlyDisabledNativeSqlDoesNotWarn() throws Exception {
        try (Capture capture = new Capture()) {
            assertNull(load(Map.of(
                "nativeSql.enabled", "false",
                "nativeSql.template", "SELECT 1 AS val")));
            assertEquals(List.of(), capture.messages);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing", "blank"})
    void enabledWithoutAPrimaryTemplateWarns(String primary) throws Exception {
        Map<String, String> annotations = new LinkedHashMap<>();
        annotations.put("nativeSql.enabled", "true");
        annotations.put("nativeSql.scalar", "true");
        if (primary.equals("blank")) {
            annotations.put("nativeSql.template", " ");
        }
        try (Capture capture = new Capture()) {
            assertNull(load(annotations));
            assertEquals(1, capture.count("'nativeSql.template'", "missing or blank"),
                capture.messages.toString());
        }
    }

    @Test void malformedVariablesWarnWithoutChangingTheParsedValues()
        throws Exception
    {
        try (Capture capture = new Capture()) {
            var definition = load(Map.of(
                "nativeSql.enabled", "true",
                "nativeSql.template", "SELECT 1 AS val",
                "nativeSql.variables",
                "sensitive_canary;=private_table;a=1;a=2;b=3;"));
            assertEquals(Map.of("a", "2", "b", "3"), definition.getVariables());
            assertEquals(1, capture.count("'nativeSql.variables'", "not 'name=value'"),
                capture.messages.toString());
            assertEquals(1, capture.count("'nativeSql.variables'", "repeat"),
                capture.messages.toString());
            capture.assertValueFree();
        }
    }

    @Test void missingRelationAliasWarnsOnceAtSchemaLoadWithoutItsValue()
        throws Exception
    {
        try (Capture capture = new Capture()) {
            load(Map.of(
                "nativeSql.enabled", "true",
                "nativeSql.template",
                "SELECT ${axisResultSelectList}, 1 AS val FROM private_table",
                "nativeSql.relationAlias", "sensitive_canary"));
            assertEquals(1, capture.count("'nativeSql.relationAlias'", "axis queries"),
                capture.messages.toString());
            capture.assertValueFree();
        }
    }

    @Test void virtualCubesDoNotRepeatTheBaseMembersWarnings() throws Exception {
        String member = """
            <CalculatedMember name="Configured" dimension="Measures">
              <Annotations>
                <Annotation name="nativeSql.enabled">true</Annotation>
                <Annotation name="nativeSql.template">SELECT 1 AS val</Annotation>
                <Annotation name="nativeSql.scalar">sensitive_canary</Annotation>
              </Annotations>
              <Formula>1</Formula>
            </CalculatedMember>
            """;
        String virtualCube = """
            <VirtualCube name="%s">
              <VirtualCubeMeasure cubeName="Sales" name="[Measures].[Quantity]"/>
              <VirtualCubeMeasure cubeName="Sales" name="[Measures].[Configured]"/>
            </VirtualCube>
            """;
        try (Capture capture = new Capture()) {
            loadSchema("""
                <Cube name="Sales"><Table name="fact"/>
                  <Measure name="Quantity" column="qty" aggregator="sum"/>
                  %s
                </Cube>
                %s%s
                """.formatted(member, virtualCube.formatted("First"),
                    virtualCube.formatted("Second")));
            assertEquals(1, capture.count("'nativeSql.scalar'", "boolean"),
                capture.messages.toString());
            capture.assertValueFree();
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

    private static NativeSqlConfig.NativeSqlDef load(
        Map<String, String> annotations) throws Exception
    {
        return load(annotations.entrySet());
    }

    private static NativeSqlConfig.NativeSqlDef load(
        Iterable<Map.Entry<String, String>> annotations) throws Exception
    {
        StringBuilder xml = new StringBuilder();
        annotations.forEach(annotation -> xml.append("<Annotation name=\"")
            .append(annotation.getKey()).append("\"><![CDATA[")
            .append(annotation.getValue()).append("]]></Annotation>"));
        return loadSchema("""
            <Cube name="Sales"><Table name="fact"/>
              <Measure name="Quantity" column="qty" aggregator="sum"/>
              <CalculatedMember name="Configured" dimension="Measures">
                <Annotations>%s</Annotations><Formula>1</Formula>
              </CalculatedMember>
            </Cube>
            """.formatted(xml));
    }

    /** Loads the cubes over a one-column fact table and, when they define
     *  [Configured], reads its definition three times as evaluation does. */
    private static NativeSqlConfig.NativeSqlDef loadSchema(String cubes)
        throws Exception
    {
        String jdbc = "jdbc:h2:mem:native_config_" + UUID.randomUUID().toString().replace("-", "")
            + ";DATABASE_TO_UPPER=false";
        try (java.sql.Connection db = DriverManager.getConnection(jdbc, "sa", "");
             java.sql.Statement statement = db.createStatement())
        {
            statement.execute("CREATE TABLE fact (qty INT)");
            Util.PropertyList props = Util.parseConnectString("Provider=mondrian;JdbcPassword=;");
            props.put("JdbcUser", "sa");
            props.put("JdbcDrivers", "org.h2.Driver");
            props.put("Jdbc", jdbc);
            props.put("CatalogContent",
                "<Schema name=\"NativeDiagnostics\">" + cubes + "</Schema>");
            mondrian.olap.Connection connection = mondrian.olap.DriverManager.getConnection(props, null);
            try {
                if (!cubes.contains("\"Configured\"")) {
                    return null;
                }
                var query = connection.parseQuery("SELECT {[Measures].[Configured]} ON COLUMNS FROM [Sales]");
                NativeSqlConfig.NativeSqlDef definition = null;
                for (var member : query.getMeasuresMembers()) {
                    if (member instanceof RolapMember rolap) {
                        for (int i = 0; i < 3; i++) {
                            var configured = NativeSqlConfig.findNativeSqlMember(rolap);
                            if (configured != null) {
                                definition = NativeSqlConfig.fromMember(configured);
                            }
                        }
                    }
                }
                return definition;
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
            for (String name : List.of(NativeSqlConfig.class.getName(), MondrianProperties.class.getName(),
                "mondrian.rolap.SchemaAnnotationDiagnostics")) {
                previous.put(name, context.getConfiguration().getLoggers().get(name));
                LoggerConfig logger = new LoggerConfig(name, org.apache.logging.log4j.Level.WARN, false);
                logger.addAppender(appender, org.apache.logging.log4j.Level.WARN, null);
                context.getConfiguration().removeLogger(name);
                context.getConfiguration().addLogger(name, logger);
            }
            context.updateLoggers();
        }
        long count(String... parts) {
            return messages.stream().filter(message ->
                List.of(parts).stream().allMatch(message::contains)).count();
        }
        void assertValueFree() {
            assertTrue(messages.stream().noneMatch(message ->
                message.contains("sensitive_canary") || message.contains("private_table")),
                messages.toString());
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
