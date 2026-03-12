/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap.sql;

import mondrian.olap.MondrianProperties;
import mondrian.spi.Dialect;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Builds an allowlisted ClickHouse {@code SETTINGS} clause from Mondrian
 * properties.
 */
public final class ClickHouseSqlSettingsSupport {
    private static final Logger LOGGER =
        LogManager.getLogger(ClickHouseSqlSettingsSupport.class);

    public static final String PROP_ENABLED =
        "mondrian.clickhouse.sqlSettings.enabled";
    public static final String PROP_GLOBAL =
        "mondrian.clickhouse.sqlSettings.global";
    public static final String PROP_BY_CATALOG =
        "mondrian.clickhouse.sqlSettings.byCatalog";
    public static final String PROP_ALLOWLIST =
        "mondrian.clickhouse.sqlSettings.allowlist";

    private static final int MAX_VALUE_LENGTH = 128;
    private static final Pattern SETTING_NAME_PATTERN =
        Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final Pattern IDENTIFIER_VALUE_PATTERN =
        Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final Pattern NUMERIC_VALUE_PATTERN =
        Pattern.compile("-?\\d+(?:\\.\\d+)?(?:[eE][+-]?\\d+)?");
    private static final Pattern QUOTED_VALUE_PATTERN =
        Pattern.compile("'(?:''|[^'])*'");

    private static final Set<String> DEFAULT_ALLOWLIST;
    static {
        final LinkedHashSet<String> allowlist = new LinkedHashSet<String>();
        allowlist.addAll(Arrays.asList(
            "max_threads",
            "max_execution_time",
            "max_memory_usage",
            "max_bytes_before_external_group_by",
            "max_bytes_before_external_sort",
            "max_rows_to_read",
            "max_bytes_to_read",
            "max_result_rows",
            "max_result_bytes",
            "readonly"));
        DEFAULT_ALLOWLIST = Collections.unmodifiableSet(allowlist);
    }

    private ClickHouseSqlSettingsSupport() {
    }

    public static String buildSettingsClause(Dialect dialect, String catalog) {
        if (!isEnabled() || !isClickHouseDialect(dialect)) {
            return null;
        }
        final Set<String> allowlist =
            parseAllowlist(MondrianProperties.instance().getProperty(
                PROP_ALLOWLIST));
        if (allowlist.isEmpty()) {
            return null;
        }

        final LinkedHashMap<String, String> settings =
            new LinkedHashMap<String, String>();
        mergeSettings(
            settings,
            MondrianProperties.instance().getProperty(PROP_GLOBAL),
            allowlist);
        final String catalogSettings =
            resolveCatalogSettings(catalog, MondrianProperties.instance()
                .getProperty(PROP_BY_CATALOG));
        mergeSettings(settings, catalogSettings, allowlist);
        if (settings.isEmpty()) {
            return null;
        }

        final StringBuilder buf = new StringBuilder("SETTINGS ");
        int i = 0;
        for (Map.Entry<String, String> entry : settings.entrySet()) {
            if (i++ > 0) {
                buf.append(", ");
            }
            buf.append(entry.getKey()).append('=').append(entry.getValue());
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "Applying ClickHouse SQL settings"
                + " (catalog=" + normalizeCatalog(catalog) + "): "
                + settings);
        }
        return buf.toString();
    }

    static Set<String> parseAllowlist(String rawValue) {
        if (rawValue == null || rawValue.trim().isEmpty()) {
            return DEFAULT_ALLOWLIST;
        }
        final LinkedHashSet<String> allowlist = new LinkedHashSet<String>();
        final String normalized = rawValue.replace(';', ',');
        for (String token : normalized.split(",")) {
            final String name = normalizeSettingName(token);
            if (name == null) {
                continue;
            }
            allowlist.add(name);
        }
        return allowlist;
    }

    static String resolveCatalogSettings(String catalog, String rawMap) {
        if (rawMap == null || rawMap.trim().isEmpty()) {
            return null;
        }
        final String normalizedCatalog = normalizeCatalog(catalog);
        if (normalizedCatalog == null) {
            return null;
        }
        for (String token : rawMap.split(";")) {
            final String entry = token == null ? "" : token.trim();
            if (entry.isEmpty()) {
                continue;
            }
            final int separator = entry.indexOf('=');
            if (separator <= 0 || separator >= entry.length() - 1) {
                continue;
            }
            final String catalogName =
                normalizeCatalog(entry.substring(0, separator));
            if (catalogName == null) {
                continue;
            }
            if (!catalogName.equals(normalizedCatalog)) {
                continue;
            }
            final String catalogSettings = entry.substring(separator + 1).trim();
            return catalogSettings.isEmpty() ? null : catalogSettings;
        }
        return null;
    }

    static void mergeSettings(
        Map<String, String> target,
        String rawSettings,
        Set<String> allowlist)
    {
        if (rawSettings == null || rawSettings.trim().isEmpty()) {
            return;
        }
        final String normalized = rawSettings.replace(';', ',');
        for (String token : normalized.split(",")) {
            final String entry = token == null ? "" : token.trim();
            if (entry.isEmpty()) {
                continue;
            }
            final int eq = entry.indexOf('=');
            if (eq <= 0 || eq >= entry.length() - 1) {
                continue;
            }
            final String settingName =
                normalizeSettingName(entry.substring(0, eq));
            final String settingValue = entry.substring(eq + 1).trim();
            if (settingName == null
                || !allowlist.contains(settingName)
                || !isValidSettingValue(settingValue))
            {
                continue;
            }
            target.put(settingName, settingValue);
        }
    }

    private static boolean isEnabled() {
        final String value =
            MondrianProperties.instance().getProperty(PROP_ENABLED);
        if (value == null) {
            return false;
        }
        final String normalized = value.trim().toLowerCase(Locale.ROOT);
        return "true".equals(normalized)
            || "1".equals(normalized)
            || "on".equals(normalized)
            || "yes".equals(normalized);
    }

    private static boolean isClickHouseDialect(Dialect dialect) {
        return dialect != null
            && dialect.getDatabaseProduct() == Dialect.DatabaseProduct.CLICKHOUSE;
    }

    private static String normalizeSettingName(String value) {
        if (value == null) {
            return null;
        }
        final String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        if (!SETTING_NAME_PATTERN.matcher(trimmed).matches()) {
            return null;
        }
        return trimmed.toLowerCase(Locale.ROOT);
    }

    private static String normalizeCatalog(String catalog) {
        if (catalog == null) {
            return null;
        }
        final String trimmed = catalog.trim();
        return trimmed.isEmpty() ? null : trimmed.toLowerCase(Locale.ROOT);
    }

    private static boolean isValidSettingValue(String value) {
        if (value == null) {
            return false;
        }
        final String trimmed = value.trim();
        if (trimmed.isEmpty() || trimmed.length() > MAX_VALUE_LENGTH) {
            return false;
        }
        return NUMERIC_VALUE_PATTERN.matcher(trimmed).matches()
            || IDENTIFIER_VALUE_PATTERN.matcher(trimmed).matches()
            || QUOTED_VALUE_PATTERN.matcher(trimmed).matches();
    }
}

