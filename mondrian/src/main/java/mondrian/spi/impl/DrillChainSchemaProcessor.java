/*
 * DrillChainSchemaProcessor — generates multi-level drill hierarchies
 * from dependsOnChain annotations in Mondrian XML schema.
 *
 * For each chain (e.g., Категория → Подкатегория, ФО → Регион → Город),
 * generates a real multi-level <Hierarchy> with <Level> elements.
 * This enables native Excel drill-down without XMLA proxy or MDX rewriting.
 *
 * Usage: set in datasources.xml DataSourceInfo:
 *   DynamicSchemaProcessor=mondrian.spi.impl.DrillChainSchemaProcessor
 */
package mondrian.spi.impl;

import mondrian.olap.Util;

import java.io.*;
import java.util.*;
import java.util.regex.*;

public class DrillChainSchemaProcessor
    extends FilterDynamicSchemaProcessor
{
    @Override
    protected String filter(
        String schemaUrl,
        Util.PropertyList connectInfo,
        InputStream stream)
        throws Exception
    {
        String schema = super.filter(schemaUrl, connectInfo, stream);
        return addDrillHierarchies(schema);
    }

    @Override
    protected String filter(
        String catalog,
        Util.PropertyList connectInfo)
    {
        return addDrillHierarchies(catalog);
    }

    /**
     * Parses the schema XML, finds dependsOnChain annotations,
     * builds chains, and injects multi-level Hierarchy elements
     * into each Dimension.
     */
    private static final org.apache.logging.log4j.Logger LOGGER =
        org.apache.logging.log4j.LogManager.getLogger(
            DrillChainSchemaProcessor.class);

    public static String addDrillHierarchies(String schema) {
        LOGGER.info("DrillChainSchemaProcessor: processing schema ("
            + schema.length() + " chars)");
        // Find all Dimensions and their Hierarchies with
        // dependsOnChain annotations.
        // Build chains per dimension, then inject new Hierarchies.

        // Step 1: Parse all levels with dependsOnChain
        // Map: hierarchy_unique_ref → ChainInfo
        Map<String, LevelInfo> levelInfos = new LinkedHashMap<>();
        // Map: child_hier → parent_hier (from dependsOnChain)
        Map<String, String> childToParent = new LinkedHashMap<>();

        // Find all <Dimension> blocks
        Pattern dimPat = Pattern.compile(
            "<Dimension\\s+name=\"([^\"]+)\"[^>]*>(.*?)</Dimension>",
            Pattern.DOTALL);
        Matcher dimMat = dimPat.matcher(schema);

        Map<String, String> dimBlocks = new LinkedHashMap<>();
        Map<String, List<String[]>> dimChains = new LinkedHashMap<>();

        while (dimMat.find()) {
            String dimName = dimMat.group(1);
            String dimBody = dimMat.group(2);

            // Parse hierarchies within this dimension
            Pattern hierPat = Pattern.compile(
                "<Hierarchy\\s+name=\"([^\"]+)\"([^>]*)>(.*?)</Hierarchy>",
                Pattern.DOTALL);
            Matcher hierMat = hierPat.matcher(dimBody);

            Map<String, String> hierChildToParent = new LinkedHashMap<>();
            Map<String, LevelInfo> hierLevels = new LinkedHashMap<>();

            while (hierMat.find()) {
                String hierName = hierMat.group(1);
                String hierAttrs = hierMat.group(2);
                String hierBody = hierMat.group(3);
                String hierFullName = dimName + "." + hierName;

                // Parse Level elements
                Pattern levelPat = Pattern.compile(
                    "<Level\\s+name=\"([^\"]+)\"([^>]*?)(/?)>(.*?)(?:</Level>|(?=<Level|$))",
                    Pattern.DOTALL);
                Matcher levelMat = levelPat.matcher(hierBody);

                // Get table name
                Pattern tablePat = Pattern.compile(
                    "<Table\\s+name=\"([^\"]+)\"");
                Matcher tableMat = tablePat.matcher(hierBody);
                String tableName = tableMat.find() ? tableMat.group(1) : null;

                // Get primaryKey from hierarchy attrs
                Pattern pkPat = Pattern.compile("primaryKey=\"([^\"]+)\"");
                Matcher pkMat = pkPat.matcher(hierAttrs);
                String primaryKey = pkMat.find() ? pkMat.group(1) : null;

                // Get allMemberName
                Pattern allPat = Pattern.compile(
                    "allMemberName=\"([^\"]+)\"");
                Matcher allMat = allPat.matcher(hierAttrs);
                String allMemberName = allMat.find()
                    ? allMat.group(1) : "All";

                while (levelMat.find()) {
                    String levelName = levelMat.group(1);
                    String levelAttrs = levelMat.group(2);
                    String levelBody = levelMat.group(4);
                    if (levelBody == null) levelBody = "";

                    // Extract column
                    Pattern colPat = Pattern.compile(
                        "column=\"([^\"]+)\"");
                    Matcher colMat = colPat.matcher(levelAttrs);
                    String column = colMat.find() ? colMat.group(1) : null;

                    // Extract type
                    Pattern typePat = Pattern.compile(
                        "type=\"([^\"]+)\"");
                    Matcher typeMat = typePat.matcher(levelAttrs);
                    String type = typeMat.find() ? typeMat.group(1) : "String";

                    // Extract uniqueMembers
                    Pattern uniqPat = Pattern.compile(
                        "uniqueMembers=\"([^\"]+)\"");
                    Matcher uniqMat = uniqPat.matcher(levelAttrs);
                    String uniqueMembers = uniqMat.find()
                        ? uniqMat.group(1) : "false";

                    // Extract levelType (for time dimensions)
                    Pattern ltPat = Pattern.compile(
                        "levelType=\"([^\"]+)\"");
                    Matcher ltMat = ltPat.matcher(levelAttrs);
                    String levelType = ltMat.find()
                        ? ltMat.group(1) : null;

                    // Check for dependsOnChain annotation
                    Pattern chainPat = Pattern.compile(
                        "drilldown\\.dependsOnChain\">([^<]+)");
                    Matcher chainMat = chainPat.matcher(
                        levelBody != null ? levelBody : "");
                    if (chainMat.find()) {
                        String chain = chainMat.group(1).trim();
                        // Parse first link: [Dim.Parent].[Level]=Name
                        String firstLink = chain.split(">")[0].trim();
                        Pattern linkPat = Pattern.compile(
                            "\\[([^\\]]+)\\]\\.\\[([^\\]]+)\\]");
                        Matcher linkMat = linkPat.matcher(firstLink);
                        if (linkMat.find()) {
                            String parentHier = linkMat.group(1);
                            hierChildToParent.put(hierFullName, parentHier);
                        }
                    }

                    hierLevels.put(hierFullName, new LevelInfo(
                        dimName, hierName, hierFullName,
                        levelName, column, type, uniqueMembers,
                        tableName, primaryKey, allMemberName,
                        levelType));
                }
            }

            // Build chains for this dimension
            Set<String> allChildren = new HashSet<>(
                hierChildToParent.keySet());
            for (String parent : new LinkedHashSet<>(
                    hierChildToParent.values()))
            {
                if (allChildren.contains(parent)) continue;
                // Top of chain
                List<String> chain = new ArrayList<>();
                chain.add(parent);
                String current = parent;
                while (true) {
                    String child = null;
                    for (Map.Entry<String, String> e
                        : hierChildToParent.entrySet())
                    {
                        if (e.getValue().equals(current)) {
                            child = e.getKey();
                            break;
                        }
                    }
                    if (child == null) break;
                    chain.add(child);
                    current = child;
                }
                if (chain.size() < 2) continue;

                // Generate hierarchy XML
                LevelInfo topInfo = hierLevels.get(chain.get(0));
                if (topInfo == null) continue;

                String topLevelName = topInfo.levelName;
                String hierDrillName = topLevelName + " Drill";

                StringBuilder hierXml = new StringBuilder();
                hierXml.append("\n      <Hierarchy name=\"")
                    .append(hierDrillName)
                    .append("\" hasAll=\"true\" allMemberName=\"Все\"");
                if (topInfo.primaryKey != null) {
                    hierXml.append(" primaryKey=\"")
                        .append(topInfo.primaryKey).append("\"");
                }
                hierXml.append(">\n");
                if (topInfo.tableName != null) {
                    hierXml.append("        <Table name=\"")
                        .append(topInfo.tableName).append("\"/>\n");
                }

                for (String hierFullName : chain) {
                    LevelInfo li = hierLevels.get(hierFullName);
                    if (li == null) continue;
                    hierXml.append("        <Level name=\"")
                        .append(li.levelName).append("\"");
                    if (li.column != null) {
                        hierXml.append(" column=\"")
                            .append(li.column).append("\"");
                    }
                    if (li.type != null) {
                        hierXml.append(" type=\"")
                            .append(li.type).append("\"");
                    }
                    hierXml.append(" uniqueMembers=\"")
                        .append(li.uniqueMembers).append("\"");
                    if (li.levelType != null) {
                        hierXml.append(" levelType=\"")
                            .append(li.levelType).append("\"");
                    }
                    hierXml.append("/>\n");
                }
                hierXml.append("      </Hierarchy>");

                // Store for injection
                if (!dimBlocks.containsKey(dimName)) {
                    dimBlocks.put(dimName, "");
                }
                dimBlocks.put(dimName,
                    dimBlocks.get(dimName) + hierXml.toString());
            }
        }

        // Inject generated hierarchies before </Dimension>
        LOGGER.info("DrillChainSchemaProcessor: generated "
            + dimBlocks.size() + " dimension blocks");
        String result = schema;
        for (Map.Entry<String, String> e : dimBlocks.entrySet()) {
            String dimName = e.getKey();
            String hierXml = e.getValue();
            if (hierXml.isEmpty()) continue;
            LOGGER.info("  Injecting drill hierarchy for: " + dimName);

            // Find </Dimension> for this dimension and insert before it
            // Use regex to find the specific dimension's closing tag
            String dimClose = "(<Dimension\\s+name=\""
                + Pattern.quote(dimName) + "\"[^>]*>.*?)(</Dimension>)";
            result = result.replaceFirst(
                "(?s)" + dimClose,
                "$1" + Matcher.quoteReplacement(hierXml) + "\n    $2");
        }

        return result;
    }

    static class LevelInfo {
        final String dimName;
        final String hierName;
        final String hierFullName;
        final String levelName;
        final String column;
        final String type;
        final String uniqueMembers;
        final String tableName;
        final String primaryKey;
        final String allMemberName;
        final String levelType;

        LevelInfo(String dimName, String hierName, String hierFullName,
                  String levelName, String column, String type,
                  String uniqueMembers, String tableName,
                  String primaryKey, String allMemberName,
                  String levelType) {
            this.dimName = dimName;
            this.hierName = hierName;
            this.hierFullName = hierFullName;
            this.levelName = levelName;
            this.column = column;
            this.type = type;
            this.uniqueMembers = uniqueMembers;
            this.tableName = tableName;
            this.primaryKey = primaryKey;
            this.allMemberName = allMemberName;
            this.levelType = levelType;
        }
    }
}
