/*
 * DrillChainSchemaProcessor — generates multi-level drill hierarchies
 * from dependsOnChain annotations in Mondrian XML schema.
 *
 * For each chain (e.g., Год → Квартал → Месяц), generates a real
 * multi-level <Hierarchy> named after the leaf level (e.g. "Месяц Drill").
 * Branching chains (e.g. Год → Квартал and Год → Полугодие) produce
 * separate drill hierarchies for each branch.
 *
 * Usage (explicit): set in datasources.xml DataSourceInfo:
 *   DynamicSchemaProcessor=mondrian.spi.impl.DrillChainSchemaProcessor
 *
 * Usage (automatic): RolapSchemaPool auto-invokes addDrillHierarchies()
 * when the schema contains drilldown.dependsOnChain annotations.
 */
package mondrian.spi.impl;

import mondrian.olap.Util;

import java.io.*;
import java.util.*;
import java.util.regex.*;

public class DrillChainSchemaProcessor
    extends FilterDynamicSchemaProcessor
{
    private static final org.apache.logging.log4j.Logger LOGGER =
        org.apache.logging.log4j.LogManager.getLogger(
            DrillChainSchemaProcessor.class);

    @Override
    protected String filter(
        String schemaUrl,
        Util.PropertyList connectInfo,
        InputStream stream)
        throws Exception
    {
        return addDrillHierarchies(super.filter(schemaUrl, connectInfo, stream));
    }

    @Override
    protected String filter(String catalog, Util.PropertyList connectInfo) {
        return addDrillHierarchies(catalog);
    }

    /**
     * Parses the schema XML, finds dependsOnChain annotations, builds chains
     * (including branching), and injects multi-level Hierarchy elements named
     * after the leaf level of each chain.
     */
    public static String addDrillHierarchies(String schema) {
        LOGGER.info(
            "DrillChainSchemaProcessor: processing schema ("
            + schema.length() + " chars)");

        Pattern dimPat = Pattern.compile(
            "<Dimension\\s+name=\"([^\"]+)\"[^>]*>(.*?)</Dimension>",
            Pattern.DOTALL);
        Matcher dimMat = dimPat.matcher(schema);

        List<int[]> injectionPositions = new ArrayList<>();
        List<String> injectionXml = new ArrayList<>();

        while (dimMat.find()) {
            String dimName = dimMat.group(1);
            String dimBody = dimMat.group(2);

            Map<String, String> hierChildToParent = new LinkedHashMap<>();
            Map<String, LevelInfo> hierLevels = new LinkedHashMap<>();

            Pattern hierPat = Pattern.compile(
                "<Hierarchy\\s+name=\"([^\"]+)\"([^>]*)>(.*?)</Hierarchy>",
                Pattern.DOTALL);
            Matcher hierMat = hierPat.matcher(dimBody);

            while (hierMat.find()) {
                String hierName = hierMat.group(1);
                String hierAttrs = hierMat.group(2);
                String hierBody = hierMat.group(3);
                String hierFullName = dimName + "." + hierName;

                String tableName = extractAttr(hierBody, "(?s)<Table\\s+name=\"([^\"]+)\"");
                String primaryKey = extractAttr(hierAttrs, "primaryKey=\"([^\"]+)\"");
                String allMemberName = extractAttrOrDefault(
                    hierAttrs, "allMemberName=\"([^\"]+)\"", "Все");

                Pattern levelPat = Pattern.compile(
                    "<Level\\s+name=\"([^\"]+)\"([^>]*?)(?:/>|>(.*?)(?:</Level>|(?=<Level)))",
                    Pattern.DOTALL);
                Matcher levelMat = levelPat.matcher(hierBody);

                while (levelMat.find()) {
                    String levelName  = levelMat.group(1);
                    String levelAttrs = levelMat.group(2);
                    String levelBody  = levelMat.group(3);
                    if (levelBody == null) levelBody = "";

                    String column        = extractAttr(levelAttrs, "column=\"([^\"]+)\"");
                    String nameColumn    = extractAttr(levelAttrs, "nameColumn=\"([^\"]+)\"");
                    String ordinalColumn = extractAttr(levelAttrs, "ordinalColumn=\"([^\"]+)\"");
                    String type          = extractAttrOrDefault(levelAttrs, "type=\"([^\"]+)\"", "String");
                    String uniqueMembers = extractAttrOrDefault(
                        levelAttrs, "uniqueMembers=\"([^\"]+)\"", "false");
                    String levelType     = extractAttr(levelAttrs, "levelType=\"([^\"]+)\"");

                    // First dependsOnChain annotation found in the level body
                    Matcher chainMat = Pattern.compile(
                        "drilldown\\.dependsOnChain\">([^<]+)").matcher(levelBody);
                    if (chainMat.find()) {
                        String firstLink = chainMat.group(1).trim().split(">")[0].trim();
                        Matcher linkMat = Pattern.compile(
                            "\\[([^\\]]+)\\]\\.\\[([^\\]]+)\\]").matcher(firstLink);
                        if (linkMat.find()) {
                            hierChildToParent.put(hierFullName, linkMat.group(1));
                        }
                    }

                    hierLevels.put(hierFullName, new LevelInfo(
                        dimName, hierName, hierFullName,
                        levelName, column, nameColumn, ordinalColumn,
                        type, uniqueMembers, tableName, primaryKey,
                        allMemberName, levelType));
                }
            }

            if (hierChildToParent.isEmpty()) continue;

            Set<String> allChildren = new HashSet<>(hierChildToParent.keySet());
            Set<String> roots = new LinkedHashSet<>();
            for (String parent : hierChildToParent.values()) {
                if (!allChildren.contains(parent)) {
                    roots.add(parent);
                }
            }

            for (String root : roots) {
                List<List<String>> paths =
                    buildAllPaths(root, hierChildToParent, new HashSet<>());

                for (List<String> path : paths) {
                    if (path.size() < 2) continue;

                    LevelInfo rootInfo = hierLevels.get(path.get(0));
                    LevelInfo leafInfo = hierLevels.get(path.get(path.size() - 1));
                    if (rootInfo == null || leafInfo == null) continue;

                    String hierDrillName = leafInfo.levelName + " Drill";
                    StringBuilder hierXml = new StringBuilder();
                    hierXml.append("\n      <Hierarchy name=\"")
                        .append(hierDrillName)
                        .append("\" hasAll=\"true\" allMemberName=\"Все\"");
                    if (rootInfo.primaryKey != null) {
                        hierXml.append(" primaryKey=\"")
                            .append(rootInfo.primaryKey).append("\"");
                    }
                    hierXml.append(">\n");
                    if (rootInfo.tableName != null) {
                        hierXml.append("        <Table name=\"")
                            .append(rootInfo.tableName).append("\"/>\n");
                    }

                    for (String hierFullName : path) {
                        LevelInfo li = hierLevels.get(hierFullName);
                        if (li == null) continue;
                        hierXml.append("        <Level name=\"")
                            .append(li.levelName).append("\"");
                        if (li.column != null) {
                            hierXml.append(" column=\"").append(li.column).append("\"");
                        }
                        if (li.nameColumn != null) {
                            hierXml.append(" nameColumn=\"").append(li.nameColumn).append("\"");
                        }
                        if (li.ordinalColumn != null) {
                            hierXml.append(" ordinalColumn=\"")
                                .append(li.ordinalColumn).append("\"");
                        }
                        hierXml.append(" type=\"").append(li.type).append("\"");
                        hierXml.append(" uniqueMembers=\"").append(li.uniqueMembers).append("\"");
                        if (li.levelType != null) {
                            hierXml.append(" levelType=\"").append(li.levelType).append("\"");
                        }
                        hierXml.append("/>\n");
                    }
                    hierXml.append("      </Hierarchy>");

                    int insertPos = dimMat.start()
                        + dimMat.group(0).lastIndexOf("</Dimension>");
                    injectionPositions.add(new int[]{insertPos});
                    injectionXml.add(hierXml.toString());

                    List<String> levelNames = new ArrayList<>();
                    for (String n : path) {
                        LevelInfo li = hierLevels.get(n);
                        levelNames.add(li != null ? li.levelName : n);
                    }
                    LOGGER.info(
                        "  [" + dimName + "] chain → "
                        + String.join(" > ", levelNames)
                        + " (as \"" + hierDrillName + "\")");
                }
            }
        }

        LOGGER.info(
            "DrillChainSchemaProcessor: "
            + injectionPositions.size() + " drill hierarchies injected");

        StringBuilder result = new StringBuilder(schema);
        for (int i = injectionPositions.size() - 1; i >= 0; i--) {
            result.insert(injectionPositions.get(i)[0],
                injectionXml.get(i) + "\n    ");
        }
        return result.toString();
    }

    /** Returns all paths (root to leaf) in the child→parent graph. */
    private static List<List<String>> buildAllPaths(
        String node,
        Map<String, String> hierChildToParent,
        Set<String> visited)
    {
        List<String> children = new ArrayList<>();
        for (Map.Entry<String, String> e : hierChildToParent.entrySet()) {
            if (e.getValue().equals(node) && !visited.contains(e.getKey())) {
                children.add(e.getKey());
            }
        }
        if (children.isEmpty()) {
            List<String> leaf = new ArrayList<>();
            leaf.add(node);
            return Collections.singletonList(leaf);
        }
        Set<String> nextVisited = new HashSet<>(visited);
        nextVisited.add(node);
        List<List<String>> result = new ArrayList<>();
        for (String child : children) {
            for (List<String> suffix
                : buildAllPaths(child, hierChildToParent, nextVisited))
            {
                List<String> path = new ArrayList<>();
                path.add(node);
                path.addAll(suffix);
                result.add(path);
            }
        }
        return result;
    }

    private static String extractAttr(String src, String regex) {
        Matcher m = Pattern.compile(regex).matcher(src);
        return m.find() ? m.group(1) : null;
    }

    private static String extractAttrOrDefault(String src, String regex, String def) {
        String v = extractAttr(src, regex);
        return v != null ? v : def;
    }

    static class LevelInfo {
        final String dimName, hierName, hierFullName;
        final String levelName, column, nameColumn, ordinalColumn;
        final String type, uniqueMembers, tableName, primaryKey;
        final String allMemberName, levelType;

        LevelInfo(
            String dimName, String hierName, String hierFullName,
            String levelName, String column, String nameColumn,
            String ordinalColumn, String type, String uniqueMembers,
            String tableName, String primaryKey,
            String allMemberName, String levelType)
        {
            this.dimName = dimName;
            this.hierName = hierName;
            this.hierFullName = hierFullName;
            this.levelName = levelName;
            this.column = column;
            this.nameColumn = nameColumn;
            this.ordinalColumn = ordinalColumn;
            this.type = type;
            this.uniqueMembers = uniqueMembers;
            this.tableName = tableName;
            this.primaryKey = primaryKey;
            this.allMemberName = allMemberName;
            this.levelType = levelType;
        }
    }
}
