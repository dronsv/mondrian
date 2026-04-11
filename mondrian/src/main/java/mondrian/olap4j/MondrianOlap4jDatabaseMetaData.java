/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
*/

package mondrian.olap4j;

import mondrian.olap.Util;
import mondrian.rolap.RolapConnection;
import mondrian.xmla.XmlaUtil;

import org.olap4j.*;
import org.olap4j.metadata.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Implementation of {@link org.olap4j.OlapDatabaseMetaData}
 * for the Mondrian OLAP engine.
 *
 * <p>This class has sub-classes which implement JDBC 3.0 and JDBC 4.0 APIs;
 * it is instantiated using {@link Factory#newDatabaseMetaData}.</p>
 *
 * @author jhyde
 * @since May 23, 2007
 */
abstract class MondrianOlap4jDatabaseMetaData implements OlapDatabaseMetaData {
    final MondrianOlap4jConnection olap4jConnection;

    private static final Comparator<Catalog> CATALOG_COMP =
        new Comparator<Catalog>() {
            @Override
            public int compare(Catalog o1, Catalog o2) {
                return o1.getName().compareTo(o2.getName());
            }
        };

    private static final Comparator<Schema> SCHEMA_COMP =
        new Comparator<Schema>() {
            @Override
            public int compare(Schema o1, Schema o2) {
                return o1.getName().compareTo(o2.getName());
            }
        };

    /**
     * Creates a MondrianOlap4jDatabaseMetaData.
     *
     * @param olap4jConnection Connection
     * @param mondrianConnection Mondrian connection
     */
    MondrianOlap4jDatabaseMetaData(
        MondrianOlap4jConnection olap4jConnection,
        RolapConnection mondrianConnection)
    {
        this.olap4jConnection = olap4jConnection;
    }

    // helpers

    /**
     * Executes a metadata query and returns the result as a JDBC
     * {@link ResultSet}.
     *
     * <p>The XMLA specification usually specifies that the returned list is
     * ordered by particular attributes. XMLA notwithstanding, the result from
     * this method is always ordered.
     *
     * @param methodName Name of the metadata request. Corresponds to the XMLA
     * method name, e.g. "MDSCHEMA_CUBES"
     *
     * @param patternValues Array of alternating parameter name and value
     * pairs. If the parameter value is null, it is ignored.
     *
     * @return Result set of metadata
     *
     * @throws org.olap4j.OlapException on error
     */
    private ResultSet getMetadata(
        String methodName,
        Object... patternValues)
        throws OlapException
    {
        Map<String, Object> restrictionMap =
            new HashMap<String, Object>();
        assert patternValues.length % 2 == 0;
        for (int i = 0; i < patternValues.length / 2; ++i) {
            final String key = (String) patternValues[i * 2];
            Object value = patternValues[i * 2 + 1];
            if (value != null) {
                if (value instanceof String) {
                    value = Collections.singletonList((String) value);
                }
                restrictionMap.put(key, value);
            }
        }
        XmlaUtil.MetadataRowset rowset =
            XmlaUtil.getMetadataRowset(
                olap4jConnection,
                methodName,
                restrictionMap);
        return olap4jConnection.factory.newFixedResultSet(
            olap4jConnection, rowset.headerList, rowset.rowList);
    }

    /**
     * Wraps a string in an object that indicates that it is to be treated as
     * a wildcard pattern, not a literal match.
     *
     * @param pattern Pattern
     * @return Wildcard, or null if pattern is null
     */
    private XmlaUtil.Wildcard wildcard(String pattern) {
        return pattern == null
            ? null
            : new XmlaUtil.Wildcard(pattern);
    }

    // implement DatabaseMetaData

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getURL() throws SQLException {
        return olap4jConnection.getMondrianConnection().getConnectString();
    }

    @Override
    public String getUserName() throws SQLException {
        // mondrian does not support a user name property
        return null;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        // all mondrian databases are read-only
        return true;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return olap4jConnection.mondrianServer.getVersion().getProductName();
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return olap4jConnection.mondrianServer.getVersion().getVersionString();
    }

    @Override
    public String getDriverName() throws SQLException {
        return olap4jConnection.driver.getName();
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return olap4jConnection.driver.getVersion();
    }

    @Override
    public int getDriverMajorVersion() {
        return olap4jConnection.driver.getMajorVersion();
    }

    @Override
    public int getDriverMinorVersion() {
        return olap4jConnection.driver.getMinorVersion();
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getStringFunctions() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsConvert(
        int fromType, int toType) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames()
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions()
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMaxConnections() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMaxStatements() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions()
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly()
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getProcedures(
        String catalog,
        String schemaPattern,
        String procedureNamePattern) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getProcedureColumns(
        String catalog,
        String schemaPattern,
        String procedureNamePattern,
        String columnNamePattern) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getTables(
        String catalog,
        String schemaPattern,
        String tableNamePattern,
        String types[]) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getSchemas() throws OlapException {
        if (false) {
            // Do not use DBSCHEMA_SCHEMATA: it has different columns than the
            // JDBC spec requires
            return getMetadata("DBSCHEMA_SCHEMATA");
        }
        List<String> headerList =
            Arrays.asList("TABLE_SCHEM", "TABLE_CAT");
        List<List<Object>> rowList = new ArrayList<List<Object>>();
        for (Catalog catalog
                : Util.sort(
                    olap4jConnection.getOlapCatalogs(),
                    CATALOG_COMP))
        {
            for (Schema schema
                    : Util.sort(
                        catalog.getSchemas(),
                        SCHEMA_COMP))
            {
                rowList.add(
                    Arrays.<Object>asList(
                        schema.getName(),
                        catalog.getName()));
            }
        }
        return olap4jConnection.factory.newFixedResultSet(
            olap4jConnection, headerList, rowList);
    }

    @Override
    public ResultSet getCatalogs() throws OlapException {
        if (false) {
            // Do not use DBSCHEMA_CATALOGS: it has different columns than the
            // JDBC spec requires
            return getMetadata("DBSCHEMA_CATALOGS");
        }

        List<String> headerList =
            Arrays.asList("TABLE_CAT");
        List<List<Object>> rowList = new ArrayList<List<Object>>();
        for (Catalog catalog
                : Util.sort(
                    olap4jConnection.getOlapCatalogs(),
                    CATALOG_COMP))
        {
            rowList.add(
                Collections.<Object>singletonList(catalog.getName()));
        }
        return olap4jConnection.factory.newFixedResultSet(
            olap4jConnection, headerList, rowList);
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getColumns(
        String catalog,
        String schemaPattern,
        String tableNamePattern,
        String columnNamePattern) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getColumnPrivileges(
        String catalog,
        String schema,
        String table,
        String columnNamePattern) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getTablePrivileges(
        String catalog,
        String schemaPattern,
        String tableNamePattern) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getBestRowIdentifier(
        String catalog,
        String schema,
        String table,
        int scope,
        boolean nullable) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getVersionColumns(
        String catalog, String schema, String table) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getPrimaryKeys(
        String catalog, String schema, String table) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getImportedKeys(
        String catalog, String schema, String table) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getExportedKeys(
        String catalog, String schema, String table) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getCrossReference(
        String parentCatalog,
        String parentSchema,
        String parentTable,
        String foreignCatalog,
        String foreignSchema,
        String foreignTable) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getIndexInfo(
        String catalog,
        String schema,
        String table,
        boolean unique,
        boolean approximate) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsResultSetConcurrency(
        int type, int concurrency) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getUDTs(
        String catalog,
        String schemaPattern,
        String typeNamePattern,
        int[] types) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public OlapConnection getConnection() {
        return olap4jConnection;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getSuperTypes(
        String catalog,
        String schemaPattern,
        String typeNamePattern) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getSuperTables(
        String catalog,
        String schemaPattern,
        String tableNamePattern) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getAttributes(
        String catalog,
        String schemaPattern,
        String typeNamePattern,
        String attributeNamePattern) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return olap4jConnection.mondrianServer.getVersion().getMajorVersion();
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return olap4jConnection.mondrianServer.getVersion().getMinorVersion();
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        // mondrian olap4j supports jdbc 4.0
        return 4;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        // mondrian olap4j supports jdbc 4.0
        return 0;
    }

    @Override
    public int getSQLStateType() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        throw new UnsupportedOperationException();
    }

    // implement java.sql.Wrapper

    // straightforward implementation of unwrap and isWrapperFor, since this
    // class already implements the interface they most likely require:
    // DatabaseMetaData and OlapDatabaseMetaData

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw olap4jConnection.helper.createException(
            "does not implement '" + iface + "'");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    // implement OlapDatabaseMetaData

    @Override
    public Set<CellSetListener.Granularity>
    getSupportedCellSetListenerGranularities()
        throws OlapException
    {
        // Cell set listener API not supported in this version of mondrian.
        return Collections.emptySet();
    }

    @Override
    public ResultSet getActions(
        String catalog,
        String schemaPattern,
        String cubeNamePattern,
        String actionNamePattern) throws OlapException
    {
        return getMetadata(
            "MDSCHEMA_ACTIONS",
            "SCHEMA_NAME", wildcard(schemaPattern),
            "CUBE_NAME", wildcard(cubeNamePattern),
            "ACTION_NAME", wildcard(actionNamePattern));
    }

    @Override
    public ResultSet getDatabases() throws OlapException {
        return getMetadata("DISCOVER_DATASOURCES");
    }

    @Override
    public ResultSet getLiterals() throws OlapException {
        return getMetadata("DISCOVER_LITERALS");
    }

    @Override
    public ResultSet getDatabaseProperties(
        String dataSourceName,
        String propertyNamePattern) throws OlapException
    {
        return getMetadata("DISCOVER_PROPERTIES");
    }

    @Override
    public ResultSet getProperties(
        String catalog,
        String schemaPattern,
        String cubeNamePattern,
        String dimensionUniqueName,
        String hierarchyUniqueName,
        String levelUniqueName,
        String memberUniqueName,
        String propertyNamePattern) throws OlapException
    {
        return getMetadata(
            "MDSCHEMA_PROPERTIES",
            "CATALOG_NAME", catalog,
            "SCHEMA_NAME", wildcard(schemaPattern),
            "CUBE_NAME", wildcard(cubeNamePattern),
            "DIMENSION_UNIQUE_NAME", dimensionUniqueName,
            "HIERARCHY_UNIQUE_NAME", hierarchyUniqueName,
            "LEVEL_UNIQUE_NAME", levelUniqueName,
            "MEMBER_UNIQUE_NAME", memberUniqueName,
            "PROPERTY_NAME", wildcard(propertyNamePattern));
    }

    @Override
    public String getMdxKeywords() throws OlapException {
        StringBuilder buf = new StringBuilder();
        for (String keyword : olap4jConnection.mondrianServer.getKeywords()) {
            if (buf.length() > 0) {
                buf.append(',');
            }
            buf.append(keyword);
        }
        return buf.toString();
    }

    @Override
    public ResultSet getCubes(
        String catalog,
        String schemaPattern,
        String cubeNamePattern)
        throws OlapException
    {
        return getMetadata(
            "MDSCHEMA_CUBES",
            "CATALOG_NAME", catalog,
            "SCHEMA_NAME", wildcard(schemaPattern),
            "CUBE_NAME", wildcard(cubeNamePattern));
    }

    @Override
    public ResultSet getDimensions(
        String catalog,
        String schemaPattern,
        String cubeNamePattern,
        String dimensionNamePattern)
        throws OlapException
    {
        return getMetadata(
            "MDSCHEMA_DIMENSIONS",
            "SCHEMA_NAME", wildcard(schemaPattern),
            "CUBE_NAME", wildcard(cubeNamePattern),
            "DIMENSION_NAME", wildcard(dimensionNamePattern));
    }

    @Override
    public ResultSet getOlapFunctions(
        String functionNamePattern) throws OlapException
    {
        return getMetadata(
            "MDSCHEMA_FUNCTIONS",
            "FUNCTION_NAME", wildcard(functionNamePattern));
    }

    @Override
    public ResultSet getHierarchies(
        String catalog,
        String schemaPattern,
        String cubeNamePattern,
        String dimensionUniqueName,
        String hierarchyNamePattern)
        throws OlapException
    {
        return getMetadata(
            "MDSCHEMA_HIERARCHIES",
            "CATALOG_NAME", catalog,
            "SCHEMA_NAME", wildcard(schemaPattern),
            "CUBE_NAME", wildcard(cubeNamePattern),
            "DIMENSION_UNIQUE_NAME", dimensionUniqueName,
            "HIERARCHY_NAME", wildcard(hierarchyNamePattern));
    }

    @Override
    public ResultSet getMeasures(
        String catalog,
        String schemaPattern,
        String cubeNamePattern,
        String measureNamePattern,
        String measureUniqueName) throws OlapException
    {
        return getMetadata(
            "MDSCHEMA_MEASURES",
            "CATALOG_NAME", catalog,
            "SCHEMA_NAME", wildcard(schemaPattern),
            "CUBE_NAME", wildcard(cubeNamePattern),
            "MEASURE_NAME", wildcard(measureNamePattern),
            "MEASURE_UNIQUE_NAME", measureUniqueName);
    }

    @Override
    public ResultSet getMembers(
        String catalog,
        String schemaPattern,
        String cubeNamePattern,
        String dimensionUniqueName,
        String hierarchyUniqueName,
        String levelUniqueName,
        String memberUniqueName,
        Set<Member.TreeOp> treeOps) throws OlapException
    {
        String treeOpString;
        if (treeOps != null) {
            final int mask =
                Member.TreeOp.getDictionary().toMask(treeOps);
            treeOpString = String.valueOf(mask);
        } else {
            treeOpString = null;
        }
        return getMetadata(
            "MDSCHEMA_MEMBERS",
            "CATALOG_NAME", catalog,
            "SCHEMA_NAME", wildcard(schemaPattern),
            "CUBE_NAME", wildcard(cubeNamePattern),
            "DIMENSION_UNIQUE_NAME", dimensionUniqueName,
            "HIERARCHY_UNIQUE_NAME", hierarchyUniqueName,
            "LEVEL_UNIQUE_NAME", levelUniqueName,
            "MEMBER_UNIQUE_NAME", memberUniqueName,
            "TREE_OP", treeOpString);
    }

    @Override
    public ResultSet getLevels(
        String catalog,
        String schemaPattern,
        String cubeNamePattern,
        String dimensionUniqueName,
        String hierarchyUniqueName,
        String levelNamePattern) throws OlapException
    {
        return getMetadata(
            "MDSCHEMA_LEVELS",
            "CATALOG_NAME", catalog,
            "SCHEMA_NAME", wildcard(schemaPattern),
            "CUBE_NAME", wildcard(cubeNamePattern),
            "DIMENSION_UNIQUE_NAME", dimensionUniqueName,
            "HIERARCHY_UNIQUE_NAME", hierarchyUniqueName,
            "LEVEL_NAME", wildcard(levelNamePattern));
    }

    @Override
    public ResultSet getSets(
        String catalog,
        String schemaPattern,
        String cubeNamePattern,
        String setNamePattern) throws OlapException
    {
        return getMetadata(
            "MDSCHEMA_SETS",
            "CATALOG_NAME", catalog,
            "SCHEMA_NAME", wildcard(schemaPattern),
            "CUBE_NAME", wildcard(cubeNamePattern),
            "SET_NAME", wildcard(setNamePattern));
    }
}

// End MondrianOlap4jDatabaseMetaData.java
