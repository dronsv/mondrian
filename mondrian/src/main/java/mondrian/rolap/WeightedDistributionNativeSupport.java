/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Sergei Semenkov
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.calc.Calc;
import mondrian.calc.impl.GenericCalc;
import mondrian.olap.Annotation;
import mondrian.olap.Axis;
import mondrian.olap.Exp;
import mondrian.olap.Member;
import mondrian.olap.MondrianDef;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Position;
import mondrian.olap.Property;
import mondrian.olap.Query;
import mondrian.olap.Result;
import mondrian.olap.Util;
import mondrian.resource.MondrianResource;
import mondrian.server.Locus;
import mondrian.spi.Dialect;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * ClickHouse-oriented native evaluator for the current Weighted Distribution
 * measure shape used in confectionery schemas.
 */
final class WeightedDistributionNativeSupport {
    private static final Logger LOGGER =
        LogManager.getLogger(WeightedDistributionNativeSupport.class);
    private static final String CACHE_KEY_PREFIX =
        WeightedDistributionNativeSupport.class.getName() + ".queryCache.";
    private static final String VIRTUAL_CUBE_NAME = "Кондитерка";
    private static final String BASE_CUBE_NAME = "Продажи";
    private static final String PRODUCT_DIMENSION_NAME = "Продукт";
    private static final String STORE_ADDRESS_HIERARCHY_NAME = "Адрес";
    private static final String STORE_RESET_HIERARCHY_NAME = "Адрес";
    private static final String SALES_MEASURE_NAME = "Продажи руб";
    private static final String WD_FORMULA_FRAGMENT_1 =
        "SUM(FILTER([ТТ.АДРЕС].[АДРЕС].MEMBERS";
    private static final String WD_FORMULA_FRAGMENT_1B =
        "NOTISEMPTY([MEASURES].[ПРОДАЖИРУБ])";
    private static final String WD_FORMULA_FRAGMENT_2 =
        "[ПРОДУКТ.Категория].[Все категории]".replaceAll("\\s+", "")
            .toUpperCase(Locale.ROOT);
    private static final String WD_FORMULA_FRAGMENT_3 =
        "[ПРОДУКТ.БРЕНД].[ВСЕБРЕНДЫ]";
    private static final String WD_FORMULA_FRAGMENT_4 =
        "[ПРОДУКТ.СКЮ].[ВСЕСКЮ]";
    private static final String WD_FORMULA_FRAGMENT_5 =
        "[ПРОДУКТ.ПРОИЗВОДИТЕЛЬ].[ВСЕПРОИЗВОДИТЕЛИ]";
    private static final String WD_FORMULA_FRAGMENT_6 =
        "[ТТ.АДРЕС].[ВСЕАДРЕСА]";
    private static volatile boolean loggedCreateSample;
    private static volatile boolean loggedPlanSample;

    private WeightedDistributionNativeSupport() {
    }

    private static void logCreate(String message) {
        if (loggedCreateSample) {
            return;
        }
        loggedCreateSample = true;
        LOGGER.info("WD native create: {}", message);
    }

    private static void logPlan(String message) {
        if (loggedPlanSample) {
            return;
        }
        loggedPlanSample = true;
        LOGGER.info("WD native plan: {}", message);
    }

    static Calc maybeCreateCalc(
        RolapCalculatedMember member,
        RolapEvaluatorRoot root)
    {
        if (!MondrianProperties.instance()
            .EnableNativeWeightedDistribution.get())
        {
            return null;
        }
        if (root == null
            || root.currentDialect == null
            || root.currentDialect.getDatabaseProduct()
                != Dialect.DatabaseProduct.CLICKHOUSE)
        {
            return null;
        }
        if (!(root instanceof RolapResult.RolapResultEvaluatorRoot)) {
            logCreate("skip root=" + root.getClass().getName());
            return null;
        }
        final RolapCube cube = member.getBaseCube() != null
            ? member.getBaseCube()
            : root.cube;
        if (cube == null || !isSupportedCube(cube, root.cube)) {
            logCreate("skip unsupported cube=" + (cube == null ? "<null>" : cube.getName()));
            return null;
        }
        if (!matchesWeightedDistributionFormula(member.getExpression())) {
            logCreate("skip formula mismatch member=" + member.getUniqueName());
            return null;
        }
        final RolapStoredMeasure salesMeasure =
            resolveSalesMeasure(cube);
        if (salesMeasure == null) {
            logCreate("skip sales measure unresolved cube=" + cube.getName());
            return null;
        }
        final Calc fallback = root.getCompiled(member.getExpression(), true, null);
        logCreate(
            "enabled member=" + member.getUniqueName()
            + " execCube=" + cube.getName()
            + " queryCube=" + (root.cube == null ? "<null>" : root.cube.getName()));
        return new WeightedDistributionNativeCalc(
            member,
            root,
            cube,
            salesMeasure,
            fallback);
    }

    static boolean matchesWeightedDistributionFormula(Exp exp) {
        if (exp == null) {
            return false;
        }
        final String normalized =
            exp.toString()
                .replaceAll("\\s+", "")
                .toUpperCase(Locale.ROOT);
        return normalized.contains(WD_FORMULA_FRAGMENT_1)
            && normalized.contains(WD_FORMULA_FRAGMENT_1B)
            && normalized.contains(WD_FORMULA_FRAGMENT_2)
            && normalized.contains(WD_FORMULA_FRAGMENT_3)
            && normalized.contains(WD_FORMULA_FRAGMENT_4)
            && normalized.contains(WD_FORMULA_FRAGMENT_5)
            && normalized.contains(WD_FORMULA_FRAGMENT_6);
    }

    private static boolean isSupportedCube(
        RolapCube executionCube,
        RolapCube queryCube)
    {
        if (BASE_CUBE_NAME.equals(executionCube.getName())
            && !executionCube.isVirtual())
        {
            return true;
        }
        return VIRTUAL_CUBE_NAME.equals(executionCube.getName())
            || queryCube != null
            && VIRTUAL_CUBE_NAME.equals(queryCube.getName());
    }

    static long estimateCellStoreEvaluations(Result result, int storeCount) {
        if (result == null || storeCount <= 0) {
            return 0L;
        }
        long cells = 1L;
        for (Axis axis : result.getAxes()) {
            if (axis == null) {
                continue;
            }
            cells = safeMultiply(cells, axis.getPositions().size());
        }
        return safeMultiply(cells, storeCount);
    }

    private static long safeMultiply(long left, long right) {
        if (left <= 0L || right <= 0L) {
            return 0L;
        }
        if (left > Long.MAX_VALUE / right) {
            return Long.MAX_VALUE;
        }
        return left * right;
    }

    private static RolapStoredMeasure resolveSalesMeasure(RolapCube cube) {
        final RolapStar.Measure starMeasure =
            cube.getStar().getFactTable()
                .lookupMeasureByName(cube.getName(), SALES_MEASURE_NAME);
        if (starMeasure == null) {
            return null;
        }
        for (Member member : cube.getMeasuresMembers()) {
            if (member instanceof RolapStoredMeasure
                && SALES_MEASURE_NAME.equals(member.getName()))
            {
                return (RolapStoredMeasure) member;
            }
        }
        return null;
    }

    static RolapLevel resolveBindingLevel(
        RolapCube cube,
        RolapLevel level)
    {
        if (level == null || cube == null) {
            return level;
        }
        final RolapCubeLevel baseLevel = cube.findBaseCubeLevel(level);
        return baseLevel == null ? level : baseLevel;
    }

    private static final class WeightedDistributionNativeCalc extends GenericCalc {
        private final RolapCalculatedMember member;
        private final RolapEvaluatorRoot root;
        private final RolapCube cube;
        private final RolapStoredMeasure salesMeasure;
        private final Calc fallback;

        private WeightedDistributionNativeCalc(
            RolapCalculatedMember member,
            RolapEvaluatorRoot root,
            RolapCube cube,
            RolapStoredMeasure salesMeasure,
            Calc fallback)
        {
            super(member.getExpression());
            this.member = member;
            this.root = root;
            this.cube = cube;
            this.salesMeasure = salesMeasure;
            this.fallback = fallback;
        }

        public Object evaluate(mondrian.olap.Evaluator evaluator) {
            final RolapEvaluator rolapEvaluator = (RolapEvaluator) evaluator;
            final QueryCache cache = getOrBuildQueryCache(rolapEvaluator);
            if (cache == null) {
                return fallback.evaluate(evaluator);
            }
            final LookupKey key = cache.lookupKey(rolapEvaluator);
            final Double value = cache.values.get(key);
            if (value == null) {
                return fallback.evaluate(evaluator);
            }
            return value;
        }

        private QueryCache getOrBuildQueryCache(RolapEvaluator evaluator) {
            final RolapResult result =
                ((RolapResult.RolapResultEvaluatorRoot) root).result;
            final QueryCache batchCache =
                getOrBuildBatchCache(result, evaluator);
            if (batchCache != null) {
                return batchCache;
            }
            final QueryCache contextCache = getOrBuildContextCache(evaluator);
            if (contextCache != null) {
                return contextCache;
            }
            maybeThrowGuardrail(result, cube, evaluator);
            return null;
        }

        private QueryCache getOrBuildBatchCache(
            RolapResult result,
            RolapEvaluator evaluator)
        {
            if (!QueryPlan.hasCompleteAxes(result)) {
                return null;
            }
            final String cacheKey = CACHE_KEY_PREFIX + member.getUniqueName();
            final Object cached = root.getCacheResult(cacheKey);
            if (cached instanceof QueryCache) {
                return (QueryCache) cached;
            }
            final QueryPlan plan =
                QueryPlan.tryCreateBatchPlan(cube, salesMeasure, result, evaluator);
            if (plan == null) {
                logPlan("batch plan unavailable");
                return null;
            }
            final QueryCache cache = executePlan(plan);
            root.putCacheResult(cacheKey, cache, true);
            return cache;
        }

        private QueryCache getOrBuildContextCache(RolapEvaluator evaluator) {
            final String cacheKey = buildContextCacheKey(evaluator);
            final Object cached = root.getCacheResult(cacheKey);
            if (cached instanceof QueryCache) {
                return (QueryCache) cached;
            }
            final QueryPlan plan =
                QueryPlan.tryCreateCurrentContextPlan(cube, salesMeasure, evaluator);
            if (plan == null) {
                logPlan("context plan unavailable key=" + cacheKey);
                return null;
            }
            logPlan("context plan ready key=" + cacheKey);
            final QueryCache cache = executePlan(plan);
            root.putCacheResult(cacheKey, cache, true);
            return cache;
        }

        private String buildContextCacheKey(RolapEvaluator evaluator) {
            final StringBuilder key =
                new StringBuilder(CACHE_KEY_PREFIX)
                    .append(member.getUniqueName())
                    .append(".context");
            for (Member memberObj : evaluator.getNonAllMembers()) {
                if (!(memberObj instanceof RolapMember)) {
                    continue;
                }
                final RolapMember current = RolapUtil.strip((RolapMember) memberObj);
                if (current == null || current.isAll() || current.isCalculated()) {
                    continue;
                }
                final RolapHierarchy hierarchy = current.getHierarchy();
                if (hierarchy.getDimension().isMeasures()) {
                    continue;
                }
                key.append('|')
                    .append(hierarchy.getUniqueName())
                    .append('=')
                    .append(String.valueOf(current.getKey()));
            }
            return key.toString();
        }

        private void maybeThrowGuardrail(
            RolapResult result,
            RolapCube cube,
            RolapEvaluator evaluator)
        {
            final int threshold = MondrianProperties.instance()
                .NativeWeightedDistributionGuardrailMaxEstimatedCellStoreEvaluations
                .get();
            if (threshold <= 0) {
                return;
            }
            final int storeCount =
                approximateStoreCount(cube, evaluator.getSchemaReader());
            final long estimated =
                estimateCellStoreEvaluations(result, storeCount);
            if (estimated <= threshold) {
                return;
            }
            throw MondrianResource.instance()
                .NativeEvaluationUnsupported.ex(
                    "WeightedDistribution: estimated cell-store evaluations "
                    + estimated
                    + " exceed guardrail "
                    + threshold
                    + "; native WD batching was not applicable");
        }

        private QueryCache executePlan(QueryPlan plan) {
            final Dialect dialect = root.currentDialect;
            final String sql = plan.toSql(dialect);
            final SqlStatement stmt =
                RolapUtil.executeQuery(
                    root.schemaReader.getDataSource(),
                    sql,
                    new Locus(
                        root.execution,
                        "WeightedDistributionNativeSupport",
                        "Error while executing native weighted distribution"));
            try {
                final ResultSet rs = stmt.getResultSet();
                final Map<LookupKey, Double> values =
                    new LinkedHashMap<LookupKey, Double>();
                while (rs.next()) {
                    ++stmt.rowCount;
                    final List<Object> keyValues =
                        new ArrayList<Object>(plan.varyingHierarchies.size());
                    for (int i = 0; i < plan.varyingHierarchies.size(); i++) {
                        keyValues.add(rs.getObject(i + 1));
                    }
                    final Object valueObject =
                        rs.getObject(plan.varyingHierarchies.size() + 1);
                    final Double value = valueObject == null
                        ? null
                        : ((Number) valueObject).doubleValue();
                    values.put(new LookupKey(keyValues), value);
                }
                final List<RolapHierarchy> hierarchies =
                    new ArrayList<RolapHierarchy>(plan.varyingHierarchies.size());
                for (HierarchyBinding binding : plan.varyingHierarchies) {
                    hierarchies.add(binding.hierarchy);
                }
                return new QueryCache(hierarchies, values);
            } catch (SQLException e) {
                throw stmt.handle(e);
            } finally {
                stmt.close();
            }
        }
    }

    private static int approximateStoreCount(
        RolapCube cube,
        mondrian.olap.SchemaReader schemaReader)
    {
        for (RolapHierarchy hierarchy : cube.getHierarchies()) {
            if (!STORE_ADDRESS_HIERARCHY_NAME.equals(hierarchy.getName())) {
                continue;
            }
            final mondrian.olap.Level[] levels = hierarchy.getLevels();
            if (levels.length == 0) {
                break;
            }
            final mondrian.olap.Level leafLevel = levels[levels.length - 1];
            final int exact =
                schemaReader.getLevelCardinality(leafLevel, true, false);
            if (exact > 0 && exact != Integer.MIN_VALUE) {
                return exact;
            }
            final int approx = leafLevel.getApproxRowCount();
            if (approx > 0 && approx != Integer.MIN_VALUE) {
                return approx;
            }
        }
        return 1000;
    }

    private static final class QueryCache {
        private final List<RolapHierarchy> varyingHierarchies;
        private final Map<LookupKey, Double> values;

        private QueryCache(
            List<RolapHierarchy> varyingHierarchies,
            Map<LookupKey, Double> values)
        {
            this.varyingHierarchies = varyingHierarchies;
            this.values = values;
        }

        private LookupKey lookupKey(RolapEvaluator evaluator) {
            final List<Object> keyValues =
                new ArrayList<Object>(varyingHierarchies.size());
            for (RolapHierarchy hierarchy : varyingHierarchies) {
                final RolapMember member = evaluator.getContext(hierarchy);
                keyValues.add(member == null || member.isAll()
                    ? null
                    : member.getKey());
            }
            return new LookupKey(keyValues);
        }
    }

    private static final class LookupKey {
        private final List<Object> values;

        private LookupKey(List<Object> values) {
            this.values = Collections.unmodifiableList(
                new ArrayList<Object>(values));
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof LookupKey
                && values.equals(((LookupKey) obj).values);
        }

        @Override
        public int hashCode() {
            return values.hashCode();
        }
    }

    private static final class QueryPlan {
        private final RolapCube cube;
        private final RolapStoredMeasure salesMeasure;
        private final String factAlias;
        private final String factTableName;
        private final List<DimensionJoin> dimensionJoins;
        private final List<HierarchyBinding> varyingHierarchies;
        private final List<HierarchyBinding> totalsHierarchies;
        private final List<HierarchyBinding> denominatorHierarchies;
        private final Map<RolapHierarchy, HierarchyState> hierarchyStates;
        private final List<String> qualifyFilters;
        private final List<String> totalsFilters;
        private final List<String> denominatorFilters;

        private QueryPlan(
            RolapCube cube,
            RolapStoredMeasure salesMeasure,
            String factAlias,
            String factTableName,
            List<DimensionJoin> dimensionJoins,
            List<HierarchyBinding> varyingHierarchies,
            List<HierarchyBinding> totalsHierarchies,
            List<HierarchyBinding> denominatorHierarchies,
            Map<RolapHierarchy, HierarchyState> hierarchyStates,
            List<String> qualifyFilters,
            List<String> totalsFilters,
            List<String> denominatorFilters)
        {
            this.cube = cube;
            this.salesMeasure = salesMeasure;
            this.factAlias = factAlias;
            this.factTableName = factTableName;
            this.dimensionJoins = dimensionJoins;
            this.varyingHierarchies = varyingHierarchies;
            this.totalsHierarchies = totalsHierarchies;
            this.denominatorHierarchies = denominatorHierarchies;
            this.hierarchyStates = hierarchyStates;
            this.qualifyFilters = qualifyFilters;
            this.totalsFilters = totalsFilters;
            this.denominatorFilters = denominatorFilters;
        }

        private static QueryPlan tryCreate(
            RolapCube cube,
            RolapStoredMeasure salesMeasure,
            RolapEvaluator evaluator,
            Map<RolapHierarchy, HierarchyState> states)
        {
            final RolapStar.Table factTable = cube.getStar().getFactTable();
            if (factTable == null || factTable.getTableName() == null) {
                logPlan("missing fact table");
                return null;
            }
            final String factAlias = factTable.getAlias();
            final String factTableName = factTable.getTableName();
            if (factAlias == null || factTableName == null) {
                logPlan("missing fact alias");
                return null;
            }
            final JoinRegistry joinRegistry =
                new JoinRegistry(cube, factTable);

            final List<HierarchyBinding> varyingHierarchies =
                collectBindings(
                    cube, factTable, states, false, false, joinRegistry);
            if (varyingHierarchies == null) {
                logPlan("varying bindings unresolved");
                return null;
            }
            final List<HierarchyBinding> totalsHierarchies =
                collectBindings(
                    cube, factTable, states, true, false, joinRegistry);
            if (totalsHierarchies == null) {
                logPlan("totals bindings unresolved");
                return null;
            }
            final List<HierarchyBinding> denominatorHierarchies =
                collectBindings(
                    cube, factTable, states, true, true, joinRegistry);
            if (denominatorHierarchies == null) {
                logPlan("denominator bindings unresolved");
                return null;
            }

            final List<String> qualifyFilters =
                collectFilters(
                    cube, factTable, evaluator, states, false, false,
                    joinRegistry);
            final List<String> totalsFilters =
                collectFilters(
                    cube, factTable, evaluator, states, true, false,
                    joinRegistry);
            final List<String> denominatorFilters =
                collectFilters(
                    cube, factTable, evaluator, states, true, true,
                    joinRegistry);
            if (qualifyFilters == null
                || totalsFilters == null
                || denominatorFilters == null)
            {
                logPlan("filters unresolved");
                return null;
            }
            return new QueryPlan(
                cube,
                salesMeasure,
                factAlias,
                factTableName,
                joinRegistry.snapshot(),
                varyingHierarchies,
                totalsHierarchies,
                denominatorHierarchies,
                states,
                qualifyFilters,
                totalsFilters,
                denominatorFilters);
        }

        private static QueryPlan tryCreateBatchPlan(
            RolapCube cube,
            RolapStoredMeasure salesMeasure,
            RolapResult result,
            RolapEvaluator evaluator)
        {
            final Map<RolapHierarchy, HierarchyState> states =
                collectAxisStates(result);
            return states == null
                ? null
                : tryCreate(cube, salesMeasure, evaluator, states);
        }

        private static QueryPlan tryCreateCurrentContextPlan(
            RolapCube cube,
            RolapStoredMeasure salesMeasure,
            RolapEvaluator evaluator)
        {
            return tryCreate(
                cube,
                salesMeasure,
                evaluator,
                Collections.<RolapHierarchy, HierarchyState>emptyMap());
        }

        private static boolean hasCompleteAxes(RolapResult result) {
            if (result == null) {
                return false;
            }
            for (Axis axis : result.getAxes()) {
                if (axis == null) {
                    return false;
                }
            }
            return true;
        }

        private static Map<RolapHierarchy, HierarchyState> collectAxisStates(
            RolapResult result)
        {
            if (!hasCompleteAxes(result)) {
                return null;
            }
            final Map<RolapHierarchy, HierarchyState> states =
                new LinkedHashMap<RolapHierarchy, HierarchyState>();
            for (Axis axis : result.getAxes()) {
                for (Position position : axis.getPositions()) {
                    for (Member axisMember : position) {
                        if (!(axisMember instanceof RolapMember)) {
                            return null;
                        }
                        final RolapMember member =
                            RolapUtil.strip((RolapMember) axisMember);
                        if (member.isMeasure() || member.isNull()) {
                            continue;
                        }
                        final RolapHierarchy hierarchy = member.getHierarchy();
                        HierarchyState state = states.get(hierarchy);
                        if (state == null) {
                            state = new HierarchyState(hierarchy);
                            states.put(hierarchy, state);
                        }
                        if (!state.observe(member)) {
                            return null;
                        }
                    }
                }
            }
            return states;
        }

        private static List<HierarchyBinding> collectBindings(
            RolapCube cube,
            RolapStar.Table factTable,
            Map<RolapHierarchy, HierarchyState> states,
            boolean resetProductDimension,
            boolean resetStoreHierarchy,
            JoinRegistry joinRegistry)
        {
            final List<HierarchyBinding> bindings =
                new ArrayList<HierarchyBinding>();
            int index = 0;
            for (HierarchyState state : states.values()) {
                if (!state.isVarying()) {
                    continue;
                }
                if (resetProductDimension
                    && PRODUCT_DIMENSION_NAME.equals(
                        state.hierarchy.getDimension().getName()))
                {
                    continue;
                }
                if (resetStoreHierarchy
                    && STORE_RESET_HIERARCHY_NAME.equals(state.hierarchy.getName()))
                {
                    continue;
                }
                final ColumnBinding columnBinding =
                    resolveColumnBinding(
                        factTable,
                        state.nonAllLevel,
                        cube,
                        joinRegistry);
                if (columnBinding == null) {
                    return null;
                }
                bindings.add(
                    new HierarchyBinding(
                        state.hierarchy,
                        columnBinding.sourceAlias,
                        columnBinding.column,
                        "wd_g" + index++));
            }
            return bindings;
        }

        private static List<String> collectFilters(
            RolapCube cube,
            RolapStar.Table factTable,
            RolapEvaluator evaluator,
            Map<RolapHierarchy, HierarchyState> states,
            boolean resetProductDimension,
            boolean resetStoreHierarchy,
            JoinRegistry joinRegistry)
        {
            final List<String> filters = new ArrayList<String>();
            final Set<RolapHierarchy> handledHierarchies =
                new LinkedHashSet<RolapHierarchy>();
            for (HierarchyState axisState : states.values()) {
                final RolapHierarchy hierarchy = axisState.hierarchy;
                if (hierarchy.getDimension().isMeasures()) {
                    continue;
                }
                if (resetProductDimension
                    && PRODUCT_DIMENSION_NAME.equals(
                        hierarchy.getDimension().getName()))
                {
                    continue;
                }
                if (resetStoreHierarchy
                    && STORE_RESET_HIERARCHY_NAME.equals(hierarchy.getName()))
                {
                    continue;
                }
                if (!axisState.isVarying()) {
                    continue;
                }
                handledHierarchies.add(hierarchy);
                if (axisState.hasAll || axisState.keys.isEmpty()) {
                    continue;
                }
                final ColumnBinding columnBinding =
                    resolveColumnBinding(
                        factTable,
                        axisState.nonAllLevel,
                        cube,
                        joinRegistry);
                if (columnBinding == null) {
                    return null;
                }
                filters.add(
                    renderInPredicate(
                        evaluator.getDialect(),
                        columnBinding,
                        axisState.keys));
            }
            for (Member memberObj : evaluator.getNonAllMembers()) {
                if (!(memberObj instanceof RolapMember)) {
                    continue;
                }
                final RolapMember current = RolapUtil.strip((RolapMember) memberObj);
                if (current == null
                    || current.isAll()
                    || current.isCalculated()
                    || current.isMeasure()
                    || current.isNull())
                {
                    continue;
                }
                final RolapHierarchy hierarchy = current.getHierarchy();
                if (handledHierarchies.contains(hierarchy)) {
                    continue;
                }
                if (hierarchy.getDimension().isMeasures()) {
                    continue;
                }
                if (resetProductDimension
                    && PRODUCT_DIMENSION_NAME.equals(
                        hierarchy.getDimension().getName()))
                {
                    continue;
                }
                if (resetStoreHierarchy
                    && STORE_RESET_HIERARCHY_NAME.equals(hierarchy.getName()))
                {
                    continue;
                }
                final ColumnBinding columnBinding =
                    resolveColumnBinding(
                        factTable,
                        current.getLevel(),
                        cube,
                        joinRegistry);
                if (columnBinding == null) {
                    return null;
                }
                filters.add(
                    renderEqualityPredicate(
                        evaluator.getDialect(),
                        columnBinding,
                        current.getKey()));
            }
            return filters;
        }

        private static ColumnBinding resolveColumnBinding(
            RolapStar.Table factTable,
            RolapLevel level,
            RolapCube cube,
            JoinRegistry joinRegistry)
        {
            final RolapLevel bindingLevel =
                WeightedDistributionNativeSupport.resolveBindingLevel(cube, level);
            if (bindingLevel == null) {
                logPlan("column binding unresolved: level=null");
                return null;
            }
            final MondrianDef.Expression expr = bindingLevel.getKeyExp();
            if (!(expr instanceof MondrianDef.Column)) {
                logPlan(
                    "column binding unresolved: non-column expr hierarchy="
                    + bindingLevel.getHierarchy().getUniqueName()
                    + " level=" + bindingLevel.getUniqueName()
                    + " expr=" + expr);
                return null;
            }
            final String columnName = ((MondrianDef.Column) expr).name;
            if (factTable.lookupColumn(columnName) != null) {
                return new ColumnBinding("f", columnName);
            }
            final RolapHierarchy hierarchy = bindingLevel.getHierarchy();
            if (!(hierarchy.getDimension() instanceof RolapCubeDimension)) {
                logPlan(
                    "column binding unresolved: non-cube dimension hierarchy="
                    + hierarchy.getUniqueName()
                    + " dimClass="
                    + hierarchy.getDimension().getClass().getName());
                return null;
            }
            final RolapCubeDimension dimension =
                (RolapCubeDimension) hierarchy.getDimension();
            if (dimension.xmlDimension == null) {
                logPlan(
                    "column binding unresolved: xmlDimension=null hierarchy="
                    + hierarchy.getUniqueName()
                    + " dim=" + dimension.getName());
                return null;
            }
            final String foreignKey = dimension.xmlDimension.foreignKey;
            final MondrianDef.RelationOrJoin relation = hierarchy.getRelation();
            final MondrianDef.Hierarchy xmlHierarchy = hierarchy.getXmlHierarchy();
            if (foreignKey == null
                || factTable.lookupColumn(foreignKey) == null
                || !(relation instanceof MondrianDef.Table)
                || xmlHierarchy == null
                || xmlHierarchy.primaryKey == null)
            {
                logPlan(
                    "column binding unresolved: hierarchy="
                    + hierarchy.getUniqueName()
                    + " level=" + bindingLevel.getUniqueName()
                    + " column=" + columnName
                    + " foreignKey=" + foreignKey
                    + " factHasFk=" + (foreignKey != null
                        && factTable.lookupColumn(foreignKey) != null)
                    + " relationClass="
                    + (relation == null ? "<null>" : relation.getClass().getName())
                    + " xmlHierarchy=" + (xmlHierarchy == null ? "<null>" : xmlHierarchy.name)
                    + " primaryKey="
                    + (xmlHierarchy == null ? "<null>" : xmlHierarchy.primaryKey)
                    + " cube=" + cube.getName());
                return null;
            }
            final MondrianDef.Table table = (MondrianDef.Table) relation;
            final String joinAlias =
                joinRegistry.ensureJoin(
                    table.name,
                    foreignKey,
                    xmlHierarchy.primaryKey);
            return new ColumnBinding(joinAlias, columnName);
        }

        private static String renderEqualityPredicate(
            Dialect dialect,
            ColumnBinding binding,
            Object value)
        {
            return renderColumnRef(dialect, binding.sourceAlias, binding.column)
                + " = "
                + renderLiteral(dialect, value);
        }

        private static String renderInPredicate(
            Dialect dialect,
            ColumnBinding binding,
            Set<Object> keys)
        {
            final StringBuilder builder = new StringBuilder();
            builder.append(
                renderColumnRef(dialect, binding.sourceAlias, binding.column));
            builder.append(" IN (");
            int i = 0;
            for (Object key : keys) {
                if (i++ > 0) {
                    builder.append(", ");
                }
                builder.append(renderLiteral(dialect, key));
            }
            builder.append(')');
            return builder.toString();
        }

        private static String renderLiteral(Dialect dialect, Object value) {
            if (value == null || value == RolapUtil.sqlNullValue) {
                return "NULL";
            }
            if (value instanceof Number) {
                return value.toString();
            }
            final StringBuilder builder = new StringBuilder();
            dialect.quoteStringLiteral(builder, String.valueOf(value));
            return builder.toString();
        }

        private String toSql(Dialect dialect) {
            final String salesColumn =
                resolveSalesColumn(dialect);
            final String storeKeyColumn =
                renderColumnRef(dialect, "f", "store_key");
            final StringBuilder sql = new StringBuilder(2048);
            sql.append("WITH eligible AS (");
            appendEligibleSubquery(sql, dialect, storeKeyColumn);
            sql.append("), store_totals AS (");
            appendTotalsSubquery(sql, dialect, salesColumn, storeKeyColumn);
            sql.append("), denom AS (");
            appendDenominatorSubquery(sql, dialect, salesColumn);
            sql.append(") SELECT ");
            appendOuterSelect(sql, dialect);
            sql.append(" FROM eligible e");
            sql.append(" INNER JOIN store_totals t ON ");
            sql.append(buildJoinCondition(
                dialect,
                Collections.singletonList("wd_store_key"),
                varyingHierarchies,
                totalsHierarchies,
                "e",
                "t"));
            sql.append(" INNER JOIN denom d ON ");
            sql.append(buildJoinCondition(
                dialect,
                Collections.<String>emptyList(),
                varyingHierarchies,
                denominatorHierarchies,
                "e",
                "d"));
            appendOuterGroupBy(sql, dialect);
            sql.append(" SETTINGS group_by_use_nulls = 1");
            return sql.toString();
        }

        private String resolveSalesColumn(Dialect dialect) {
            final MondrianDef.Expression expr =
                salesMeasure.getMondrianDefExpression();
            if (!(expr instanceof MondrianDef.Column)) {
                throw Util.newInternal(
                    "Weighted distribution sales measure must be column-backed");
            }
            return renderColumnRef(
                dialect,
                "f",
                ((MondrianDef.Column) expr).name);
        }

        private void appendEligibleSubquery(
            StringBuilder sql,
            Dialect dialect,
            String storeKeyColumn)
        {
            sql.append("SELECT ");
            appendSelectBindings(sql, dialect, varyingHierarchies, "f");
            if (!varyingHierarchies.isEmpty()) {
                sql.append(", ");
            }
            sql.append(storeKeyColumn).append(" AS wd_store_key");
            sql.append(" FROM ");
            appendFrom(sql, dialect);
            appendWhere(sql, qualifyFilters);
            appendGroupByCube(sql, dialect, varyingHierarchies, "f", true);
        }

        private void appendTotalsSubquery(
            StringBuilder sql,
            Dialect dialect,
            String salesColumn,
            String storeKeyColumn)
        {
            sql.append("SELECT ");
            appendSelectBindings(sql, dialect, totalsHierarchies, "f");
            if (!totalsHierarchies.isEmpty()) {
                sql.append(", ");
            }
            sql.append(storeKeyColumn).append(" AS wd_store_key, ");
            sql.append("sum(").append(salesColumn).append(") AS wd_store_total");
            sql.append(" FROM ");
            appendFrom(sql, dialect);
            appendWhere(sql, totalsFilters);
            appendGroupByCube(sql, dialect, totalsHierarchies, "f", true);
        }

        private void appendDenominatorSubquery(
            StringBuilder sql,
            Dialect dialect,
            String salesColumn)
        {
            sql.append("SELECT ");
            appendSelectBindings(sql, dialect, denominatorHierarchies, "f");
            if (!denominatorHierarchies.isEmpty()) {
                sql.append(", ");
            }
            sql.append("sum(").append(salesColumn).append(") AS wd_denominator");
            sql.append(" FROM ");
            appendFrom(sql, dialect);
            appendWhere(sql, denominatorFilters);
            appendGroupByCube(sql, dialect, denominatorHierarchies, "f", false);
        }

        private void appendOuterSelect(
            StringBuilder sql,
            Dialect dialect)
        {
            for (int i = 0; i < varyingHierarchies.size(); i++) {
                if (i > 0) {
                    sql.append(", ");
                }
                sql.append(renderColumnRef(
                    dialect,
                    "e",
                    varyingHierarchies.get(i).alias));
            }
            if (!varyingHierarchies.isEmpty()) {
                sql.append(", ");
            }
            sql.append("if(d.wd_denominator = 0, NULL, ");
            sql.append("sum(t.wd_store_total) / d.wd_denominator * 100");
            sql.append(") AS wd_value");
        }

        private void appendOuterGroupBy(
            StringBuilder sql,
            Dialect dialect)
        {
            sql.append(" GROUP BY ");
            int count = 0;
            for (HierarchyBinding binding : varyingHierarchies) {
                if (count++ > 0) {
                    sql.append(", ");
                }
                sql.append(renderColumnRef(dialect, "e", binding.alias));
            }
            if (count > 0) {
                sql.append(", ");
            }
            sql.append(renderColumnRef(dialect, "d", "wd_denominator"));
        }

        private void appendFrom(StringBuilder sql, Dialect dialect) {
            sql.append(dialect.quoteIdentifier(factTableName));
            sql.append(' ');
            sql.append(dialect.quoteIdentifier("f"));
            for (DimensionJoin join : dimensionJoins) {
                sql.append(" INNER JOIN ");
                sql.append(dialect.quoteIdentifier(join.tableName));
                sql.append(' ');
                sql.append(dialect.quoteIdentifier(join.alias));
                sql.append(" ON ");
                sql.append(
                    renderColumnRef(dialect, "f", join.factForeignKey));
                sql.append(" = ");
                sql.append(
                    renderColumnRef(dialect, join.alias, join.dimensionPrimaryKey));
            }
        }

        private void appendWhere(
            StringBuilder sql,
            List<String> filters)
        {
            if (filters.isEmpty()) {
                return;
            }
            sql.append(" WHERE ");
            for (int i = 0; i < filters.size(); i++) {
                if (i > 0) {
                    sql.append(" AND ");
                }
                sql.append(filters.get(i));
            }
        }

        private void appendSelectBindings(
            StringBuilder sql,
            Dialect dialect,
            List<HierarchyBinding> bindings,
            String sourceAlias)
        {
            for (int i = 0; i < bindings.size(); i++) {
                if (i > 0) {
                    sql.append(", ");
                }
                final HierarchyBinding binding = bindings.get(i);
                sql.append(
                    renderColumnRef(
                        dialect,
                        binding.sourceAliasFor(sourceAlias),
                        binding.column));
                sql.append(" AS ");
                sql.append(dialect.quoteIdentifier(binding.alias));
            }
        }

        private void appendGroupByCube(
            StringBuilder sql,
            Dialect dialect,
            List<HierarchyBinding> bindings,
            String sourceAlias,
            boolean includeStoreKey)
        {
            if (bindings.isEmpty() && !includeStoreKey) {
                return;
            }
            sql.append(" GROUP BY ");
            for (int i = 0; i < bindings.size(); i++) {
                if (i > 0) {
                    sql.append(", ");
                }
                sql.append(renderColumnRef(
                    dialect,
                    bindings.get(i).sourceAliasFor(sourceAlias),
                    bindings.get(i).column));
            }
            if (includeStoreKey) {
                if (!bindings.isEmpty()) {
                    sql.append(", ");
                }
                sql.append(renderColumnRef(dialect, sourceAlias, "store_key"));
            }
        }

        private static String buildJoinCondition(
            Dialect dialect,
            List<String> baseColumns,
            List<HierarchyBinding> leftBindings,
            List<HierarchyBinding> rightBindings,
            String leftAlias,
            String rightAlias)
        {
            final List<String> predicates = new ArrayList<String>();
            for (String baseColumn : baseColumns) {
                predicates.add(
                    renderColumnRef(dialect, leftAlias, baseColumn)
                    + " = "
                    + renderColumnRef(dialect, rightAlias, baseColumn));
            }
            for (HierarchyBinding left : leftBindings) {
                final HierarchyBinding right =
                    findBinding(rightBindings, left.hierarchy);
                if (right == null) {
                    continue;
                }
                final String leftExpr =
                    renderColumnRef(dialect, leftAlias, left.alias);
                final String rightExpr =
                    renderColumnRef(dialect, rightAlias, right.alias);
                predicates.add(
                    "(" + leftExpr + " = " + rightExpr
                    + " OR (" + leftExpr + " IS NULL AND "
                    + rightExpr + " IS NULL))");
            }
            if (predicates.isEmpty()) {
                return "1 = 1";
            }
            final StringBuilder builder = new StringBuilder();
            for (int i = 0; i < predicates.size(); i++) {
                if (i > 0) {
                    builder.append(" AND ");
                }
                builder.append(predicates.get(i));
            }
            return builder.toString();
        }

        private static HierarchyBinding findBinding(
            List<HierarchyBinding> bindings,
            RolapHierarchy hierarchy)
        {
            for (HierarchyBinding binding : bindings) {
                if (binding.hierarchy.equals(hierarchy)) {
                    return binding;
                }
            }
            return null;
        }

        private static String renderColumnRef(
            Dialect dialect,
            String tableAlias,
            String column)
        {
            return dialect.quoteIdentifier(tableAlias, column);
        }
    }

    private static final class HierarchyState {
        private final RolapHierarchy hierarchy;
        private final Set<Object> keys = new LinkedHashSet<Object>();
        private RolapLevel nonAllLevel;
        private boolean hasAll;

        private HierarchyState(RolapHierarchy hierarchy) {
            this.hierarchy = hierarchy;
        }

        private boolean observe(RolapMember member) {
            if (member.isCalculated()) {
                return false;
            }
            if (member.isAll()) {
                hasAll = true;
                return true;
            }
            if (nonAllLevel == null) {
                nonAllLevel = member.getLevel();
            } else if (!nonAllLevel.equals(member.getLevel())) {
                return false;
            }
            keys.add(member.getKey());
            return true;
        }

        private boolean isVarying() {
            return hasAll || keys.size() > 1;
        }
    }

    private static final class HierarchyBinding {
        private final RolapHierarchy hierarchy;
        private final String sourceAlias;
        private final String column;
        private final String alias;

        private HierarchyBinding(
            RolapHierarchy hierarchy,
            String sourceAlias,
            String column,
            String alias)
        {
            this.hierarchy = hierarchy;
            this.sourceAlias = sourceAlias;
            this.column = column;
            this.alias = alias;
        }

        private String sourceAliasFor(String defaultAlias) {
            return sourceAlias == null ? defaultAlias : sourceAlias;
        }
    }

    private static final class ColumnBinding {
        private final String sourceAlias;
        private final String column;

        private ColumnBinding(String sourceAlias, String column) {
            this.sourceAlias = sourceAlias;
            this.column = column;
        }
    }

    private static final class DimensionJoin {
        private final String alias;
        private final String tableName;
        private final String factForeignKey;
        private final String dimensionPrimaryKey;

        private DimensionJoin(
            String alias,
            String tableName,
            String factForeignKey,
            String dimensionPrimaryKey)
        {
            this.alias = alias;
            this.tableName = tableName;
            this.factForeignKey = factForeignKey;
            this.dimensionPrimaryKey = dimensionPrimaryKey;
        }
    }

    private static final class JoinRegistry {
        private final RolapCube cube;
        private final RolapStar.Table factTable;
        private final Map<String, DimensionJoin> joins =
            new LinkedHashMap<String, DimensionJoin>();

        private JoinRegistry(RolapCube cube, RolapStar.Table factTable) {
            this.cube = cube;
            this.factTable = factTable;
        }

        private String ensureJoin(
            String tableName,
            String factForeignKey,
            String dimensionPrimaryKey)
        {
            final String key =
                tableName + "|" + factForeignKey + "|" + dimensionPrimaryKey;
            final DimensionJoin existing = joins.get(key);
            if (existing != null) {
                return existing.alias;
            }
            final String alias = "wdj" + joins.size();
            joins.put(
                key,
                new DimensionJoin(
                    alias,
                    tableName,
                    factForeignKey,
                    dimensionPrimaryKey));
            return alias;
        }

        private List<DimensionJoin> snapshot() {
            return Collections.unmodifiableList(
                new ArrayList<DimensionJoin>(joins.values()));
        }
    }
}
