/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2005-2017 Hitachi Vantara
// Copyright (C) 2021 Sergei Semenkov
// All Rights Reserved.
*/
package mondrian.olap;

import mondrian.calc.Calc;
import mondrian.rolap.RolapSchema;
import mondrian.rolap.RolapUtil;

import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

/**
 * <code>DelegatingSchemaReader</code> implements {@link SchemaReader} by
 * delegating all methods to an underlying {@link SchemaReader}.
 *
 * <p>It is a convenient base class if you want to override just a few of
 * {@link SchemaReader}'s methods.</p>
 *
 * @author jhyde
 * @since Feb 26, 2003
 */
public abstract class DelegatingSchemaReader implements SchemaReader {
    protected final SchemaReader schemaReader;

    /**
     * Creates a DelegatingSchemaReader.
     *
     * @param schemaReader Parent reader to delegate unhandled calls to
     */
    protected DelegatingSchemaReader(SchemaReader schemaReader) {
        this.schemaReader = schemaReader;
    }

    @Override
    public RolapSchema getSchema() {
        return schemaReader.getSchema();
    }

    @Override
    public Role getRole() {
        return schemaReader.getRole();
    }

    @Override
    public Cube getCube() {
        return schemaReader.getCube();
    }

    @Override
    public List<Dimension> getCubeDimensions(Cube cube) {
        return schemaReader.getCubeDimensions(cube);
    }

    @Override
    public List<Hierarchy> getDimensionHierarchies(Dimension dimension) {
        return schemaReader.getDimensionHierarchies(dimension);
    }

    @Override
    public List<Member> getHierarchyRootMembers(Hierarchy hierarchy) {
        return schemaReader.getHierarchyRootMembers(hierarchy);
    }

    @Override
    public Member getMemberParent(Member member) {
        return schemaReader.getMemberParent(member);
    }

    @Override
    public Member substitute(Member member) {
        return schemaReader.substitute(member);
    }

    @Override
    public List<Member> getMemberChildren(Member member) {
        return schemaReader.getMemberChildren(member);
    }

    @Override
    public List<Member> getMemberChildren(List<Member> members) {
        return schemaReader.getMemberChildren(members);
    }

    @Override
    public void getParentChildContributingChildren(
        Member dataMember, Hierarchy hierarchy, List<Member> list)
    {
        schemaReader.getParentChildContributingChildren(
            dataMember, hierarchy, list);
    }

    @Override
    public int getMemberDepth(Member member) {
        return schemaReader.getMemberDepth(member);
    }

    @Override
    public final Member getMemberByUniqueName(
        List<Id.Segment> uniqueNameParts,
        boolean failIfNotFound)
    {
        return getMemberByUniqueName(
            uniqueNameParts, failIfNotFound, MatchType.EXACT);
    }

    @Override
    public Member getMemberByUniqueName(
        List<Id.Segment> uniqueNameParts,
        boolean failIfNotFound,
        MatchType matchType)
    {
        return schemaReader.getMemberByUniqueName(
            uniqueNameParts, failIfNotFound, matchType);
    }

    @Override
    public final OlapElement lookupCompound(
        OlapElement parent, List<Id.Segment> names,
        boolean failIfNotFound, int category)
    {
        return lookupCompound(
            parent, names, failIfNotFound, category, MatchType.EXACT);
    }

    @Override
    public final OlapElement lookupCompound(
        OlapElement parent,
        List<Id.Segment> names,
        boolean failIfNotFound,
        int category,
        MatchType matchType)
    {
        if (MondrianProperties.instance().SsasCompatibleNaming.get()) {
            return new NameResolver().resolve(
                parent,
                Util.toOlap4j(names),
                failIfNotFound,
                category,
                matchType,
                getNamespaces());
        }
        return lookupCompoundInternal(
            parent,
            names,
            failIfNotFound,
            category,
            matchType);
    }

    @Override
    public List<NameResolver.Namespace> getNamespaces() {
        return schemaReader.getNamespaces();
    }

    public OlapElement lookupCompoundInternal(
        OlapElement parent, List<Id.Segment> names,
        boolean failIfNotFound, int category, MatchType matchType)
    {
        return schemaReader.lookupCompound(
            parent, names, failIfNotFound, category, matchType);
    }

    @Override
    public Member getCalculatedMember(List<Id.Segment> nameParts) {
        return schemaReader.getCalculatedMember(nameParts);
    }

    @Override
    public NamedSet getNamedSet(List<Id.Segment> nameParts) {
        return schemaReader.getNamedSet(nameParts);
    }

    @Override
    public void getMemberRange(
        Level level,
        Member startMember,
        Member endMember,
        List<Member> list)
    {
        schemaReader.getMemberRange(level, startMember, endMember, list);
    }

    @Override
    public Member getLeadMember(Member member, int n) {
        return schemaReader.getLeadMember(member, n);
    }

    @Override
    public int compareMembersHierarchically(Member m1, Member m2) {
        return schemaReader.compareMembersHierarchically(m1, m2);
    }

    @Override
    public OlapElement getElementChild(OlapElement parent, Id.Segment name) {
        return getElementChild(parent, name, MatchType.EXACT);
    }

    @Override
    public OlapElement getElementChild(
        OlapElement parent, Id.Segment name, MatchType matchType)
    {
        return schemaReader.getElementChild(parent, name, matchType);
    }

    @Override
    public List<Member> getLevelMembers(
            Level level, boolean includeCalculated, Evaluator context)
    {
        return schemaReader.getLevelMembers(level, includeCalculated, context);
    }

    @Override
    public List<Member> getLevelMembers(
        Level level, boolean includeCalculated)
    {
        return getLevelMembers(level, includeCalculated, null);
    }

    @Override
    public List<Level> getHierarchyLevels(Hierarchy hierarchy) {
        return schemaReader.getHierarchyLevels(hierarchy);
    }

    @Override
    public Member getHierarchyDefaultMember(Hierarchy hierarchy) {
        return schemaReader.getHierarchyDefaultMember(hierarchy);
    }

    @Override
    public boolean isDrillable(Member member) {
        return schemaReader.isDrillable(member);
    }

    @Override
    public boolean isVisible(Member member) {
        return schemaReader.isVisible(member);
    }

    @Override
    public Cube[] getCubes() {
        return schemaReader.getCubes();
    }

    @Override
    public List<Member> getCalculatedMembers(Hierarchy hierarchy) {
        return schemaReader.getCalculatedMembers(hierarchy);
    }

    @Override
    public List<Member> getCalculatedMembers(Level level) {
        return schemaReader.getCalculatedMembers(level);
    }

    @Override
    public List<Member> getCalculatedMembers() {
        return schemaReader.getCalculatedMembers();
    }

    @Override
    public int getChildrenCountFromCache(Member member) {
        return schemaReader.getChildrenCountFromCache(member);
    }

    @Override
    public int getLevelCardinality(
        Level level, boolean approximate, boolean materialize)
    {
        return schemaReader.getLevelCardinality(
            level, approximate, materialize);
    }

    @Override
    public List<Member> getLevelMembers(Level level, Evaluator context) {
        return schemaReader.getLevelMembers(level, context);
    }

    @Override
    public List<Member> getMemberChildren(Member member, Evaluator context) {
        return schemaReader.getMemberChildren(member, context);
    }

    @Override
    public List<Member> getMemberChildren(
        List<Member> members, Evaluator context)
    {
        return schemaReader.getMemberChildren(members, context);
    }

    @Override
    public void getMemberAncestors(Member member, List<Member> ancestorList) {
        schemaReader.getMemberAncestors(member, ancestorList);
    }

    @Override
    public Member lookupMemberChildByName(
        Member member, Id.Segment memberName, MatchType matchType)
    {
        return schemaReader.lookupMemberChildByName(
            member, memberName, matchType);
    }

    @Override
    public List<Member> lookupMemberChildrenByNames(
        Member parent, List<Id.NameSegment> childNames, MatchType matchType)
    {
        return schemaReader.lookupMemberChildrenByNames(
            parent, childNames, matchType);
    }

    @Override
    public NativeEvaluator getNativeSetEvaluator(
        FunDef fun, Exp[] args, Evaluator evaluator, Calc calc)
    {
        return schemaReader.getNativeSetEvaluator(fun, args, evaluator, calc);
    }

    @Override
    public Parameter getParameter(String name) {
        return schemaReader.getParameter(name);
    }

    @Override
    public DataSource getDataSource() {
        return schemaReader.getDataSource();
    }

    @Override
    public SchemaReader withoutAccessControl() {
        return schemaReader.withoutAccessControl();
    }

    @Override
    public SchemaReader withLocus() {
        return RolapUtil.locusSchemaReader(
            schemaReader.getSchema().getInternalConnection(),
            this);
    }

    @Override
    public Map<? extends Member, Access> getMemberChildrenWithDetails(
        Member member,
        Evaluator evaluator)
    {
        return schemaReader.getMemberChildrenWithDetails(member, evaluator);
    }
}

// End DelegatingSchemaReader.java
