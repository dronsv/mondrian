/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 eMondrian contributors
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.MondrianDef;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A synthetic hierarchy that projects a single level from an existing
 * hierarchy as a standalone flat hierarchy.
 *
 * <p>This is <b>not</b> an XML clone &mdash; it is a lightweight projection
 * wrapper that reuses the source hierarchy's relation and primary key while
 * exposing exactly one level (the level whose {@code flatName} was set in
 * the schema XML).
 *
 * <p>A flat field may appear as a level in multiple real hierarchies
 * (e.g. СКЮ in both Товар and Марка). All source links are registered
 * so that crossjoin pruning can find the common hierarchy between
 * any two flat fields.
 *
 * <p>Created at cube-wrapper time in {@link RolapCubeDimension} for every
 * level that carries a {@code flatName} attribute.
 */
public class SyntheticFlatHierarchy extends RolapHierarchy {

    private static final Logger LOGGER =
        LogManager.getLogger(SyntheticFlatHierarchy.class);

    /**
     * A link to one source hierarchy and the level within it that
     * this flat field projects.
     */
    public record SourceLink(
        RolapHierarchy hierarchy,
        RolapLevel level,
        int depth) {}

    private final List<SourceLink> sourceLinks = new ArrayList<>();

    /**
     * Creates a SyntheticFlatHierarchy.
     *
     * @param cube         the cube being initialised
     * @param dimension    the dimension that owns this hierarchy
     * @param sourceHier   the real hierarchy that contains the source level
     * @param sourceLevel  the level being projected as a flat hierarchy
     * @param flatName     the user-visible name for the synthetic hierarchy
     * @param xmlCubeDim   the XML cube-dimension element (passed to super)
     */
    public SyntheticFlatHierarchy(
        RolapCube cube,
        RolapDimension dimension,
        RolapHierarchy sourceHier,
        RolapLevel sourceLevel,
        String flatName,
        MondrianDef.CubeDimension xmlCubeDim)
    {
        super(
            cube,
            dimension,
            buildSyntheticXml(sourceHier, sourceLevel, flatName),
            xmlCubeDim);

        addSourceLink(sourceHier, sourceLevel);

        LOGGER.debug(
            "Created SyntheticFlatHierarchy [{}] from [{}.{}]",
            flatName, sourceHier.getUniqueName(), sourceLevel.getName());
    }

    // ----- source link management ------------------------------------------

    /**
     * Registers an additional source hierarchy+level for this flat field.
     * Called during deduplication when the same canonical column appears
     * in multiple real hierarchies.
     */
    public void addSourceLink(
        RolapHierarchy hierarchy, RolapLevel level)
    {
        sourceLinks.add(new SourceLink(
            hierarchy, level, level.getDepth()));
    }

    /** All source links — one per real hierarchy containing this column. */
    public List<SourceLink> getSourceLinks() {
        return Collections.unmodifiableList(sourceLinks);
    }

    /** Returns the first (primary) source hierarchy. */
    public RolapHierarchy getSourceHierarchy() {
        return sourceLinks.isEmpty() ? null : sourceLinks.get(0).hierarchy();
    }

    /** Returns the first (primary) source level. */
    public RolapLevel getSourceLevel() {
        return sourceLinks.isEmpty() ? null : sourceLinks.get(0).level();
    }

    /**
     * Finds a source link that shares the given hierarchy.
     * Used by crossjoin pruning to determine ancestor dependency.
     *
     * @return the SourceLink in the given hierarchy, or null
     */
    public SourceLink findLinkForHierarchy(RolapHierarchy hierarchy) {
        for (SourceLink link : sourceLinks) {
            if (link.hierarchy().equals(hierarchy)) {
                return link;
            }
        }
        return null;
    }

    /** Flat fields are always visible &mdash; that is their purpose. */
    @Override
    public boolean isShowHierarchy() {
        return true;
    }

    // ----- synthetic XML builder --------------------------------------------

    /**
     * Builds a minimal {@link MondrianDef.Hierarchy} that the
     * {@link RolapHierarchy} super-constructor can consume.
     *
     * <p>The resulting XML object carries:
     * <ul>
     *   <li>{@code name} = flatName</li>
     *   <li>{@code hasAll} = true</li>
     *   <li>{@code allMemberName} = "All " + flatName</li>
     *   <li>{@code relation}, {@code primaryKey}, {@code primaryKeyTable}
     *       copied from the source hierarchy</li>
     *   <li>a single {@link MondrianDef.Level} that mirrors the source
     *       level's column/table/type/uniqueMembers</li>
     * </ul>
     */
    private static MondrianDef.Hierarchy buildSyntheticXml(
        RolapHierarchy sourceHier,
        RolapLevel sourceLevel,
        String flatName)
    {
        MondrianDef.Hierarchy xmlHier = sourceHier.getXmlHierarchy();

        MondrianDef.Hierarchy synth = new MondrianDef.Hierarchy();
        synth.name = flatName;
        synth.hasAll = true;
        synth.allMemberName = "All " + flatName;
        synth.visible = true;
        synth.showHierarchy = true;

        // Copy relation topology from source hierarchy
        if (xmlHier != null) {
            synth.relation = xmlHier.relation;
            synth.primaryKey = xmlHier.primaryKey;
            synth.primaryKeyTable = xmlHier.primaryKeyTable;
        }

        // Build the single projected level
        synth.levels = new MondrianDef.Level[] {
            buildSyntheticLevel(sourceLevel, flatName)
        };

        // Empty array — no member reader parameters needed
        synth.memberReaderParameters = new MondrianDef.MemberReaderParameter[0];

        return synth;
    }

    /**
     * Builds a minimal {@link MondrianDef.Level} that mirrors the source
     * level's key column, table, datatype, and uniqueMembers flag.
     */
    private static MondrianDef.Level buildSyntheticLevel(
        RolapLevel sourceLevel,
        String flatName)
    {
        MondrianDef.Level lvl = new MondrianDef.Level();
        lvl.name = flatName;
        lvl.visible = true;
        lvl.levelType = "Regular";
        lvl.hideMemberIf = "Never";
        lvl.type = sourceLevel.getDatatype() != null
            ? sourceLevel.getDatatype().name()
            : "String";
        lvl.uniqueMembers = sourceLevel.isUnique();

        // Extract column/table from source level's key expression
        MondrianDef.Expression keyExp = sourceLevel.getKeyExp();
        if (keyExp instanceof MondrianDef.Column col) {
            lvl.column = col.name;
            lvl.table = col.table;
        }

        // No properties, no closures
        lvl.properties = new MondrianDef.Property[0];

        return lvl;
    }
}

// End SyntheticFlatHierarchy.java
