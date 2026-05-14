/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2017 Hitachi Vantara and others
// Copyright (C) 2021 Sergei Semenkov
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/**
 * RolapCubeDimension wraps a RolapDimension for a specific Cube.
 *
 * @author Will Gorman, 19 October 2007
 */
public class RolapCubeDimension extends RolapDimension {

    private static final Logger LOGGER =
        LogManager.getLogger(RolapCubeDimension.class);

    RolapCube cube;

    RolapDimension rolapDimension;
    int cubeOrdinal;
    MondrianDef.CubeDimension xmlDimension;

    /**
     * Creates a RolapCubeDimension.
     *
     * @param cube Cube
     * @param rolapDim Dimension wrapped by this dimension
     * @param cubeDim XML element definition
     * @param name Name of dimension
     * @param cubeOrdinal Ordinal of dimension within cube
     * @param hierarchyList List of hierarchies in cube
     * @param highCardinality Whether high cardinality dimension
     */
    public RolapCubeDimension(
        RolapCube cube,
        RolapDimension rolapDim,
        MondrianDef.CubeDimension cubeDim,
        String name,
        int cubeOrdinal,
        List<RolapHierarchy> hierarchyList,
        final boolean highCardinality)
    {
        super(
            null,
            name,
            cubeDim.caption != null
                ? cubeDim.caption
                : rolapDim.getCaption(),
            cubeDim.visible,
            cubeDim.description != null
                ? cubeDim.description
                : rolapDim.getDescription(),
            null,
            highCardinality,
            cubeDim.annotations != null
                ? RolapHierarchy.createAnnotationMap(cubeDim.annotations)
                : rolapDim.getAnnotationMap());
        this.xmlDimension = cubeDim;
        this.rolapDimension = rolapDim;
        this.cubeOrdinal = cubeOrdinal;
        this.cube = cube;
        this.caption = cubeDim.caption;

        // create new hierarchies
        hierarchies = new RolapCubeHierarchy[rolapDim.getHierarchies().length];

        RolapCube factCube = null;
        if (cube.isVirtual()) {
          factCube = lookupFactCube(cubeDim, cube.getSchema());
        }
        for (int i = 0; i < rolapDim.getHierarchies().length; i++) {
          final RolapCubeHierarchy cubeHierarchy =
                new RolapCubeHierarchy(
                    this,
                    cubeDim,
                    (RolapHierarchy) rolapDim.getHierarchies()[i],
                    ((HierarchyBase) rolapDim.getHierarchies()[i]).getSubName(),
                    hierarchyList.size(),
                    factCube);
            hierarchies[i] = cubeHierarchy;
            hierarchyList.add(cubeHierarchy);
        }

        // Create synthetic flat hierarchies for levels with flatName
        // canonical key → synthetic flat hierarchy for source link registration
        java.util.Map<String, SyntheticFlatHierarchy> canonicalToFlat =
            new java.util.LinkedHashMap<>();
        List<RolapCubeHierarchy> flatList = new ArrayList<>();

        for (Hierarchy hier : rolapDim.getHierarchies()) {
            if (!(hier instanceof RolapHierarchy rolapHier)) {
                continue;
            }
            MondrianDef.Hierarchy xmlHier = rolapHier.getXmlHierarchy();
            if (xmlHier == null || xmlHier.levels == null) {
                continue;
            }

            for (MondrianDef.Level xmlLevel : xmlHier.levels) {
                if (xmlLevel.flatName == null
                    || xmlLevel.flatName.isEmpty())
                {
                    continue;
                }
                // Phase 1: only simple column keyExp supported
                if (xmlLevel.column == null
                    || xmlLevel.column.isEmpty())
                {
                    LOGGER.warn(
                        "Skipping flatName='{}' on level '{}' — "
                        + "Phase 1 requires simple column keyExp",
                        xmlLevel.flatName, xmlLevel.name);
                    continue;
                }

                // Find corresponding RolapLevel in the live hierarchy
                RolapLevel sourceLevel = null;
                for (Level lev : rolapHier.getLevels()) {
                    if (!lev.isAll()
                        && lev.getName().equals(xmlLevel.name))
                    {
                        sourceLevel = (RolapLevel) lev;
                        break;
                    }
                }
                if (sourceLevel == null) {
                    continue;
                }

                String table =
                    xmlLevel.table != null ? xmlLevel.table : "";
                String canonical = table + "\0" + xmlLevel.column;
                SyntheticFlatHierarchy existingFlat =
                    canonicalToFlat.get(canonical);
                if (existingFlat != null) {
                    // Deduplicated — register additional source link
                    existingFlat.addSourceLink(rolapHier, sourceLevel);
                    LOGGER.debug(
                        "Added source link [{}.{}] to flat [{}]",
                        rolapHier.getUniqueName(),
                        sourceLevel.getName(),
                        existingFlat.getName());
                    continue;
                }

                SyntheticFlatHierarchy flatHier =
                    new SyntheticFlatHierarchy(
                        cube, (RolapDimension) rolapDim,
                        rolapHier, sourceLevel,
                        xmlLevel,
                        xmlLevel.flatName, cubeDim);

                canonicalToFlat.put(canonical, flatHier);

                RolapCubeHierarchy cubeFlat = new RolapCubeHierarchy(
                    this, cubeDim, flatHier,
                    xmlLevel.flatName,
                    hierarchyList.size(),
                    factCube);
                flatList.add(cubeFlat);
                hierarchyList.add(cubeFlat);
            }
        }

        if (!flatList.isEmpty()) {
            RolapCubeHierarchy[] combined =
                new RolapCubeHierarchy[
                    hierarchies.length + flatList.size()];
            System.arraycopy(
                hierarchies, 0, combined, 0, hierarchies.length);
            for (int i = 0; i < flatList.size(); i++) {
                combined[hierarchies.length + i] = flatList.get(i);
            }
            hierarchies = combined;
        }
    }

    RolapCube lookupFactCube(
        MondrianDef.CubeDimension cubeDim, RolapSchema schema)
    {
      if (cubeDim instanceof MondrianDef.VirtualCubeDimension virtualCubeDim) {
        if (virtualCubeDim.cubeName != null) {
          return schema.lookupCube(virtualCubeDim.cubeName);
        }
      }
      return null;
    }

    public RolapCube getCube() {
        return cube;
    }

    @Override public Schema getSchema() {
        return rolapDimension.getSchema();
    }

    // this method should eventually replace the call below
    public int getOrdinal() {
        return cubeOrdinal;
    }

    @Override public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RolapCubeDimension)) {
            return false;
        }

        RolapCubeDimension that = (RolapCubeDimension)o;
        if (!cube.equals(that.cube)) {
            return false;
        }
        return getUniqueName().equals(that.getUniqueName());
    }

    @Override RolapCubeHierarchy newHierarchy(
        String subName, boolean hasAll, RolapHierarchy closureFor)
    {
        throw new UnsupportedOperationException();
    }

    @Override public String getCaption() {
        if (caption != null) {
            return caption;
        }
        return this.name;
    }

    @Override public void setCaption(String caption) {
        if (true) {
            throw new UnsupportedOperationException();
        }
        rolapDimension.setCaption(caption);
    }

    @Override public DimensionType getDimensionType() {
        return rolapDimension.getDimensionType();
    }

}

// End RolapCubeDimension.java
