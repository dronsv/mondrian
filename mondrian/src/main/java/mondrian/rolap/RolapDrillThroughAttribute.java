/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2021-2022 Sergei Semenkov
// All Rights Reserved.
*/

package mondrian.rolap;

import mondrian.olap.Dimension;
import mondrian.olap.Hierarchy;
import mondrian.olap.Level;
import mondrian.olap.OlapElement;

public class RolapDrillThroughAttribute extends RolapDrillThroughColumn {
    private final Dimension dimension;
    private final Hierarchy hierarchy;
    private final Level level;

    public RolapDrillThroughAttribute(
            String name,
            Dimension dimension,
            Hierarchy hierarchy,
            Level level
    ) {
        super(name);
        this.dimension = dimension;
        this.hierarchy = hierarchy;
        this.level = level;
    }

    public Dimension getDimension() { return this.dimension; }

    public Hierarchy getHierarchy() { return this.hierarchy; }

    public Level getLevel() { return this.level; }

    @Override public OlapElement getOlapElement() {
        if(this.level != null) {
            return this.level;
        }
        if(this.hierarchy != null) {
            return this.hierarchy;
        }
        return this.dimension;
    }
}

