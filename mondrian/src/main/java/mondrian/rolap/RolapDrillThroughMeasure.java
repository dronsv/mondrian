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

import mondrian.olap.Member;
import mondrian.olap.OlapElement;

public class RolapDrillThroughMeasure extends RolapDrillThroughColumn {
    private final Member measure;

    public RolapDrillThroughMeasure(
            String name,
            Member measure
    ) {
        super(name);
        this.measure = measure;
    }

    public Member getMeasure() { return this.measure; }

    @Override public OlapElement getOlapElement() {
        return this.measure;
    }
}

