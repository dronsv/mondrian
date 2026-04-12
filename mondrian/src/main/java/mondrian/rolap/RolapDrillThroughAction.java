/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2021 Sergei Semenkov
// All Rights Reserved.
*/

package mondrian.rolap;

import java.util.ArrayList;
import java.util.List;
import mondrian.olap.OlapElement;

public class RolapDrillThroughAction extends RolapAction {
    private final boolean isDefault;
    private List<RolapDrillThroughColumn> columnList;

    public RolapDrillThroughAction (
            String name,
            String caption,
            String description,
            boolean isDefault,
            List<RolapDrillThroughColumn> columnList
    ) {
        super(
                name,
                caption,
                description);
        this.isDefault = isDefault;
        this.columnList = columnList;
        if(this.columnList == null) {
            this.columnList = new ArrayList<>();
        }
    }

    public boolean getIsDefault() { return this.isDefault; }

    public List<RolapDrillThroughColumn> getColumns() {
        return this.columnList;
    }

    public List<OlapElement> getOlapElements() {
        List<OlapElement> olapElementList = new ArrayList<>();
        for(RolapDrillThroughColumn rolapDrillThroughColumn: this.columnList) {
            olapElementList.add(rolapDrillThroughColumn.getOlapElement());
        }
        return olapElementList;
    }

}

