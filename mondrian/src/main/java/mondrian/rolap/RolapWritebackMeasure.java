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

import mondrian.olap.Member;

public class RolapWritebackMeasure  extends RolapWritebackColumn{
    private final Member measure;
    private final String columnName;

    public RolapWritebackMeasure(
            Member measure,
            String columnName
    ) {
        this.measure = measure;
        this.columnName = columnName;
    }

    public Member getMeasure() { return this.measure; }

    public String getColumnName() { return this.columnName; }
}
