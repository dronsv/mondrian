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

import mondrian.olap.Dimension;

public class RolapWritebackAttribute extends RolapWritebackColumn{
    private final Dimension dimension;
    private final String columnName;

    public RolapWritebackAttribute(
            Dimension dimension,
            String columnName
    ) {
        this.dimension = dimension;
        this.columnName = columnName;
    }

    public Dimension getDimension() { return this.dimension; }

    public String getColumnName() { return this.columnName; }
}
