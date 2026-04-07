/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.Member;

/**
 * Runtime execution classification for measure members.
 */
public enum MeasureExecutionKind {
    STORED(false),
    CALCULATED_MDX(true),
    CALCULATED_NATIVE_SQL(false);

    private final boolean expandsFormulaDependencies;

    MeasureExecutionKind(boolean expandsFormulaDependencies) {
        this.expandsFormulaDependencies = expandsFormulaDependencies;
    }

    public boolean expandsFormulaDependencies() {
        return expandsFormulaDependencies;
    }

    public static MeasureExecutionKind forMember(Member member) {
        if (member == null || !member.isMeasure() || !member.isCalculated()) {
            return STORED;
        }
        if (!(member instanceof RolapMember)) {
            return CALCULATED_MDX;
        }
        return NativeSqlConfig.findNativeSqlMember((RolapMember) member) != null
            ? CALCULATED_NATIVE_SQL
            : CALCULATED_MDX;
    }
}
