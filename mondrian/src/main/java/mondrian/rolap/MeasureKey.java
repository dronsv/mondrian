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

import java.util.*;

/**
 * Composite key for a {@link PhysicalValueRequest}: physical measure
 * unique name plus the set of hierarchy unique names that the request
 * resets (pins to All) when computing the measure.
 *
 * <p>Two requests for the same physical measure but different reset
 * signatures must land in different plans and produce different cells —
 * e.g. plain {@code [Measures].[АКБ]} vs the coordinate-pin tuple form
 * of {@code [Measures].[ОКБ]} (which is semantically АКБ pinned across
 * all 8 product hierarchies). Empty reset signature = byte-identical
 * to the legacy "measureId only" key.
 */
public final class MeasureKey {
    private final String measureId;
    private final Set<String> resetSignature;

    private MeasureKey(String measureId, Set<String> resetSignature) {
        this.measureId = Objects.requireNonNull(measureId);
        this.resetSignature = resetSignature.isEmpty()
            ? Collections.emptySet()
            : Collections.unmodifiableSet(new TreeSet<>(resetSignature));
    }

    public static MeasureKey of(String measureId) {
        return new MeasureKey(measureId, Collections.emptySet());
    }

    public static MeasureKey of(String measureId, Set<String> resetSignature) {
        return new MeasureKey(measureId, resetSignature);
    }

    public String measureId() { return measureId; }
    public Set<String> resetSignature() { return resetSignature; }
    public boolean hasReset() { return !resetSignature.isEmpty(); }

    @Override public boolean equals(Object o) {
        if (!(o instanceof MeasureKey k)) return false;
        return measureId.equals(k.measureId)
            && resetSignature.equals(k.resetSignature);
    }

    @Override public int hashCode() {
        return measureId.hashCode() * 31 + resetSignature.hashCode();
    }

    @Override public String toString() {
        return resetSignature.isEmpty()
            ? measureId
            : measureId + "#reset=" + resetSignature;
    }
}
