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
 * An execution shape: a set of compatible PhysicalValueRequests
 * that can be answered by a single SQL query.
 * Output of CoordinateClassMerger (Phase C).
 */
public class CoordinateClassPlan {
    private final String classId;
    private final List<PhysicalValueRequest> requests;

    public CoordinateClassPlan(
        String classId,
        List<PhysicalValueRequest> requests)
    {
        this.classId = classId;
        this.requests = Collections.unmodifiableList(
            new ArrayList<PhysicalValueRequest>(requests));
    }

    public String getClassId() { return classId; }
    public List<PhysicalValueRequest> getRequests() { return requests; }

    public Set<String> getMeasureIds() {
        Set<String> ids = new LinkedHashSet<String>();
        for (PhysicalValueRequest r : requests) {
            ids.add(r.getPhysicalMeasureId());
        }
        return ids;
    }
}
