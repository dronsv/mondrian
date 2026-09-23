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

    /**
     * Whether every request in this plan resets the same hierarchies.
     *
     * <p>{@link CoordinateClassMerger} only groups requests whose reset
     * sets are equal, so plans it builds are always uniform. Plans
     * assembled directly (tests, and the stored-request extraction in
     * {@link NativeQueryEngine}) are not bound by that, and
     * {@link NativeQuerySqlGenerator} then renders each request in its
     * own scope via a correlated scalar subquery. A non-uniform plan
     * therefore has no single subselect restriction, and anything that
     * wants to describe the plan's SQL with one — the NQE prefetch read
     * guard — must decline rather than take the first request's
     * (dronsv/mondrian#49 review).
     *
     * @return true when the plan has one reset set, or no requests
     */
    public boolean hasUniformResetHierarchies() {
        if (requests.isEmpty()) {
            return true;
        }
        Set<mondrian.olap.Hierarchy> first =
            requests.get(0).getResetHierarchies();
        for (PhysicalValueRequest r : requests) {
            if (!first.equals(r.getResetHierarchies())) {
                return false;
            }
        }
        return true;
    }

    public Set<String> getMeasureIds() {
        Set<String> ids = new LinkedHashSet<String>();
        for (PhysicalValueRequest r : requests) {
            ids.add(r.getPhysicalMeasureId());
        }
        return ids;
    }
}
