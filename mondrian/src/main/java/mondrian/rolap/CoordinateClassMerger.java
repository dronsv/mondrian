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
 * Phase C: groups compatible PhysicalValueRequests into
 * CoordinateClassPlans. Each plan becomes one SQL query.
 */
public class CoordinateClassMerger {

    /**
     * Groups requests into coordinate class plans.
     * Each plan contains requests that can be answered by a single SQL query.
     *
     * @param requests all PhysicalValueRequests from DependencyResolver
     * @return list of CoordinateClassPlans, never null
     */
    public static List<CoordinateClassPlan> merge(
        List<PhysicalValueRequest> requests)
    {
        if (requests == null || requests.isEmpty()) {
            return Collections.emptyList();
        }

        // Group compatible requests.
        // Use first-fit: for each request, find the first existing plan
        // that is compatible, or start a new plan.
        List<List<PhysicalValueRequest>> groups =
            new ArrayList<List<PhysicalValueRequest>>();

        for (PhysicalValueRequest req : requests) {
            boolean merged = false;
            for (List<PhysicalValueRequest> group : groups) {
                // Check compatibility with first request in group
                // (all requests in a group are mutually compatible)
                if (group.get(0).isCompatibleWith(req)) {
                    group.add(req);
                    merged = true;
                    break;
                }
            }
            if (!merged) {
                List<PhysicalValueRequest> newGroup =
                    new ArrayList<PhysicalValueRequest>();
                newGroup.add(req);
                groups.add(newGroup);
            }
        }

        // Convert groups to CoordinateClassPlans
        List<CoordinateClassPlan> plans =
            new ArrayList<CoordinateClassPlan>(groups.size());
        for (int i = 0; i < groups.size(); i++) {
            String classId = generateClassId(groups.get(i));
            plans.add(new CoordinateClassPlan(classId, groups.get(i)));
        }
        return plans;
    }

    /**
     * Generates a human-readable class ID from the request group.
     * Uses reset hierarchy names and source cube name to distinguish
     * plans.  When a plan targets a cross-cube measure (sourceCubeName
     * is non-null), the cube name is appended so plans for different
     * fact tables get distinct IDs.
     */
    private static String generateClassId(
        List<PhysicalValueRequest> group)
    {
        PhysicalValueRequest first = group.get(0);
        StringBuilder sb = new StringBuilder();

        // Build from reset hierarchies
        if (first.getResetHierarchies().isEmpty()) {
            sb.append("Identity");
        } else {
            sb.append("Reset");
            for (mondrian.olap.Hierarchy h : first.getResetHierarchies()) {
                sb.append("_").append(h.getName());
            }
        }

        // Append source cube name for cross-cube plans
        if (first.getSourceCubeName() != null) {
            sb.append("@").append(first.getSourceCubeName());
        }

        return sb.toString();
    }
}
