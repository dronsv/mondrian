/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.olap.fun;

import junit.framework.TestCase;
import mondrian.olap.Evaluator;
import mondrian.olap.FunDef;
import mondrian.olap.Hierarchy;
import mondrian.olap.Level;
import mondrian.olap.Member;
import mondrian.olap.SchemaReader;
import mondrian.olap.Syntax;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DrilldownLevelFunDefTest extends TestCase {

    public void testDrillIncludeCalcMembersDedupesRepeatedChild() {
        final Scenario scenario = scenario();

        final List<Member> result = scenario.fun.drill(
            1,
            Arrays.asList(scenario.parent, scenario.sibling, scenario.child),
            scenario.evaluator,
            true);

        assertEquals(3, result.size());
        assertEquals(1, countByUniqueName(result, "[H].[Parent]"));
        assertEquals(1, countByUniqueName(result, "[H].[Sibling]"));
        assertEquals(1, countByUniqueName(result, "[H].[Parent].[Child]"));
    }

    public void testDrillWithoutIncludeCalcMembersKeepsLegacyDuplicates() {
        final Scenario scenario = scenario();

        final List<Member> result = scenario.fun.drill(
            1,
            Arrays.asList(scenario.parent, scenario.sibling, scenario.child),
            scenario.evaluator,
            false);

        assertEquals(4, result.size());
        assertEquals(2, countByUniqueName(result, "[H].[Parent].[Child]"));
    }

    private int countByUniqueName(List<Member> members, String uniqueName) {
        int count = 0;
        for (Member member : members) {
            if (uniqueName.equals(member.getUniqueName())) {
                count++;
            }
        }
        return count;
    }

    private Scenario scenario() {
        final Scenario scenario = new Scenario();

        final Hierarchy hierarchy = mock(Hierarchy.class);
        final Level level1 = mock(Level.class);
        final Level level2 = mock(Level.class);
        when(level1.getDepth()).thenReturn(1);
        when(level2.getDepth()).thenReturn(2);

        scenario.parent = mock(Member.class);
        when(scenario.parent.getHierarchy()).thenReturn(hierarchy);
        when(scenario.parent.getLevel()).thenReturn(level1);
        when(scenario.parent.getUniqueName()).thenReturn("[H].[Parent]");

        scenario.sibling = mock(Member.class);
        when(scenario.sibling.getHierarchy()).thenReturn(hierarchy);
        when(scenario.sibling.getLevel()).thenReturn(level1);
        when(scenario.sibling.getUniqueName()).thenReturn("[H].[Sibling]");

        scenario.child = mock(Member.class);
        when(scenario.child.getHierarchy()).thenReturn(hierarchy);
        when(scenario.child.getLevel()).thenReturn(level2);
        when(scenario.child.getUniqueName()).thenReturn("[H].[Parent].[Child]");
        when(scenario.child.getParentMember()).thenReturn(scenario.parent);

        final SchemaReader schemaReader = mock(SchemaReader.class);
        when(schemaReader.getCalculatedMembers(hierarchy)).thenReturn(
            Collections.<Member>emptyList());
        when(schemaReader.getMemberChildren(scenario.parent)).thenReturn(
            Collections.singletonList(scenario.child));
        when(schemaReader.getMemberChildren(scenario.sibling)).thenReturn(
            Collections.<Member>emptyList());

        scenario.evaluator = mock(Evaluator.class);
        when(scenario.evaluator.getSchemaReader()).thenReturn(schemaReader);

        final FunDef dummyFunDef = mock(FunDef.class);
        when(dummyFunDef.getName()).thenReturn("DrilldownLevel");
        when(dummyFunDef.getSignature()).thenReturn("DrilldownLevel(<Set>)");
        when(dummyFunDef.getDescription()).thenReturn("test");
        when(dummyFunDef.getSyntax()).thenReturn(Syntax.Function);
        when(dummyFunDef.getReturnCategory()).thenReturn(0);
        when(dummyFunDef.getParameterCategories()).thenReturn(new int[] {0});

        scenario.fun = new DrilldownLevelFunDef(dummyFunDef);
        return scenario;
    }

    private static class Scenario {
        private DrilldownLevelFunDef fun;
        private Evaluator evaluator;
        private Member parent;
        private Member sibling;
        private Member child;
    }
}
