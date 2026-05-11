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

import mondrian.olap.Axis;
import mondrian.olap.Member;
import mondrian.olap.Position;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NativeQueryEngineCellMeasureTest {

    @Test public void testAxisMeasureWinsOverSlicerMeasure() {
        Member axisMeasure = measure("[Measures].[Axis]");
        Member slicerMeasure = measure("[Measures].[Slicer]");
        Member rowMember = dimensionMember("Row");

        RolapEvaluator evaluator = mock(RolapEvaluator.class);
        when(evaluator.getMembers()).thenReturn(new Member[] {slicerMeasure});

        Member result = NativeQueryEngine.findMeasureForCell(
            new Axis[] {
                axis(position(axisMeasure)),
                axis(position(rowMember))
            },
            new int[] {0, 0},
            evaluator);

        assertSame(axisMeasure, result);
    }

    @Test public void testSlicerMeasureUsedWhenAxesContainNoMeasure() {
        Member slicerMeasure = measure("[Measures].[Sales]");
        Member colMember = dimensionMember("Month");
        Member rowMember = dimensionMember("District");

        RolapEvaluator evaluator = mock(RolapEvaluator.class);
        when(evaluator.getMembers()).thenReturn(new Member[] {slicerMeasure});

        Member result = NativeQueryEngine.findMeasureForCell(
            new Axis[] {
                axis(position(colMember)),
                axis(position(rowMember))
            },
            new int[] {0, 0},
            evaluator);

        assertSame(slicerMeasure, result);
    }

    @Test public void testReturnsNullWhenContextMeasureIsMissing() {
        Member member = dimensionMember("District");

        RolapEvaluator evaluator = mock(RolapEvaluator.class);
        when(evaluator.getMembers()).thenReturn(new Member[0]);

        Member result = NativeQueryEngine.findMeasureForCell(
            new Axis[] {axis(position(member))},
            new int[] {0},
            evaluator);

        assertNull(result);
    }

    @Test public void testReturnsNullWhenContextSlotIsNotMeasure() {
        Member member = dimensionMember("District");

        RolapEvaluator evaluator = mock(RolapEvaluator.class);
        when(evaluator.getMembers()).thenReturn(new Member[] {member});

        Member result = NativeQueryEngine.findMeasureForCell(
            new Axis[] {axis(position(member))},
            new int[] {0},
            evaluator);

        assertNull(result);
    }

    private static Member measure(String uniqueName) {
        Member member = mock(Member.class);
        when(member.isMeasure()).thenReturn(true);
        when(member.getUniqueName()).thenReturn(uniqueName);
        return member;
    }

    private static Member dimensionMember(String name) {
        Member member = mock(Member.class);
        when(member.isMeasure()).thenReturn(false);
        when(member.getName()).thenReturn(name);
        return member;
    }

    private static Axis axis(Position... positions) {
        final List<Position> positionList =
            Collections.unmodifiableList(Arrays.asList(positions));
        return new Axis() {
            @Override public List<Position> getPositions() {
                return positionList;
            }
        };
    }

    private static Position position(Member... members) {
        return new SimplePosition(Arrays.asList(members));
    }

    private static final class SimplePosition
        extends ArrayList<Member>
        implements Position
    {
        SimplePosition(List<Member> members) {
            super(members);
        }
    }
}
