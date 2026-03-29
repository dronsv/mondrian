/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap4j;

import junit.framework.TestCase;
import mondrian.olap.Axis;
import mondrian.olap.QueryAxis;

import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MondrianOlap4jCellSetAxisTest extends TestCase {

    public void testAcceptsGenericAxisImplementation() {
        final MondrianOlap4jCellSet cellSet = mock(MondrianOlap4jCellSet.class);
        final QueryAxis queryAxis = mock(QueryAxis.class);
        final mondrian.olap.Position position0 = mock(mondrian.olap.Position.class);
        final mondrian.olap.Position position1 = mock(mondrian.olap.Position.class);
        final Axis axis = mock(Axis.class);

        when(position0.size()).thenReturn(1);
        when(position1.size()).thenReturn(1);
        when(axis.getPositions()).thenReturn(Arrays.asList(position0, position1));

        final MondrianOlap4jCellSetAxis wrapped =
            new MondrianOlap4jCellSetAxis(cellSet, queryAxis, axis);

        assertEquals(2, wrapped.getPositionCount());
        assertEquals(2, wrapped.getPositions().size());
        assertEquals(1, wrapped.getPositions().get(0).getMembers().size());
        assertEquals(0, wrapped.getPositions().get(0).getOrdinal());
        assertEquals(1, wrapped.getPositions().get(1).getOrdinal());
    }
}
