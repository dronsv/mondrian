package mondrian.olap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import org.junit.jupiter.api.Test;

/**
 * #83 invariant pin — {@code CubeBase.getTimeLevel} returns the FIRST
 * level of the requested type, scanning hierarchies in declaration
 * order. Synthetic flat time hierarchies (which after #83 carry real
 * time level types) are appended AFTER the calendar hierarchy by
 * RolapCubeDimension, so no-arg {@code Ytd()} / {@code getYearLevel()}
 * must keep resolving to the calendar hierarchy's levels.
 *
 * <p>This is a characterization test: it passes against current code
 * and exists to fail if either the scan order of getTimeLevel or the
 * append-order of synthetic flat hierarchies changes.
 */
class CubeBaseTimeLevelOrderTest {

    @Test
    void getTimeLevel_prefersCalendarHierarchyOverFlatTimeHierarchies() {
        Level calYear = timeLevel(LevelType.TimeYears);
        Level calMonth = timeLevel(LevelType.TimeMonths);
        Hierarchy calendar = hierarchyWithLevels(calYear, calMonth);

        // Flat projections of the same levels — appended after the
        // calendar hierarchy, as RolapCubeDimension does.
        Level flatYear = timeLevel(LevelType.TimeYears);
        Level flatMonth = timeLevel(LevelType.TimeMonths);
        Hierarchy flatYearHier = hierarchyWithLevels(
            timeLevel(LevelType.Regular), flatYear);
        Hierarchy flatMonthHier = hierarchyWithLevels(
            timeLevel(LevelType.Regular), flatMonth);

        Dimension timeDim = mock(Dimension.class);
        when(timeDim.getDimensionType())
            .thenReturn(DimensionType.TimeDimension);
        when(timeDim.getHierarchies()).thenReturn(
            new Hierarchy[] { calendar, flatYearHier, flatMonthHier });

        CubeBase cube = mock(
            CubeBase.class,
            withSettings().defaultAnswer(CALLS_REAL_METHODS));
        cube.dimensions = new Dimension[] { timeDim };

        assertSame(
            calYear, cube.getYearLevel(),
            "getYearLevel must resolve to the calendar hierarchy's "
                + "Year level, not a flat projection");
        assertSame(
            calMonth, cube.getMonthLevel(),
            "getMonthLevel must resolve to the calendar hierarchy's "
                + "Month level, not a flat projection");
    }

    private static Level timeLevel(LevelType type) {
        Level level = mock(Level.class);
        when(level.getLevelType()).thenReturn(type);
        return level;
    }

    private static Hierarchy hierarchyWithLevels(Level... levels) {
        Hierarchy hierarchy = mock(Hierarchy.class);
        when(hierarchy.getLevels()).thenReturn(levels);
        return hierarchy;
    }
}
