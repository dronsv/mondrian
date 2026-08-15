package mondrian.olap.fun;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import mondrian.olap.Evaluator;
import mondrian.olap.Hierarchy;
import mondrian.olap.Level;
import mondrian.olap.Member;

import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * #83 follow-up — {@code FunUtil.periodsToDate} silently returned an
 * empty set when the member belonged to a different hierarchy than the
 * target level (e.g. {@code Ytd(<flat time member>)} resolving the Year
 * level from the calendar hierarchy). An explicit hierarchy-mismatch
 * guard turns that into a clear evaluation error, mirroring the
 * validation {@code PeriodsToDateFunDef.getResultType} already applies
 * when both arguments are explicit.
 *
 * <p>The same-hierarchy "level lower than member" case must keep
 * returning an empty list — upstream documents that as valid
 * (see comment in periodsToDate).
 */
class PeriodsToDateHierarchyGuardTest {

    @Test
    void periodsToDate_memberFromOtherHierarchy_throwsClearError() {
        Hierarchy calendarHier = mock(Hierarchy.class);
        when(calendarHier.getUniqueName()).thenReturn("[Period]");

        Hierarchy flatHier = mock(Hierarchy.class);
        when(flatHier.getUniqueName()).thenReturn("[Period.Month]");

        Level yearLevel = mock(Level.class);
        when(yearLevel.getHierarchy()).thenReturn(calendarHier);

        Member flatMember = mock(Member.class);
        when(flatMember.getHierarchy()).thenReturn(flatHier);
        when(flatMember.getUniqueName())
            .thenReturn("[Period.Month].[2025-03]");

        Evaluator evaluator = mock(Evaluator.class);

        MondrianEvaluationException e = assertThrows(
            MondrianEvaluationException.class,
            () -> FunUtil.periodsToDate(evaluator, yearLevel, flatMember),
            "Hierarchy mismatch must fail loudly, not return empty");
        assertTrue(
            e.getMessage().contains("[Period]"),
            "Message must name the required hierarchy: " + e.getMessage());
        assertTrue(
            e.getMessage().contains("[Period.Month].[2025-03]"),
            "Message must name the offending member: " + e.getMessage());
    }

    @Test
    void periodsToDate_sameHierarchyLevelBelowMember_returnsEmptyList() {
        Hierarchy hier = mock(Hierarchy.class);

        Level quarterLevel = mock(Level.class);
        when(quarterLevel.getHierarchy()).thenReturn(hier);

        Level yearLevel = mock(Level.class);
        when(yearLevel.getHierarchy()).thenReturn(hier);

        // Member is at Year level; target level (Quarter) is below it —
        // walking up the parent chain never reaches Quarter.
        Member yearMember = mock(Member.class);
        when(yearMember.getHierarchy()).thenReturn(hier);
        when(yearMember.getLevel()).thenReturn(yearLevel);
        when(yearMember.getParentMember()).thenReturn(null);

        Evaluator evaluator = mock(Evaluator.class);

        List<Member> result =
            FunUtil.periodsToDate(evaluator, quarterLevel, yearMember);

        assertTrue(
            result.isEmpty(),
            "Same-hierarchy lower-level target must keep returning "
                + "an empty list (documented upstream behavior)");
    }
}
