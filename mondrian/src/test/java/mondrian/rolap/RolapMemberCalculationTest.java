package mondrian.rolap;

import mondrian.calc.Calc;
import mondrian.olap.Exp;
import mondrian.olap.MondrianProperties;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RolapMemberCalculationTest {
    @Test public void testDelegatesToMemberCompiledExpressionWhenShareResetDisabled() {
        final boolean previous =
            MondrianProperties.instance()
                .CalcShareMeasureAutoResetPeerHierarchies.get();
        MondrianProperties.instance()
            .CalcShareMeasureAutoResetPeerHierarchies.set(false);
        try {
            final RolapCalculatedMember member =
                mock(RolapCalculatedMember.class);
            final RolapHierarchy hierarchy = mock(RolapHierarchy.class);
            final RolapEvaluatorRoot root = mock(RolapEvaluatorRoot.class);
            final Exp exp = mock(Exp.class);
            final Calc compiled = mock(Calc.class);

            when(member.isEvaluated()).thenReturn(true);
            when(member.getSolveOrder()).thenReturn(0);
            when(member.getHierarchy()).thenReturn(hierarchy);
            when(hierarchy.getOrdinalInCube()).thenReturn(0);
            when(member.getExpression()).thenReturn(exp);
            when(member.getCompiledExpression(root)).thenReturn(compiled);

            final RolapMemberCalculation calculation =
                new RolapMemberCalculation(member);

            assertSame(compiled, calculation.getCompiledExpression(root));
            verify(member).getCompiledExpression(root);
        } finally {
            MondrianProperties.instance()
                .CalcShareMeasureAutoResetPeerHierarchies.set(previous);
        }
    }
}
