/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import junit.framework.TestCase;
import mondrian.mdx.MemberExpr;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.Member;
import mondrian.olap.Syntax;
import mondrian.olap.type.DecimalType;
import org.mockito.Mockito;

public class SqlConstraintUtilsCarrierMeasureTest extends TestCase {

    public void testResolveContextStoredMeasureReturnsStoredMeasure() {
        final RolapStoredMeasure measure =
            Mockito.mock(RolapStoredMeasure.class);
        final Evaluator evaluator = Mockito.mock(Evaluator.class);
        Mockito.when(evaluator.getMembers())
            .thenReturn(new Member[] {measure});

        assertSame(measure, SqlConstraintUtils.resolveContextStoredMeasure(evaluator));
    }

    public void testResolveContextStoredMeasureReturnsStoredMeasureFromCalculatedMeasure() {
        final RolapCube cube = Mockito.mock(RolapCube.class);
        final RolapStoredMeasure sales =
            Mockito.mock(RolapStoredMeasure.class);
        final RolapStoredMeasure akb =
            Mockito.mock(RolapStoredMeasure.class);
        Mockito.when(sales.getCube()).thenReturn(cube);
        Mockito.when(akb.getCube()).thenReturn(cube);

        final FunDef dummy = Mockito.mock(FunDef.class);
        Mockito.when(dummy.getSyntax()).thenReturn(Syntax.Function);
        Mockito.when(dummy.getName()).thenReturn("dummy");

        final Exp expression =
            new ResolvedFunCall(
                dummy,
                new Exp[] {new MemberExpr(sales), new MemberExpr(akb)},
                new DecimalType(1, 1));

        final RolapCalculatedMember calculatedMember =
            Mockito.mock(RolapCalculatedMember.class);
        Mockito.when(calculatedMember.getExpression()).thenReturn(expression);

        final Evaluator evaluator = Mockito.mock(Evaluator.class);
        Mockito.when(evaluator.getMembers())
            .thenReturn(new Member[] {calculatedMember});

        assertSame(sales, SqlConstraintUtils.resolveContextStoredMeasure(evaluator));
    }

    public void testResolveContextStoredMeasureReturnsNullForMixedCubes() {
        final RolapCube salesCube = Mockito.mock(RolapCube.class);
        final RolapCube wdCube = Mockito.mock(RolapCube.class);
        final RolapStoredMeasure sales =
            Mockito.mock(RolapStoredMeasure.class);
        final RolapStoredMeasure wd =
            Mockito.mock(RolapStoredMeasure.class);
        Mockito.when(sales.getCube()).thenReturn(salesCube);
        Mockito.when(wd.getCube()).thenReturn(wdCube);

        final FunDef dummy = Mockito.mock(FunDef.class);
        Mockito.when(dummy.getSyntax()).thenReturn(Syntax.Function);
        Mockito.when(dummy.getName()).thenReturn("dummy");

        final Exp expression =
            new ResolvedFunCall(
                dummy,
                new Exp[] {new MemberExpr(sales), new MemberExpr(wd)},
                new DecimalType(1, 1));

        final RolapCalculatedMember calculatedMember =
            Mockito.mock(RolapCalculatedMember.class);
        Mockito.when(calculatedMember.getExpression()).thenReturn(expression);

        final Evaluator evaluator = Mockito.mock(Evaluator.class);
        Mockito.when(evaluator.getMembers())
            .thenReturn(new Member[] {calculatedMember});

        assertNull(SqlConstraintUtils.resolveContextStoredMeasure(evaluator));
    }
}
