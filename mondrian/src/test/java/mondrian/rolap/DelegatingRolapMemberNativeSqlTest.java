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

import junit.framework.TestCase;
import mondrian.calc.Calc;
import mondrian.olap.Annotation;
import mondrian.olap.Exp;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DelegatingRolapMemberNativeSqlTest extends TestCase {

    public void testDelegatesCompiledExpressionForNativeSqlCalculatedMember() {
        final RolapCalculatedMember member = mock(RolapCalculatedMember.class);
        final RolapEvaluatorRoot root = mock(RolapEvaluatorRoot.class);
        final Calc expected = mock(Calc.class);

        when(member.getName()).thenReturn("WD %");
        when(member.getAnnotationMap()).thenReturn(nativeSqlAnnotations());
        when(member.getCompiledExpression(same(root))).thenReturn(expected);

        final DelegatingRolapMember delegating = new DelegatingRolapMember(member);

        assertSame(expected, delegating.getCompiledExpression(root));
        verify(member).getCompiledExpression(same(root));
    }

    private static Map<String, Annotation> nativeSqlAnnotations() {
        final Map<String, Annotation> anns =
            new LinkedHashMap<String, Annotation>();
        anns.put("nativeSql.enabled", ann("true"));
        anns.put("nativeSql.template", ann("SELECT 1 AS val"));
        return anns;
    }

    private static Annotation ann(final String value) {
        return new Annotation() {
            public String getName() {
                return null;
            }

            public Object getValue() {
                return value;
            }
        };
    }
}
