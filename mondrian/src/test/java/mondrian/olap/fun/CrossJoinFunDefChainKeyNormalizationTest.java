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

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

public class CrossJoinFunDefChainKeyNormalizationTest extends TestCase {

  public void testToComparableChainKeyStringNormalizesNumericTypes() {
    assertEquals("337407", CrossJoinFunDef.toComparableChainKeyString(Double.valueOf(337407.0d)));
    assertEquals("42", CrossJoinFunDef.toComparableChainKeyString(Long.valueOf(42L)));
  }

  public void testToComparableChainKeyStringPreservesStringCodes() {
    assertEquals("0012", CrossJoinFunDef.toComparableChainKeyString("0012"));
  }

  public void testToComparableChainKeyStringUnwrapsRolapMemberKey() throws Exception {
    final Class<?> memberKeyClass = Class.forName("mondrian.rolap.MemberKey");
    final Class<?> rolapMemberClass = Class.forName("mondrian.rolap.RolapMember");
    final Constructor<?> ctor =
        memberKeyClass.getDeclaredConstructor(rolapMemberClass, Object.class);
    ctor.setAccessible(true);
    final Object memberKey = ctor.newInstance(new Object[] { null, "Central" });

    assertEquals("Central", CrossJoinFunDef.toComparableChainKeyString(memberKey));
  }

  public void testChainPatternToExactSignatureRequiresNoNulls() throws Exception {
    final Class<?> patternClass =
        Class.forName("mondrian.olap.fun.CrossJoinFunDef$ChainPattern");
    final Constructor<?> patternCtor =
        patternClass.getDeclaredConstructor(String[].class);
    patternCtor.setAccessible(true);
    final Method toExactSignature =
        patternClass.getDeclaredMethod("toExactSignature");
    toExactSignature.setAccessible(true);

    final Object exactPattern =
        patternCtor.newInstance(new Object[] { new String[] { "A", "B" } });
    final Object exactSignature = toExactSignature.invoke(exactPattern);
    assertNotNull(exactSignature);

    final Object wildcardPattern =
        patternCtor.newInstance(new Object[] { new String[] { "A", null } });
    final Object wildcardSignature = toExactSignature.invoke(wildcardPattern);
    assertNull(wildcardSignature);
  }

}
