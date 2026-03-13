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
import mondrian.calc.TupleCollections;
import mondrian.calc.TupleList;
import mondrian.olap.Member;
import mondrian.rolap.RolapLevel;
import mondrian.rolap.RolapMember;
import mondrian.rolap.sql.dependency.DependencyRegistry;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

  public void testTupleDependentJoinMatchesDeterminantFromUnaryAgainstMixedTuple() throws Exception {
    final RolapLevel manufacturerLevel = mock(RolapLevel.class);
    when(manufacturerLevel.getUniqueName()).thenReturn("[Продукт.Производитель].[Производитель]");
    final RolapLevel brandLevel = mock(RolapLevel.class);
    when(brandLevel.getUniqueName()).thenReturn("[Продукт.Бренд].[Бренд]");

    final RolapMember manufacturerA = mockRolapMember(manufacturerLevel, "mfr-a", "MFR A");
    final RolapMember manufacturerB = mockRolapMember(manufacturerLevel, "mfr-b", "MFR B");
    final List<RolapMember> unaryMembers = Arrays.asList(manufacturerA, manufacturerB);

    final TupleList unaryList = TupleCollections.createList(1, 2);
    unaryList.addTuple(manufacturerA);
    unaryList.addTuple(manufacturerB);

    final Member chainA = mock(Member.class);
    final RolapMember brandA = mockRolapMember(brandLevel, "brand-a", "Brand A");
    final RolapMember brandOrphan = mockRolapMember(brandLevel, "brand-x", "Brand X");
    when(brandA.getPropertyValue("manufacturer_key")).thenReturn("mfr-a");
    when(brandOrphan.getPropertyValue("manufacturer_key")).thenReturn("mfr-x");

    final TupleList tupleList = TupleCollections.createList(2, 2);
    tupleList.addTuple(chainA, brandA);
    tupleList.addTuple(chainA, brandOrphan);

    final DependencyRegistry.CompiledDependencyRule rule =
        new DependencyRegistry.CompiledDependencyRule(
            manufacturerLevel.getUniqueName(),
            DependencyRegistry.DependencyMappingType.PROPERTY,
            "manufacturer_key",
            true,
            false);

    final Method method =
        CrossJoinFunDef.class.getDeclaredMethod(
            "tryBuildDependencyJoinedTupleListForTupleDependentColumn",
            TupleList.class,
            List.class,
            RolapLevel.class,
            DependencyRegistry.CompiledDependencyRule.class,
            TupleList.class,
            int.class,
            boolean.class);
    method.setAccessible(true);

    final TupleList joined =
        (TupleList) method.invoke(
            null,
            unaryList,
            unaryMembers,
            manufacturerLevel,
            rule,
            tupleList,
            1,
            false);

    assertNotNull(joined);
    assertEquals(1, joined.size());
    assertEquals(3, joined.getArity());
    assertSame(chainA, joined.get(0, 0));
    assertSame(brandA, joined.get(1, 0));
    assertEquals("mfr-a", ((RolapMember) joined.get(2, 0)).getKey());
  }

  private RolapMember mockRolapMember(RolapLevel level, Object key, String name) {
    final RolapMember member = mock(RolapMember.class);
    when(member.getLevel()).thenReturn(level);
    when(member.getKey()).thenReturn(key);
    when(member.getName()).thenReturn(name);
    when(member.isAll()).thenReturn(false);
    when(member.isCalculated()).thenReturn(false);
    return member;
  }

}
