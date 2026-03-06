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
import mondrian.rolap.RolapLevel;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

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

  public void testCanUseExactSignatureLookupRequiresUniqueDeterminantLevels()
      throws Exception {
    final Method canUseExactLookup =
        CrossJoinFunDef.class.getDeclaredMethod(
            "canUseExactSignatureLookup",
            List.class);
    canUseExactLookup.setAccessible(true);

    final RolapLevel uniqueLevel = newRolapLevelWithUniqueFlag(true);
    final RolapLevel nonUniqueLevel = newRolapLevelWithUniqueFlag(false);

    final List<Object> uniqueColumns = new ArrayList<Object>();
    uniqueColumns.add(newChainDeterminantColumn(0, uniqueLevel, 0));
    uniqueColumns.add(newChainDeterminantColumn(1, uniqueLevel, 1));
    assertEquals(Boolean.TRUE, canUseExactLookup.invoke(null, uniqueColumns));

    final List<Object> mixedColumns = new ArrayList<Object>();
    mixedColumns.add(newChainDeterminantColumn(0, uniqueLevel, 0));
    mixedColumns.add(newChainDeterminantColumn(1, nonUniqueLevel, 1));
    assertEquals(Boolean.FALSE, canUseExactLookup.invoke(null, mixedColumns));
  }

  private static Object newChainDeterminantColumn(
      int column,
      RolapLevel determinantLevel,
      int orderIndex) throws Exception {
    final Class<?> columnClass =
        Class.forName("mondrian.olap.fun.CrossJoinFunDef$ChainDeterminantColumn");
    final Class<?> ruleClass =
        Class.forName("mondrian.rolap.sql.dependency.DependencyRegistry$CompiledDependencyRule");
    final Constructor<?> ctor = columnClass.getDeclaredConstructor(
        int.class,
        RolapLevel.class,
        ruleClass,
        int.class);
    ctor.setAccessible(true);
    return ctor.newInstance(column, determinantLevel, null, orderIndex);
  }

  private static RolapLevel newRolapLevelWithUniqueFlag(boolean unique)
      throws Exception {
    final Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
    final Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
    unsafeField.setAccessible(true);
    final Object unsafe = unsafeField.get(null);
    final Method allocateInstance =
        unsafeClass.getMethod("allocateInstance", Class.class);
    final RolapLevel level =
        (RolapLevel) allocateInstance.invoke(unsafe, RolapLevel.class);

    final Field uniqueFlagField = RolapLevel.class.getDeclaredField("FLAG_UNIQUE");
    uniqueFlagField.setAccessible(true);
    final int uniqueFlag = uniqueFlagField.getInt(null);

    final Field flagsField = RolapLevel.class.getDeclaredField("flags");
    flagsField.setAccessible(true);
    flagsField.setInt(level, unique ? uniqueFlag : 0);
    return level;
  }
}
