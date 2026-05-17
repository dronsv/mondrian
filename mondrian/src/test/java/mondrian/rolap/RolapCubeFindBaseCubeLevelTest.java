/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026
// All Rights Reserved.
*/

package mondrian.rolap;

import mondrian.olap.Dimension;
import mondrian.olap.Hierarchy;
import mondrian.olap.Level;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RolapCubeFindBaseCubeLevelTest {

  /**
   * Regression for emondrian-clickhouse#76: when
   * {@code findBaseCubeLevel} walks the base cube's hierarchies and finds a
   * level whose name matches the target but whose runtime type is plain
   * {@link RolapLevel} (e.g. a level borrowed from a synthetic-flat source
   * hierarchy rather than a cube-projected {@link RolapCubeLevel}), the
   * method must return {@code null} instead of throwing
   * {@code ClassCastException}. Returning {@code null} lets the upstream
   * helper {@code SqlTupleReader.getBaseStarKeyColumnOrNull} decline the
   * native filter path safely, instead of propagating a SOAP fault to the
   * XMLA client.
   */
  @Test
  public void testReturnsNullWhenMatchedLevelIsNotRolapCubeLevel()
      throws Exception
  {
    RolapCube cube = mock(RolapCube.class);

    Field f = RolapCube.class.getDeclaredField("virtualToBaseMap");
    f.setAccessible(true);
    f.set(cube, new HashMap<RolapLevel, RolapCubeLevel>());

    doCallRealMethod().when(cube).findBaseCubeLevel(any(RolapLevel.class));

    RolapLevel input = mock(RolapLevel.class);
    Dimension inputDim = mock(Dimension.class);
    RolapHierarchy inputHier = mock(RolapHierarchy.class);
    when(input.getDimension()).thenReturn(inputDim);
    when(input.getHierarchy()).thenReturn(inputHier);
    when(input.getName()).thenReturn("Месяц");
    when(inputDim.getName()).thenReturn("Период");
    when(inputHier.getName()).thenReturn("Месяц");

    Dimension cubeDim = mock(Dimension.class);
    when(cubeDim.getName()).thenReturn("Период");
    Hierarchy cubeHier = mock(Hierarchy.class);
    when(cubeHier.getName()).thenReturn("Месяц");

    Level plainLvl = mock(Level.class);
    when(plainLvl.getName()).thenReturn("Месяц");
    when(cubeHier.getLevels()).thenReturn(new Level[] { plainLvl });
    when(cubeDim.getHierarchies()).thenReturn(new Hierarchy[] { cubeHier });

    when(cube.getDimensions()).thenReturn(new Dimension[] { cubeDim });

    assertNull(cube.findBaseCubeLevel(input));
  }
}
