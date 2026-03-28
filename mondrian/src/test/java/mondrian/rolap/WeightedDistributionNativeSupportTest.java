package mondrian.rolap;

import junit.framework.TestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.when;

public class WeightedDistributionNativeSupportTest extends TestCase {
    public void testMatchesCurrentWeightedDistributionFormula() {
        assertTrue(
            WeightedDistributionNativeSupport.matchesWeightedDistributionFormula(
                new TestExp(
                    "IIF(IsEmpty([Measures].[Продажи руб]) OR "
                    + "([Measures].[Продажи руб], [Продукт.Категория].[Все категории], "
                    + "[Продукт.Подкатегория].[Все подкатегории], [Продукт.Вес].[Все веса], "
                    + "[Продукт.Бренд].[Все бренды], [Продукт.СКЮ].[Все СКЮ], "
                    + "[Продукт.Производитель].[Все производители], "
                    + "[Продукт.Код товара в сети].[Все коды], "
                    + "[Продукт.Название в сети].[Все названия], "
                    + "[ТТ.Адрес].[Все адреса]) = 0, NULL, "
                    + "Sum(Filter([ТТ.Адрес].[Адрес].Members, NOT IsEmpty([Measures].[Продажи руб])), "
                    + "([Measures].[Продажи руб], [Продукт.Категория].[Все категории], "
                    + "[Продукт.Подкатегория].[Все подкатегории], [Продукт.Вес].[Все веса], "
                    + "[Продукт.Бренд].[Все бренды], [Продукт.СКЮ].[Все СКЮ], "
                    + "[Продукт.Производитель].[Все производители], "
                    + "[Продукт.Код товара в сети].[Все коды], "
                    + "[Продукт.Название в сети].[Все названия])) / "
                    + "([Measures].[Продажи руб], [Продукт.Категория].[Все категории], "
                    + "[Продукт.Подкатегория].[Все подкатегории], [Продукт.Вес].[Все веса], "
                    + "[Продукт.Бренд].[Все бренды], [Продукт.СКЮ].[Все СКЮ], "
                    + "[Продукт.Производитель].[Все производители], "
                    + "[Продукт.Код товара в сети].[Все коды], "
                    + "[Продукт.Название в сети].[Все названия], "
                    + "[ТТ.Адрес].[Все адреса]) * 100)")));
    }

    public void testRejectsOtherFormulaShapes() {
        assertFalse(
            WeightedDistributionNativeSupport.matchesWeightedDistributionFormula(
                new TestExp("[Measures].[Продажи руб] / 100")));
    }

    public void testEstimateCellStoreEvaluations() {
        assertEquals(
            56000L * 840L,
            WeightedDistributionNativeSupport.estimateCellStoreEvaluations(
                new TestResult(56000),
                840));
    }

    public void testResolveBindingLevelMapsVirtualLevelToBaseLevel() {
        final RolapCube cube = mock(RolapCube.class);
        final RolapLevel virtualLevel = mock(RolapLevel.class);
        final RolapCubeLevel baseLevel = mock(RolapCubeLevel.class);

        when(cube.findBaseCubeLevel(same(virtualLevel))).thenReturn(baseLevel);

        assertSame(
            baseLevel,
            WeightedDistributionNativeSupport.resolveBindingLevel(
                cube,
                virtualLevel));
    }

    public void testResolveBindingLevelKeepsOriginalLevelWhenNoBaseMatch() {
        final RolapCube cube = mock(RolapCube.class);
        final RolapLevel level = mock(RolapLevel.class);

        assertSame(
            level,
            WeightedDistributionNativeSupport.resolveBindingLevel(
                cube,
                level));
    }

    private static final class TestExp extends mondrian.olap.ExpBase {
        private final String text;

        private TestExp(String text) {
            this.text = text;
        }

        public int getCategory() {
            return 0;
        }

        public mondrian.olap.type.Type getType() {
            return null;
        }

        public void unparse(java.io.PrintWriter pw) {
            pw.print(text);
        }

        public mondrian.olap.Exp accept(mondrian.olap.Validator validator) {
            return this;
        }

        public mondrian.calc.Calc accept(
            mondrian.calc.ExpCompiler compiler)
        {
            return null;
        }

        public Object accept(mondrian.mdx.MdxVisitor visitor) {
            return null;
        }

        public String toString() {
            return text;
        }

        public TestExp clone() {
            return new TestExp(text);
        }
    }

    private static final class TestResult implements mondrian.olap.Result {
        private final mondrian.olap.Axis[] axes;

        private TestResult(final int rows) {
            this.axes = new mondrian.olap.Axis[] {
                new TestAxis(1),
                new TestAxis(rows)
            };
        }

        public mondrian.olap.Query getQuery() {
            return null;
        }

        public mondrian.olap.Axis[] getAxes() {
            return axes;
        }

        public mondrian.olap.Axis getSlicerAxis() {
            return new TestAxis(1);
        }

        public mondrian.olap.Cell getCell(int[] pos) {
            return null;
        }

        public void print(java.io.PrintWriter pw) {
        }

        public void close() {
        }
    }

    private static final class TestAxis implements mondrian.olap.Axis {
        private final java.util.List<mondrian.olap.Position> positions;

        private TestAxis(int size) {
            this.positions = new java.util.ArrayList<mondrian.olap.Position>();
            for (int i = 0; i < size; i++) {
                this.positions.add(new EmptyPosition());
            }
        }

        public java.util.List<mondrian.olap.Position> getPositions() {
            return positions;
        }
    }

    private static final class EmptyPosition
        extends java.util.AbstractList<mondrian.olap.Member>
        implements mondrian.olap.Position
    {
        public mondrian.olap.Member get(int index) {
            throw new IndexOutOfBoundsException(index);
        }

        public int size() {
            return 0;
        }
    }
}
