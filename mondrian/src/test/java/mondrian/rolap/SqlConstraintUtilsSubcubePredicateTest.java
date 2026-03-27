package mondrian.rolap;

import junit.framework.TestCase;
import mondrian.olap.Query;
import mondrian.rolap.sql.SqlQuery;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SqlConstraintUtilsSubcubePredicateTest extends TestCase {

  public void testAddSubcubeConstraintAddsWhereClauseWhenPredicateExists() {
    SqlQuery sqlQuery = mock( SqlQuery.class );
    Query query = mock( Query.class );
    RolapCube baseCube = mock( RolapCube.class );
    StarPredicate predicate = mock( StarPredicate.class );

    when( query.getSubcubePredicates( baseCube ) ).thenReturn( predicate );
    doAnswer( invocation -> {
      StringBuilder where = (StringBuilder) invocation.getArguments()[1];
      where.append( "brand = 'Zimniy Buket'" );
      return null;
    } ).when( predicate ).toSql( any( SqlQuery.class ), any( StringBuilder.class ) );

    SqlConstraintUtils.addSubcubeConstraint( sqlQuery, query, baseCube );

    verify( sqlQuery ).addWhere( "brand = 'Zimniy Buket'" );
  }

  public void testAddSubcubeConstraintSkipsMissingPredicate() {
    SqlQuery sqlQuery = mock( SqlQuery.class );
    Query query = mock( Query.class );
    RolapCube baseCube = mock( RolapCube.class );

    when( query.getSubcubePredicates( baseCube ) ).thenReturn( null );

    SqlConstraintUtils.addSubcubeConstraint( sqlQuery, query, baseCube );

    verify( sqlQuery, never() ).addWhere( anyString() );
  }
}
