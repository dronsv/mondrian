/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import junit.framework.TestCase;
import mondrian.olap.Member;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ResultLoaderColumnOffsetTest extends TestCase {

  public void testLoadResultStartsFromConfiguredColumnOffset() throws Exception {
    final TupleReader.MemberBuilder memberBuilder = mock( TupleReader.MemberBuilder.class );
    when( memberBuilder.getMemberCacheLock() ).thenReturn( new Object() );
    final RolapLevel level = mock( RolapLevel.class );
    when( level.getUniqueName() ).thenReturn( "[Mock].[L]" );
    final RecordingTarget nativeTarget =
      new RecordingTarget( null, level, memberBuilder );
    nativeTarget.open();

    final SqlStatement stmt = mock( SqlStatement.class );
    final ResultSet rs = mock( ResultSet.class );
    when( stmt.getResultSet() ).thenReturn( rs );
    when( rs.next() ).thenReturn( false );

    final ResultLoader loader =
      new ResultLoader(
        0,
        Collections.<TargetBase>singletonList( nativeTarget ),
        stmt,
        true,
        null,
        null,
        3 );

    loader.loadResult();

    assertEquals( 3, nativeTarget.firstColumn );
  }

  public void testLoadResultStartsFromConfiguredColumnOffsetForEnumPath()
    throws Exception {
    final TupleReader.MemberBuilder memberBuilder = mock( TupleReader.MemberBuilder.class );
    when( memberBuilder.getMemberCacheLock() ).thenReturn( new Object() );
    final RolapLevel level = mock( RolapLevel.class );
    when( level.getUniqueName() ).thenReturn( "[Mock].[L]" );
    final RolapMember enumMember = mock( RolapMember.class );

    final RecordingTarget enumTarget =
      new RecordingTarget(
        Collections.singletonList( enumMember ),
        level,
        memberBuilder );
    enumTarget.open();
    final RecordingTarget nativeTarget =
      new RecordingTarget( null, level, memberBuilder );
    nativeTarget.open();

    final List<TargetBase> targets = new ArrayList<TargetBase>();
    targets.add( enumTarget );
    targets.add( nativeTarget );

    final SqlStatement stmt = mock( SqlStatement.class );
    final ResultSet rs = mock( ResultSet.class );
    when( stmt.getResultSet() ).thenReturn( rs );
    when( rs.next() ).thenReturn( false );

    final ResultLoader loader =
      new ResultLoader(
        1,
        targets,
        stmt,
        true,
        null,
        null,
        4 );

    loader.loadResult();

    assertEquals( 4, nativeTarget.firstColumn );
  }

  private static class RecordingTarget extends TargetBase {
    private int firstColumn = -1;

    RecordingTarget(
      List<RolapMember> srcMembers,
      RolapLevel level,
      TupleReader.MemberBuilder memberBuilder ) {
      super( srcMembers, level, memberBuilder );
    }

    @Override
    public void open() {
      setList( new ArrayList<RolapMember>() );
    }

    @Override
    public List<Member> close() {
      return Collections.<Member>emptyList();
    }

    @Override
    int internalAddRow( SqlStatement stmt, int column ) {
      if ( firstColumn < 0 ) {
        firstColumn = column;
      }
      add( null );
      return column + 1;
    }
  }
}
