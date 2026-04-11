/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.spi;

import junit.framework.TestCase;
import mondrian.rolap.BitKey;
import mondrian.util.ByteString;

import java.util.Collections;

public class SegmentHeaderIdentityTest extends TestCase {

    public void testSameSubcubeFingerprintSameUniqueId() {
        final SegmentHeader left = newHeader("subcube:fingerprint:A");
        final SegmentHeader right = newHeader("subcube:fingerprint:A");

        assertEquals(left.getUniqueID(), right.getUniqueID());
    }

    public void testDifferentSubcubeFingerprintDifferentUniqueId() {
        final SegmentHeader left = newHeader("subcube:fingerprint:A");
        final SegmentHeader right = newHeader("subcube:fingerprint:B");

        assertFalse(left.getUniqueID().equals(right.getUniqueID()));
    }

    private SegmentHeader newHeader(String subcubeFingerprint) {
        return new SegmentHeader(
            "schema",
            new ByteString(new byte[] {1, 2, 3}),
            "cube",
            "measure",
            Collections.<SegmentColumn>emptyList(),
            Collections.<String>emptyList(),
            "fact",
            BitKey.Factory.makeBitKey(8),
            Collections.<SegmentColumn>emptyList(),
            subcubeFingerprint);
    }
}

