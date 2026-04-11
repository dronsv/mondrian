/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
*/

package mondrian.util;

/**
 * The <code>FauxMemoryMonitor</code> implements the <code>MemoryMonitor</code>
 * interface but does nothing: all methods are empty.
 *
 * @author <a>Richard M. Emberson</a>
 * @since Feb 03 2007
 */
public class FauxMemoryMonitor implements MemoryMonitor {
    FauxMemoryMonitor() {
    }

    @Override
    public boolean addListener(Listener listener, int thresholdPercentage) {
        return true;
    }

    @Override
    public boolean addListener(final Listener listener) {
        return true;
    }

    @Override
    public void updateListenerThreshold(Listener listener, int percentage) {
        // empty
    }

    @Override
    public boolean removeListener(Listener listener) {
        return true;
    }
    @Override
    public void removeAllListener() {
        // empty
    }
    @Override
    public long getMaxMemory() {
        return Runtime.getRuntime().maxMemory();
    }
    @Override
    public long getUsedMemory() {
        return Runtime.getRuntime().freeMemory();
    }
}

// End FauxMemoryMonitor.java
