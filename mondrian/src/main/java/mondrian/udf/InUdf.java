/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
*/

package mondrian.udf;

import mondrian.olap.*;
import mondrian.olap.type.*;
import mondrian.spi.UserDefinedFunction;

import java.util.List;

/**
 * User-defined function <code>IN</code>.
 *
 * @author schoi
 */
public class InUdf implements UserDefinedFunction {

    @Override public Object execute(Evaluator evaluator, Argument[] arguments) {
        Object arg0 = arguments[0].evaluate(evaluator);
        List arg1 = (List) arguments[1].evaluate(evaluator);

        for (Object anArg1 : arg1) {
            if (((Member) arg0).getUniqueName().equals(
                    ((Member) anArg1).getUniqueName()))
            {
                return Boolean.TRUE;
            }
        }
        return Boolean.FALSE;
    }

    @Override public String getDescription() {
        return "Returns true if the member argument is contained in the set "
            + "argument.";
    }

    @Override public String getName() {
        return "IN";
    }

    @Override public Type[] getParameterTypes() {
        return new Type[] {
            MemberType.Unknown,
            new SetType(MemberType.Unknown)
        };
    }

    @Override public String[] getReservedWords() {
        // This function does not require any reserved words.
        return null;
    }

    @Override public Type getReturnType(Type[] parameterTypes) {
        return new BooleanType();
    }

    @Override public Syntax getSyntax() {
        return Syntax.Infix;
    }

}

// End InUdf.java
