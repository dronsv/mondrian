/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
*/

package mondrian.udf;

import mondrian.olap.Evaluator;
import mondrian.olap.Syntax;
import mondrian.olap.type.StringType;
import mondrian.olap.type.Type;
import mondrian.spi.UserDefinedFunction;
import mondrian.util.Format;

import java.util.Date;
import java.util.Locale;

/**
 * User-defined function <code>CurrentDateString<code>, which returns the
 * current date value as a formatted string, based on a format string passed in
 * as a parameter.  The format string conforms to the format string implemented
 * by {@link Format}.
 *
 * @author Zelaine Fong
 */
public class CurrentDateStringUdf implements UserDefinedFunction {

    @Override
    public Object execute(Evaluator evaluator, Argument[] arguments) {
        Object arg = arguments[0].evaluateScalar(evaluator);

        final Locale locale = Locale.getDefault();
        final Format format = new Format((String) arg, locale);
        Date currDate = evaluator.getQueryStartTime();
        return format.format(currDate);
    }

    @Override
    public String getDescription() {
        return "Returns the current date formatted as specified by the format "
            + "parameter.";
    }

    @Override
    public String getName() {
        return "CurrentDateString";
    }

    @Override
    public Type[] getParameterTypes() {
        return new Type[] { new StringType() };
    }

    @Override
    public String[] getReservedWords() {
        return null;
    }

    @Override
    public Type getReturnType(Type[] parameterTypes) {
        return new StringType();
    }

    @Override
    public Syntax getSyntax() {
        return Syntax.Function;
    }

}

// End CurrentDateStringUdf.java
