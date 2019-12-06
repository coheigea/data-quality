// ============================================================================
//
// Copyright (C) 2006-2016 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.dataquality.statistics.type;

import java.math.BigInteger;
import java.util.regex.Pattern;

import org.talend.dataquality.statistics.datetime.SystemDateTimePatternManager;

/**
 * Utility class refering data types given single value
 * 
 * @author zhao
 *
 */
public class TypeInferenceUtils {

    private static final Pattern patternInteger = Pattern.compile("^([-－+＋])?[0-9０-９]+$");

    private static final Pattern patternDouble = Pattern.compile("^[-+－＋]?"// Positive/Negative sign
            + "("// BEGIN Decimal part
            + "[0-9０-９]+([,.．][0-9０-９]+)?|"// Alternative I (w/o grouped integer part)
            + "(" // BEGIN Alternative II (with grouped integer part)
            + "[0-9]{1,3}" // starting digits
            + "(" // BEGIN grouped part
            + "((,[0-9]{3})*"// US integer part
            + "(\\.[0-9]+)?"// US float part
            + "|" // OR
            + "((\\.[0-9]{3})*|([ \u00A0\u2007\u202F][0-9]{3})*)"// EU integer part
            + "(,[0-9]+)?)"// EU float part
            + ")"// END grouped part
            + ")" // END Alternative II
            + ")" // END Decimal part
            + "([ \u3000]?[eEｅＥ][-+－＋]?[0-9０-９]+)?" // scientific part
            + "([ \u3000]?[%％])?$"); // percentage part

    /**
     * Detect if the given value is a double type.
     * 
     * <p>
     * Note:<br>
     * 1. This method support only English locale.<br>
     * e.g. {@code TypeInferenceUtils.isDouble("3.4")} returns {@code true}.<br>
     * e.g. {@code TypeInferenceUtils.isDouble("3,4")} returns {@code false}.<br>
     * 2. Exponential notation can be detected as a valid double.<br>
     * e.g. {@code TypeInferenceUtils.isDouble("1.0E+4")} returns {@code true}.<br>
     * e.g. {@code TypeInferenceUtils.isDouble("1.0e-4")} returns {@code true}.<br>
     * e.g. {@code TypeInferenceUtils.isDouble("1.0e-04")} returns {@code true}.<br>
     * 3. Numbers marked with a type is invalid.<br>
     * e.g. {@code TypeInferenceUtils.isDouble("3.4d")} returns {@code false}.<br>
     * e.g. {@code TypeInferenceUtils.isDouble("123L")} returns {@code false}.<br>
     * 4. White space is invalid.<br>
     * e.g. {@code TypeInferenceUtils.isDouble(" 3.4")} returns {@code false}.<br>
     * e.g. {@code TypeInferenceUtils.isDouble("3.4 ")} returns {@code false}.<br>
     * 5. "." is not obligatory.<br>
     * e.g. {@code TypeInferenceUtils.isDouble("100")} returns {@code true}.
     * <P>
     * 
     * @param value the value to be detected.
     * @return true if the value is a double type, false otherwise.
     */
    public static boolean isDouble(String value) {
        if (!isEmpty(value) && patternDouble.matcher(value).matches()) {
            return true;
        }
        return false;
    }

    /**
     * Detect if the given value is a integer type.
     * 
     * @param value the value to be detected.
     * @return true if the value is a integer type, false otherwise.
     */
    public static boolean isInteger(String value) {
        if (!isEmpty(value) && patternInteger.matcher(value).matches()) {
            return true;
        }
        return false;
    }

    public static boolean isNumber(String value) {
        return isDouble(value) || isInteger(value);

    }

    /**
     * Get big integer from a string.
     * 
     * @param value
     * @return big integer instance , or null if numer format exception occurrs.
     */
    public static BigInteger getBigInteger(String value) {
        BigInteger bint = null;
        try {
            bint = new BigInteger(value);
        } catch (NumberFormatException e) {
            return null;
        }
        return bint;
    }

    /**
     * Detect if the given value is a boolean type.
     * 
     * @param value the value to be detected.
     * @return true if the value is a boolean type, false otherwise.
     */
    public static boolean isBoolean(String value) {
        if (isEmpty(value)) {
            return false;
        }
        if ((value.trim().length() == 4 || value.trim().length() == 5)
                && ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value))) { //$NON-NLS-1$ //$NON-NLS-2$
            return true;
        }
        return false;
    }

    /**
     * Detect if the given value is a date type. <br>
     *
     * @param value the value to be detected.
     * @return true if the value is a date type, false otherwise.
     * @see SystemDateTimePatternManager#isDate(String)
     */
    public static boolean isDate(String value) {
        return SystemDateTimePatternManager.isDate(value);
    }

    /**
     * Detect if the given value is a time type.
     * 
     * @param value
     * @return
     */
    public static boolean isTime(String value) {
        return SystemDateTimePatternManager.isTime(value);
    }

    /**
     * Detect if the given value is blank or null.
     * 
     * @param value the value to be detected.
     * @return true if the value is blank or null, false otherwise.
     */
    public static boolean isEmpty(String value) {
        return value == null || value.trim().length() == 0;
    }

    /**
     * 
     * @param type the expected type
     * @param value the value to be detected
     * @return true if the type of value is expected, false otherwise.
     */
    public static boolean isValid(DataTypeEnum type, String value) {

        switch (type) {
        case BOOLEAN:
            return isBoolean(value);
        case INTEGER:
            return isInteger(value);
        case DOUBLE:
            return isDouble(value);
        case DATE:
            return isDate(value);
        case STRING:
            // Everything can be a string
            return true;
        default:
            // Unsupported type
            return false;
        }
    }

    public static DataTypeEnum getDataType(String value) {
        DataTypeEnum dataTypeEnum = getNativeDataType(value);
        // STRING means we didn't find any native data types
        if (DataTypeEnum.STRING.equals(dataTypeEnum)) {
            if (isDate(value)) {
                // 5. detect date
                dataTypeEnum = DataTypeEnum.DATE;
            } else if (isTime(value)) {
                // 6. detect date
                dataTypeEnum = DataTypeEnum.TIME;
            }
        }
        return dataTypeEnum;
    }

    /**
     * discovery for native/basic data types
     * 
     * @param value to discover
     * @return the discovered data type
     */
    public static DataTypeEnum getNativeDataType(String value) {
        if (TypeInferenceUtils.isEmpty(value)) {
            // 1. detect empty
            return DataTypeEnum.EMPTY;
        } else if (TypeInferenceUtils.isBoolean(value)) {
            // 2. detect boolean
            return DataTypeEnum.BOOLEAN;
        } else if (TypeInferenceUtils.isInteger(value)) {
            // 3. detect integer
            return DataTypeEnum.INTEGER;
        } else if (TypeInferenceUtils.isDouble(value)) {
            // 4. detect double
            return DataTypeEnum.DOUBLE;
        }
        // will return string when no matching
        return DataTypeEnum.STRING;
    }
}
