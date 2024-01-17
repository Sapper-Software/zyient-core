/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.zyient.base.common.utils;


import com.google.common.base.Preconditions;
import lombok.NonNull;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class CollectionUtils {

    public static <T> void setAtIndex(@NonNull List<T> array,
                                      int index,
                                      @NonNull T value) {
        Preconditions.checkArgument(index > 0);
        if (array.size() > index) {
            array.set(index, value);
        } else {
            for (int ii = array.size(); ii <= index; ii++) {
                array.add(null);
            }
            array.set(index, value);
        }
    }

    /**
     * Set the value of the specified field to a list of elements converted from the
     * List of input strings.
     * <pre>
     * Supported Lists:
     *      Boolean
     *      Char
     *      Short
     *      Integer
     *      Long
     *      Float
     *      Double
     *      BigInteger
     *      BigDecimal
     *      Date (java.util)
     * </pre>
     *
     * @param source - Source object to set the field value in.
     * @param field  - Field in the source type to set.
     * @param values - List of string values to convert from.
     * @throws Exception
     */
    public static final void setListValues(@NonNull Object source,
                                           @NonNull Field field,
                                           @NonNull List<String> values)
            throws Exception {

        Class<?> type = field.getType();
        if (!type.equals(List.class)) {
            throw new Exception(
                    String.format("Invalid field type. [expected=%s][actual=%s]",
                            List.class.getCanonicalName(),
                            type.getCanonicalName()));
        }
        Class<?> ptype = ReflectionHelper.getGenericCollectionType(field);
        Preconditions.checkNotNull(ptype);
        if (ptype.equals(String.class)) {
            ReflectionHelper.setValue(values, source, field);
        } else if (ptype.equals(Boolean.class)) {
            List<Boolean> bl = createBoolList(values);
            ReflectionHelper.setValue(bl, source, field);
        } else if (ptype.equals(Character.class)) {
            List<Character> bl = createCharList(values);
            ReflectionHelper.setValue(bl, source, field);
        } else if (ptype.equals(Short.class)) {
            List<Short> bl = createShortList(values);
            ReflectionHelper.setValue(bl, source, field);
        } else if (ptype.equals(Integer.class)) {
            List<Integer> bl = createIntList(values);
            ReflectionHelper.setValue(bl, source, field);
        } else if (ptype.equals(Long.class)) {
            List<Long> bl = createLongList(values);
            ReflectionHelper.setValue(bl, source, field);
        } else if (ptype.equals(Float.class)) {
            List<Float> bl = createFloatList(values);
            ReflectionHelper.setValue(bl, source, field);
        } else if (ptype.equals(Double.class)) {
            List<Double> bl = createDoubleList(values);
            ReflectionHelper.setValue(bl, source, field);
        } else if (ptype.equals(BigInteger.class)) {
            List<BigInteger> bl = createBigIntegerList(values);
            ReflectionHelper.setValue(bl, source, field);
        } else if (ptype.equals(BigDecimal.class)) {
            List<BigDecimal> bl = createBigDecimalList(values);
            ReflectionHelper.setValue(bl, source, field);
        } else if (ptype.equals(Date.class)) {
            List<Date> bl = createDateList(values);
            ReflectionHelper.setValue(bl, source, field);
        }
    }

    private static List<Boolean> createBoolList(List<String> values) {
        List<Boolean> nvalues = new ArrayList<>(values.size());
        for (String value : values) {
            nvalues.add(Boolean.parseBoolean(value));
        }
        return nvalues;
    }

    private static List<Character> createCharList(List<String> values) {
        List<Character> nvalues = new ArrayList<>(values.size());
        for (String value : values) {
            nvalues.add(value.charAt(0));
        }
        return nvalues;
    }

    private static List<Short> createShortList(List<String> values) {
        List<Short> nvalues = new ArrayList<>(values.size());
        for (String value : values) {
            nvalues.add(Short.parseShort(value));
        }
        return nvalues;
    }

    private static List<Integer> createIntList(List<String> values) {
        List<Integer> nvalues = new ArrayList<>(values.size());
        for (String value : values) {
            nvalues.add(Integer.parseInt(value));
        }
        return nvalues;
    }

    private static List<Long> createLongList(List<String> values) {
        List<Long> nvalues = new ArrayList<>(values.size());
        for (String value : values) {
            nvalues.add(Long.parseLong(value));
        }
        return nvalues;
    }

    private static List<Float> createFloatList(List<String> values) {
        List<Float> nvalues = new ArrayList<>(values.size());
        for (String value : values) {
            nvalues.add(Float.parseFloat(value));
        }
        return nvalues;
    }

    private static List<Double> createDoubleList(List<String> values) {
        List<Double> nvalues = new ArrayList<>(values.size());
        for (String value : values) {
            nvalues.add(Double.parseDouble(value));
        }
        return nvalues;
    }

    private static List<BigInteger> createBigIntegerList(List<String> values) {
        List<BigInteger> nvalues = new ArrayList<>(values.size());
        for (String value : values) {
            nvalues.add(new BigInteger(value));
        }
        return nvalues;
    }

    private static List<BigDecimal> createBigDecimalList(List<String> values) {
        List<BigDecimal> nvalues = new ArrayList<>(values.size());
        for (String value : values) {
            nvalues.add(new BigDecimal(value));
        }
        return nvalues;
    }

    private static List<Date> createDateList(List<String> values) {
        List<Date> nvalues = new ArrayList<>(values.size());
        SimpleDateFormat format = new SimpleDateFormat();
        try {
            for (String value : values) {
                Date dt = format.parse(value);
                nvalues.add(dt);
            }
            return nvalues;
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Set the value of the specified field to a Set of elements converted from the
     * List of input strings.
     * <pre>
     * Supported Lists:
     *      Boolean
     *      Char
     *      Short
     *      Integer
     *      Long
     *      Float
     *      Double
     *      BigInteger
     *      BigDecimal
     *      Date (java.util)
     * </pre>
     *
     * @param source - Source object to set the field value in.
     * @param field  - Field in the source type to set.
     * @param values - List of string values to convert from.
     * @throws Exception
     */
    public static final void setSetValues(@NonNull Object source,
                                          @NonNull Field field,
                                          @NonNull List<String> values)
            throws Exception {
        Preconditions.checkArgument(source != null);
        Preconditions.checkArgument(field != null);
        Preconditions.checkArgument(values != null);

        Class<?> type = field.getType();
        if (!type.equals(Set.class)) {
            throw new Exception(
                    String.format("Invalid field type. [expected=%s][actual=%s]",
                            Set.class.getCanonicalName(),
                            type.getCanonicalName()));
        }
        Class<?> ptype = ReflectionHelper.getGenericCollectionType(field);
        Preconditions.checkNotNull(ptype);
        if (ptype.equals(String.class)) {
            Set<String> nvalues = new HashSet<>(values.size());
            nvalues.addAll(values);
            ReflectionHelper.setValue(nvalues, source, field);
        } else if (ptype.equals(Boolean.class)) {
            Set<Boolean> bl = createBoolSet(values);
            ReflectionHelper.setValue(bl, source, field);
        } else if (ptype.equals(Character.class)) {
            Set<Character> bl = createCharSet(values);
            ReflectionHelper.setValue(bl, source, field);
        } else if (ptype.equals(Short.class)) {
            Set<Short> bl = createShortSet(values);
            ReflectionHelper.setValue(bl, source, field);
        } else if (ptype.equals(Integer.class)) {
            Set<Integer> bl = createIntSet(values);
            ReflectionHelper.setValue(bl, source, field);
        } else if (ptype.equals(Long.class)) {
            Set<Long> bl = createLongSet(values);
            ReflectionHelper.setValue(bl, source, field);
        } else if (ptype.equals(Float.class)) {
            Set<Float> bl = createFloatSet(values);
            ReflectionHelper.setValue(bl, source, field);
        } else if (ptype.equals(Double.class)) {
            Set<Double> bl = createDoubleSet(values);
            ReflectionHelper.setValue(bl, source, field);
        } else if (ptype.equals(BigInteger.class)) {
            Set<BigInteger> bl = createBigIntegerSet(values);
            ReflectionHelper.setValue(bl, source, field);
        } else if (ptype.equals(BigDecimal.class)) {
            Set<BigDecimal> bl = createBigDecimalSet(values);
            ReflectionHelper.setValue(bl, source, field);
        } else if (ptype.equals(Date.class)) {
            Set<Date> bl = createDateSet(values);
            ReflectionHelper.setValue(bl, source, field);
        }
    }

    private static Set<Boolean> createBoolSet(List<String> values) {
        Set<Boolean> nvalues = new HashSet<>(values.size());
        for (String value : values) {
            nvalues.add(Boolean.parseBoolean(value));
        }
        return nvalues;
    }

    private static Set<Character> createCharSet(List<String> values) {
        Set<Character> nvalues = new HashSet<>(values.size());
        for (String value : values) {
            nvalues.add(value.charAt(0));
        }
        return nvalues;
    }

    private static Set<Short> createShortSet(List<String> values) {
        Set<Short> nvalues = new HashSet<>(values.size());
        for (String value : values) {
            nvalues.add(Short.parseShort(value));
        }
        return nvalues;
    }

    private static Set<Integer> createIntSet(List<String> values) {
        Set<Integer> nvalues = new HashSet<>(values.size());
        for (String value : values) {
            nvalues.add(Integer.parseInt(value));
        }
        return nvalues;
    }

    private static Set<Long> createLongSet(List<String> values) {
        Set<Long> nvalues = new HashSet<>(values.size());
        for (String value : values) {
            nvalues.add(Long.parseLong(value));
        }
        return nvalues;
    }

    private static Set<Float> createFloatSet(List<String> values) {
        Set<Float> nvalues = new HashSet<>(values.size());
        for (String value : values) {
            nvalues.add(Float.parseFloat(value));
        }
        return nvalues;
    }

    private static Set<Double> createDoubleSet(List<String> values) {
        Set<Double> nvalues = new HashSet<>(values.size());
        for (String value : values) {
            nvalues.add(Double.parseDouble(value));
        }
        return nvalues;
    }

    private static Set<BigInteger> createBigIntegerSet(List<String> values) {
        Set<BigInteger> nvalues = new HashSet<>(values.size());
        for (String value : values) {
            nvalues.add(new BigInteger(value));
        }
        return nvalues;
    }

    private static Set<BigDecimal> createBigDecimalSet(List<String> values) {
        Set<BigDecimal> nvalues = new HashSet<>(values.size());
        for (String value : values) {
            nvalues.add(new BigDecimal(value));
        }
        return nvalues;
    }

    private static Set<Date> createDateSet(List<String> values) {
        Set<Date> nvalues = new HashSet<>(values.size());
        SimpleDateFormat format = new SimpleDateFormat();
        try {
            for (String value : values) {
                Date dt = format.parse(value);
                nvalues.add(dt);
            }
            return nvalues;
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }


    public static <T> Collection<T> getRange(@NonNull Collection<T> source, int batchSize, int offset) {
        if (batchSize <= 0 || batchSize > source.size()) batchSize = source.size();
        if (offset < 0) offset = 0;
        if (offset >= source.size()) return null;
        List<T> result = new ArrayList<>();
        int index = 0;
        for (T t : source) {
            if ((index + offset) >= source.size() || index >= batchSize) break;
            result.add(t);
            index++;
        }
        return result;
    }

    public static <T> Collection<T> getRange(@NonNull T[] source, int batchSize, int offset) {
        if (batchSize <= 0 || batchSize > source.length) batchSize = source.length;
        if (offset < 0) offset = 0;
        if (offset >= source.length) return null;
        List<T> result = new ArrayList<>();
        int index = 0;
        for (T t : source) {
            if ((index + offset) >= source.length || index >= batchSize) break;
            result.add(t);
            index++;
        }
        return result;
    }
}
