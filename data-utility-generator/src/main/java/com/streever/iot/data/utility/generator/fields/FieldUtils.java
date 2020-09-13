package com.streever.iot.data.utility.generator.fields;

import com.streever.iot.data.utility.generator.fields.support.GeoLocation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FieldUtils {
//    public static boolean isPrimitiveType(Object source) {
//        return WRAPPER_TYPE_MAP.containsKey(source.getClass());
//    }

    private static final Map<Class<?>, String> HIVE_TYPE_MAP;

    public static String sqlType(Object value) {
        Class clazz = value.getClass();
        String rtn = HIVE_TYPE_MAP.get(clazz);
        if (value instanceof List) {
            if (((List<?>) value).size() > 0) {
                String subType = HIVE_TYPE_MAP.get(((List<?>) value).get(0).getClass());
                if (subType != null) {
                    rtn = rtn + "<" + subType + ">";
                }
            }
        }
        return rtn;
    }

    static {
        HIVE_TYPE_MAP = new HashMap<Class<?>, String>(16);
        HIVE_TYPE_MAP.put(Integer.class, "INT");
        HIVE_TYPE_MAP.put(Byte.class, "SMALLINT");
        HIVE_TYPE_MAP.put(Character.class, "CHAR");
        HIVE_TYPE_MAP.put(Boolean.class, "BOOLEAN");
        HIVE_TYPE_MAP.put(Double.class, "DOUBLE");
        HIVE_TYPE_MAP.put(Float.class, "FLOAT");
        HIVE_TYPE_MAP.put(Long.class, "BIGINT");
        HIVE_TYPE_MAP.put(Short.class, "SMALLINT");
        HIVE_TYPE_MAP.put(String.class, "STRING");
        HIVE_TYPE_MAP.put(GeoLocation.class, "STRING");
        HIVE_TYPE_MAP.put(ArrayList.class, "ARRAY");
    }


}
