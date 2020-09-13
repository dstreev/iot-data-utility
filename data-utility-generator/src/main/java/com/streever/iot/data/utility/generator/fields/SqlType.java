package com.streever.iot.data.utility.generator.fields;

import java.util.HashMap;
import java.util.Map;

public enum SqlType {
    HIVE;

    // Mapping Layer
    public String getSqlField(Object value) {
        /*
        https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types#LanguageManualTypes-NumericTypes
         */
        String rtn = null;
        rtn = FieldUtils.sqlType(value);
        if (rtn == null) {
            throw new RuntimeException("Map missing for type: " + value.getClass().getSimpleName());
        }
        return rtn;
    }

}
