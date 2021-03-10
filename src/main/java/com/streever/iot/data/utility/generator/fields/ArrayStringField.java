package com.streever.iot.data.utility.generator.fields;

import org.apache.commons.lang3.RandomStringUtils;

public class ArrayStringField extends ArrayField<String> {

    @Override
    protected String get() {
        return RandomStringUtils.random(getCharacterLength().intValue(), getCharacters());
    }
}
