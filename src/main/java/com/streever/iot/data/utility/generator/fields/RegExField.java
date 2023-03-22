package com.streever.iot.data.utility.generator.fields;

import com.mifmif.common.regex.Generex;
import com.streever.iot.data.utility.generator.fields.support.Pool;

public class RegExField extends FieldBase<String> {
    private Pool<String> pool;
    private String regex = "[A-Z]{3}";
    private Generex generex = new Generex(regex);

    @Override
    public FieldType getFieldType() {
        return FieldType.STRING;
    }

    public Pool<String> getPool() {
        return pool;
    }

    public void setPool(Pool<String> pool) {
        this.pool = pool;
    }

    public String getRegex() {
        return regex;
    }

    public void setRegex(String regex) {

        this.generex = new Generex(regex);

        this.regex = regex;
    }

    protected void buildPool() {
        if (pool != null) {
            for (int i = 0; i < pool.getSize(); i++) {
                pool.getItems().add(generex.random());
            }
            pool.setInitialized(true);
        }
    }

    @Override
    public String getNext() {
        String rtn = null;
        if (pool == null) {
            rtn = generex.random();
        } else {
            if (pool != null && pool.getInitialized() == Boolean.FALSE) {
                buildPool();
            }
            if (pool != null) {
                rtn = pool.getItem();
            }
        }
        setLast(rtn);
        return rtn;
    }

}
