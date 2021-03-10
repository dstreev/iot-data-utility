package com.streever.iot.data.utility.generator.fields;

import com.streever.iot.data.utility.generator.fields.support.Pool;
import com.streever.iot.data.utility.generator.fields.support.Range;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.UUID;

public class UUIDField extends FieldBase<String> {
    private Pool<String> pool;

    public Pool<String> getPool() {
        return pool;
    }

    public void setPool(Pool<String> pool) {
        this.pool = pool;
    }

    protected void buildPool() {
        if (pool != null) {
            for (int i = 0; i < pool.getSize(); i++) {
                UUID uuid = UUID.randomUUID();
                pool.getItems().add(uuid.toString());
            }
            pool.setInitialized(true);
        }
    }


    @Override
    public String getNext() {
        String rtn = null; // return uuid.toString();

        if (pool == null) {
            UUID uuid = UUID.randomUUID();
            rtn = uuid.toString();
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
