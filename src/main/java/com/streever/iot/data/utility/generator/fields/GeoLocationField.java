package com.streever.iot.data.utility.generator.fields;

import com.streever.iot.data.utility.generator.fields.support.GeoCircle;
import com.streever.iot.data.utility.generator.fields.support.GeoLocation;
import com.streever.iot.data.utility.generator.fields.support.Pool;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Date;

public class GeoLocationField extends FieldBase<GeoLocation> {

    private GeoCircle center;
    private Pool<GeoLocation> pool;

    public GeoCircle getCenter() {
        return center;
    }

    public void setCenter(GeoCircle center) {
        this.center = center;
    }

    public Pool<GeoLocation> getPool() {
        return pool;
    }

    public void setPool(Pool<GeoLocation> pool) {
        this.pool = pool;
    }

    private void buildPool() {
        if (pool.getInitialized() != Boolean.TRUE) {
            for (int i = 0; i < pool.getSize(); i++) {
                pool.getItems().add(newValue());
            }
            pool.setInitialized(Boolean.TRUE);
        }
    }

    // TODO: Build Random GeoLocation from GeoCenter
    protected GeoLocation newValue() {
        throw new NotImplementedException();
//        return null;
    }

    @Override
    public GeoLocation getNext() {
        GeoLocation rtn = null;
        if (!isMaintainState()) {
            if (pool == null) {
                rtn = newValue();
            } else {
                if (pool != null && pool.getInitialized() == Boolean.FALSE) {
                    buildPool();
                }
                if (pool != null) {
                    rtn = pool.getItem();
                }
            }
        } else {
            stateValues.clear();
            rtn = newValue();
            stateValues.put("lat", rtn.getLatitude());
            stateValues.put("long", rtn.getLongitude());
        }
        setLast(rtn);
        return rtn;
    }
}
