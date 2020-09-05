package com.streever.iot.data.utility.generator;

import java.util.List;

public class Cardinality {

    private int factor = 1;
    // Future items.  Something to randomized factors greater than 1.

    /*
    Field in the parent record that needs to be carried into this relationship.

    HOLD ON THIS.  Let's try picking up the "keyFields" from the parent.

     */
//    private List<String> referenceFields;

    public int getFactor() {
        return factor;
    }

    public void setFactor(int factor) {
        this.factor = factor;
    }

//    public List<String> getReferenceFields() {
//        return referenceFields;
//    }
//
//    public void setReferenceFields(List<String> referenceFields) {
//        this.referenceFields = referenceFields;
//    }
}
