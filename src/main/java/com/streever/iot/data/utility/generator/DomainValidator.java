package com.streever.iot.data.utility.generator;

public class DomainValidator {
    private Domain domain;
    public DomainValidator() {
    }

    public Domain getDomain() {
        return domain;
    }

    public void setDomain(Domain domain) {
        this.domain = domain;
    }

    public Boolean validate() {
        Boolean rtn = Boolean.TRUE;

        return rtn;
    }
}
