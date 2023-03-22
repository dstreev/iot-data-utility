package com.streever.iot.data.utility.generator;

public interface SqlBuilder {
    String build();
    void setDomain(Domain domain);
    Domain getDomain();
}
