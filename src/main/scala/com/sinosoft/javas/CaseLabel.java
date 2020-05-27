package com.sinosoft.javas;

import org.neo4j.graphdb.Label;

class CaseLabel implements Label {
    private String name;
    public CaseLabel(String name) {
        this.name = name;
    }

    public String name(){
        return name;
    }
}
