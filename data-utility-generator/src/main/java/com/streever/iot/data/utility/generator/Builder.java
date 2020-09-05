package com.streever.iot.data.utility.generator;

import com.streever.iot.data.utility.generator.fields.TerminateException;
import com.streever.iot.data.utility.generator.output.FileOutput;
import com.streever.iot.data.utility.generator.output.Output;
import com.streever.iot.data.utility.generator.output.OutputBase;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * This class will bring together the Record and Output Spec's and generator the desired output
 */
public class Builder {
    private int count = 10; // default if not specified.
    private Record record;

    private OutputSpec outputSpec;

    private Map<Record, Output> outputMap = new TreeMap<Record, Output>();

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public Record getRecord() {
        return record;
    }

    public void setRecord(Record record) {
        this.record = record;
    }

    public OutputSpec getOutputSpec() {
        return outputSpec;
    }

    public void setOutputSpec(OutputSpec outputSpec) {
        this.outputSpec = outputSpec;
    }

    /*
                Using the record and output specs, build and assign the writers for each
                record.
                 */
    protected boolean mapOutputSpecs() {
        boolean rtn = Boolean.TRUE;
        if (getOutputSpec() != null) {
            if (getOutputSpec().getDefault() != null) {
                outputMap.put(getRecord(), getOutputSpec().getDefault());
                // When filename in output spec if not set, use the 'record.id'
                // TODO: file extension
                if (getOutputSpec().getDefault() instanceof FileOutput && ((FileOutput) getOutputSpec().getDefault()).getFilename() == null) {
                    ((FileOutput) getOutputSpec().getDefault()).setFilename(getRecord().getId());
                }
                if (getRecord().getRelationships() != null) {
                    rtn = mapOutputSpecRelationships(getRecord().getRelationships(), getOutputSpec().getRelationships());
                }
            } else {
                rtn = Boolean.FALSE;
            }
        } else {
            rtn = Boolean.FALSE;
        }
        return rtn;
    }

    protected boolean mapOutputSpecRelationships(Map<String, Relationship> relationships, Map<String, OutputBase> outputRelationships) {
        boolean rtn = Boolean.TRUE;
        Set<String> relationshipNames = relationships.keySet();
        for (String key : relationshipNames) {
            Relationship relationship = relationships.get(key);
            Record record = relationship.getRecord();
            OutputBase output = outputRelationships.get(key);
            if (output == null) {
                // Output Spec matching name not found.
                // TODO: Clone Default and assign.  Reset output (filename)
                //   or throw exception.
                System.out.println("Missing output spec for: " + key);
                rtn = Boolean.FALSE;
            } else {
                if (!output.isUsed()) {
                    // When filename in output spec if not set, use the 'record.id'
                    // TODO: file extension
                    if (output instanceof FileOutput && ((FileOutput) output).getFilename() == null) {
                        ((FileOutput) output).setFilename(getRecord().getId());
                    }
                    outputMap.put(record, output);
                    output.setUsed(Boolean.TRUE);
                } else {
                    System.out.println("Output has been previous used");
                    rtn = Boolean.FALSE;
                }
            }
            // If there are relationships, recurse and set them.
            if (record.getRelationships() != null) {
                if (!mapOutputSpecRelationships(record.getRelationships(), outputRelationships)) {
                    rtn = Boolean.FALSE;
                }
            }
        }
        return rtn;
    }

    protected void link(Record record, String id) {
        record.setId(id);
        if (record.getRelationships() != null) {
            Set<String> relationshipKeys = record.getRelationships().keySet();
            for (String key : relationshipKeys) {
                Relationship relationship = record.getRelationships().get(key);
                Record rRecord = relationship.getRecord();
                rRecord.setParent(record);
                link(rRecord, key);
            }
        }
    }

    public void init() {
        link(getRecord(), "default");
        boolean map = mapOutputSpecs();
        System.out.println("Map Processing Successful: " + map);
    }

    /*
    Combine the record and output spec to ensure they are valid.
    For instance:
    - Each 'record' has an output speck
    - relationship map name aren't duplicated
     */
    protected boolean validate() {

        return true;
    }

    protected void write(Record record, Map<String, Object> parentKeys) {
        Output output = this.outputMap.get(record);
        // The last generated recordset
        output.write(record.getValueMap());
    }

    public void run() {
        try {
            while (count > 0) {
                getRecord().next(null);
                write(getRecord(), null);
                writeRelationships(getRecord().getRelationships(), getRecord().getKeyMap());

                count--;
            }
        } catch (TerminateException te) {

        }
    }

    protected void writeRelationships(Map<String, Relationship> relationships, Map<String, Object> parentKeys) {
        if (relationships != null) {
            Set<String> relationshipKeys = relationships.keySet();
            for (String key : relationshipKeys) {
                Relationship relationship = relationships.get(key);
                Record rRecord = relationship.getRecord();
                try {
                    // TODO: Address Cardinality HERE!!
                    // For now, just do 1-1.
                    rRecord.next(parentKeys);
                    write(rRecord, parentKeys);
                    // Recurse into hierarchy
                    writeRelationships(rRecord.getRelationships(), rRecord.getKeyMap());
                } catch (TerminateException te) {

                }
            }
        }
    }

}
