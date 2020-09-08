package com.streever.iot.data.utility.generator;

import com.streever.iot.data.utility.generator.fields.FieldBase;
import com.streever.iot.data.utility.generator.fields.FieldProperties;
import com.streever.iot.data.utility.generator.fields.TerminateException;
import com.streever.iot.data.utility.generator.output.FileOutput;
import com.streever.iot.data.utility.generator.output.Output;
import com.streever.iot.data.utility.generator.output.OutputBase;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * This class will bring together the Record and Output Spec's and generator the desired output
 */
public class Builder {
    private boolean initialized = false;
    private long count = 10; // default if not specified.
    private Record record;
    // Added to the Output when it's opened.
    private String outputPrefix;

    private OutputSpec outputSpec;

    private Map<Record, Output> outputMap = new TreeMap<Record, Output>();

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public Record getRecord() {
        return record;
    }

    public void setRecord(Record record) {
        this.record = record;
    }

    public String getOutputPrefix() {
        return outputPrefix;
    }

    public void setOutputPrefix(String outputPrefix) {
        this.outputPrefix = outputPrefix;
    }

    public OutputSpec getOutputSpec() {
        if (outputSpec == null) {
            // Load Default.
            outputSpec = OutputSpec.deserialize("/default_out.yaml");
            System.out.println("Loading default output spec (System.out)");
        }
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
                    // The id is set in the linking process and is not a serialized element
                    ((FileOutput) getOutputSpec().getDefault()).setFilename(getRecord().getId());
                }
                if (getRecord().getRelationships() != null) {
                    // Start processing relationships
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
                try {
                    OutputBase spec = (OutputBase) getOutputSpec().getDefault().clone();
                    outputMap.put(record, spec);
                    spec.link(record);
                    System.out.println("Cloned 'default' spec for record: " + key);
                } catch (CloneNotSupportedException cnse) {
                    cnse.printStackTrace();
                }
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
                    System.out.println("Output has been previous used, define a separate output for:" +
                            record.getId());
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
        link(getRecord(), "main");
        boolean map = mapOutputSpecs();
        System.out.println("Map Processing Successful: " + map);
        if (!validate()) {
            throw new RuntimeException("Failed to Validate");
        }
        initialized = true;
    }

    /*
    Combine the record and output spec to ensure they are valid.
    For instance:
    - Each 'record' has an output speck
    - relationship map name aren't duplicated
     */
    protected boolean validate() {
        boolean rtn = Boolean.TRUE;
        if (this.getRecord() != null) {
            if (!this.getRecord().validate()) {
                rtn = Boolean.FALSE;
            }
        } else {
            rtn = Boolean.FALSE;
            System.err.println("Validation Issue: Set 'Record' in builder");
        }
        return rtn;
    }

    protected void write(Record record, Map<FieldProperties, Object> parentKeys) throws IOException {
        Output output = this.outputMap.get(record);
        // The last generated recordset
        output.write(record.getValueMap());
    }

    protected void openOutput() throws IOException {
        // Initialize if null;
        getOutputSpec();
        // Open specs
        Set<Record> outputKeys = outputMap.keySet();
        for (Record record : outputKeys) {
            outputMap.get(record).open(outputPrefix);
        }
    }

    protected void closeOutput() {
        Set<Record> outputKeys = outputMap.keySet();
        for (Record record : outputKeys) {
            outputMap.get(record).close();
        }
    }

    /*
    Return the number of records output. This count reflect the parent
    record count, not the cumulative children records (if defined).
     */
    public long[] run() {
        if (!initialized) {
            throw new RuntimeException("Builder was not initialized. Call init() before run().");
        }
        long lclCount[] = new long[2];
        lclCount[0] = count;
        try {
            openOutput();
            while (lclCount[0] > 0) {
                getRecord().next(null);
                write(getRecord(), null);
                lclCount[1] += writeRelationships(getRecord().getRelationships(), getRecord().getKeyMap());

                lclCount[0]--;
            }
        } catch (TerminateException te) {
            System.out.println("Terminate Exception Raised after " + (count - lclCount[0]) + " records");
        } catch (IOException ioe) {
            System.err.println("Issue Writing to file");
            ioe.printStackTrace();
        } finally {
            closeOutput();
        }
        lclCount[0] = count - lclCount[0];
        return lclCount;
    }

    protected long writeRelationships(Map<String, Relationship> relationships, Map<FieldProperties, Object> parentKeys) {
        long count = 0;
        if (relationships != null) {
            Set<String> relationshipKeys = relationships.keySet();
            for (String key : relationshipKeys) {
                Relationship relationship = relationships.get(key);
                Record rRecord = relationship.getRecord();
                try {
                    // TODO: Address Cardinality HERE!!
                    // For now, just do 1-1.
                    int range = relationship.getCardinality().getMax() - relationship.getCardinality().getMin();
                    if (range <= 0) {
                        // Assume 1-1 relationship.
                        rRecord.next(parentKeys);
                        write(rRecord, parentKeys);
                        count++;
                        // Recurse into hierarchy
                        count += writeRelationships(rRecord.getRelationships(), rRecord.getKeyMap());
                    } else {
                        // Assume min to max relationship.
                        int rNum = ThreadLocalRandom.current().nextInt(relationship.getCardinality().getMin(), relationship.getCardinality().getMax() + 1);
                        for (int i=relationship.getCardinality().getMin();i<rNum;i++) {
                            rRecord.next(parentKeys);
                            write(rRecord, parentKeys);
                            count++;
                            // Recurse into hierarchy
                            count += writeRelationships(rRecord.getRelationships(), rRecord.getKeyMap());
                        }
                    }
                } catch (TerminateException | IOException te) {

                }
            }
        }
        return count;
    }

}
