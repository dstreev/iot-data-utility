package com.streever.iot.data.utility.generator;

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
public class RecordBuilder {
    private boolean initialized = false;
    private long count = -1; // default if not specified.
    private long size = -1; // default size output limit. -1 = no check
    Integer progressIndicatorCount = 5000;
    private Schema schema;
    // Added to the Output when it's opened.
    private String outputPrefix;

    private OutputSpec outputSpec;

    private Map<Schema, Output> outputMap = new TreeMap<Schema, Output>();

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
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
            outputSpec = OutputSpec.deserialize("/standard/csv_std.yaml");
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
                getOutputSpec().getDefault().setName(getSchema().getId());
                outputMap.put(getSchema(), getOutputSpec().getDefault());
                // When filename in output spec if not set, use the 'record.id'
                // TODO: file extension
                if (getOutputSpec().getDefault() instanceof FileOutput && ((FileOutput) getOutputSpec().getDefault()).getFilename() == null) {
                    // The id is set in the linking process and is not a serialized element
                    ((FileOutput) getOutputSpec().getDefault()).setFilename(getSchema().getId());
                }
                if (getSchema().getRelationships() != null) {
                    // Start processing relationships
                    rtn = mapOutputSpecRelationships(getSchema().getRelationships(), getOutputSpec().getRelationships());
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
            Schema record = relationship.getRecord();
            OutputBase output = outputRelationships.get(key);
            if (output == null) {
                // Output Spec matching name not found.
                try {
                    OutputBase spec = (OutputBase) getOutputSpec().getDefault().clone();
                    spec.setName(key);
                    outputMap.put(record, spec);
                    spec.link(record);
                    System.out.println("Cloned 'default' spec for record: " + key);
                } catch (CloneNotSupportedException cnse) {
                    cnse.printStackTrace();
                }
            } else {
                if (!output.isUsed()) {
                    output.setName(key);
                    // When filename in output spec if not set, use the 'record.id'
                    // TODO: file extension
                    if (output instanceof FileOutput && ((FileOutput) output).getFilename() == null) {
                        ((FileOutput) output).setFilename(getSchema().getId());
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

    protected void link(Schema record, String id) {
        record.setId(id);
        if (record.getRelationships() != null) {
            Set<String> relationshipKeys = record.getRelationships().keySet();
            for (String key : relationshipKeys) {
                Relationship relationship = record.getRelationships().get(key);
                Schema rRecord = relationship.getRecord();
                rRecord.setParent(record);
                link(rRecord, key);
            }
        }
    }

    public void init() {
        link(getSchema(), "main");
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
        if (this.getSchema() != null) {
            if (!this.getSchema().validate()) {
                rtn = Boolean.FALSE;
            }
        } else {
            rtn = Boolean.FALSE;
            System.err.println("Validation Issue: Set 'Record' in builder");
        }
        return rtn;
    }

    protected long write(Schema record, Map<FieldProperties, Object> parentKeys) throws IOException {
        Output output = this.outputMap.get(record);
        // The last generated recordset
        return output.write(record.getValueMap());
    }

    protected void openOutput() throws IOException {
        // Initialize if null;
        getOutputSpec();
        // Open specs
        Set<Schema> outputKeys = outputMap.keySet();
        for (Schema record : outputKeys) {
            outputMap.get(record).open(outputPrefix);
        }
    }

    protected void closeOutput() throws IOException {
        Set<Schema> outputKeys = outputMap.keySet();
        for (Schema record : outputKeys) {
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
        // 0 for counts
        // 1 for size
        long runStatus[] = new long[2];
        long localCount = count;
        long localSize = size;
        try {
            openOutput();
            long loop = 0;
            do {
//            while (localCount > 0) {
                getSchema().next(null);
                long lsize = write(getSchema(), null);
                runStatus[1] += lsize;
                if (localSize != -1)
                    localSize -= lsize;
                runStatus[0]++;
                long[] rStatus = writeRelationships(getSchema().getRelationships(), getSchema().getKeyMap());
                runStatus[0] += rStatus[0];
                runStatus[1] += rStatus[1];
                if (localCount != -1)
                    localCount--;
                if (localSize != -1)
                    localSize -= rStatus[1];
                if (loop % progressIndicatorCount == 0) {
                    System.out.printf("[ %d, %d, %d ]%n", loop, runStatus[0], runStatus[1]);
                }
                loop++;
            } while ((localCount > 0 || localCount == -1) && (localSize > 0 || localSize == -1));
        } catch (TerminateException te) {
            System.out.println("Terminate Exception Raised after " + (runStatus[0]) + " records");
        } catch (IOException ioe) {
            System.err.println("Issue Writing to file");
            ioe.printStackTrace();
        } finally {
            try {
                closeOutput();
                System.out.printf("[ %d, %d ]%n", runStatus[0], runStatus[1]);
            } catch (IOException e) {
                // TODO: Handle ioexception
                e.printStackTrace();
            }
        }
//        runStatus[0] = count - runStatus[0];
        return runStatus;
    }

    protected long[] writeRelationships(Map<String, Relationship> relationships, Map<FieldProperties, Object> parentKeys) {
        // status[0] for counts
        // status[1] for size
        long[] status = {0,0};
        if (relationships != null) {
            Set<String> relationshipKeys = relationships.keySet();
            for (String key : relationshipKeys) {
                Relationship relationship = relationships.get(key);
                Schema rRecord = relationship.getRecord();
                try {
                    // TODO: Address Cardinality HERE!!
                    // For now, just do 1-1.
                    int range = relationship.getCardinality().getMax() - relationship.getCardinality().getMin();
                    if (range <= 0) {
                        // Assume 1-1 relationship.
                        rRecord.next(parentKeys);
                        write(rRecord, parentKeys);
                        status[0]++;
                        // Recurse into hierarchy
                        long[] rStatus;
                        rStatus = writeRelationships(rRecord.getRelationships(), rRecord.getKeyMap());
                        status[0] += rStatus[0];
                        status[1] += rStatus[1];
                    } else {
                        // Assume min to max relationship.
                        int rNum = ThreadLocalRandom.current().nextInt(relationship.getCardinality().getMin(), relationship.getCardinality().getMax() + 1);
                        for (int i=relationship.getCardinality().getMin();i<rNum;i++) {
                            rRecord.next(parentKeys);
                            status[1] += write(rRecord, parentKeys);
                            status[0]++;
                            // Recurse into hierarchy
                            long[] rStatus;
                            rStatus = writeRelationships(rRecord.getRelationships(), rRecord.getKeyMap());
                            status[0] += rStatus[0];
                            status[1] += rStatus[1];
                        }
                    }
                } catch (TerminateException | IOException te) {

                }
            }
        }
        return status;
    }

}
