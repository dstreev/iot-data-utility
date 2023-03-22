package com.streever.iot.data.utility.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.streever.iot.data.utility.generator.fields.TerminateException;
import com.streever.iot.data.utility.generator.output.LocalFileOutput;
import com.streever.iot.data.utility.generator.output.Output;
import com.streever.iot.data.utility.generator.output.OutputBase;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * This class will bring together the Record and Output Spec's and generator the desired output
 */
public class DomainBuilder {
    private static Logger LOG = LogManager.getLogger(DomainBuilder.class);
    private boolean initialized = false;
    private long count = -1; // default if not specified.
    private long size = -1; // default size output limit. -1 = no check
    Integer progressIndicatorCount = 5000;
    private Domain domain;
    private Schema anchorSchema;

    // Added to the Output when it's opened.
    private String outputPrefix;

    private OutputConfig outputConfig;

    private Random randomizer = null;

    private String terminationReason = null;
    private ObjectMapper mapper = new ObjectMapper();

    public String getTerminationReason() {
        return terminationReason;
    }

    public void setTerminationReason(String terminationReason) {
        this.terminationReason = terminationReason;
    }

//    private Map<String, Output> outputMap = new TreeMap<String, Output>();

    public DomainBuilder() {

    }

    public DomainBuilder(Domain domain, String anchorSchema) {
        this.domain = domain;
        if (domain.getSchemas().size() == 1) {
            this.anchorSchema = domain.getSchemas().get(0);
        } else {
            if (anchorSchema != null) {
                for (Schema schema : domain.getSchemas()) {
                    if (schema.getTitle().equals(anchorSchema)) {
                        this.anchorSchema = schema;
                    }
                }
            } else {
                throw new RuntimeException("You need to specify a schema to write with `-s`");
            }
            if (this.anchorSchema == null) {
                throw new RuntimeException("Couldn't locate schema.title: " + anchorSchema + " in domain.");
            }
        }
    }

    public Random getRandomizer() {
        if (randomizer == null) {
            randomizer = new Random(new Date().getTime());
        }
        return randomizer;
    }

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

    public Domain getDomain() {
        return domain;
    }

    public void setDomain(Domain domain) {
        this.domain = domain;
    }

    public String getOutputPrefix() {
        return outputPrefix;
    }

    public void setOutputPrefix(String outputPrefix) {
        this.outputPrefix = outputPrefix;
    }

    public OutputConfig getOutputConfig() {
        if (outputConfig == null) {
            // Load Default.
            outputConfig = OutputConfig.deserialize("/standard/json_std.yaml");
            System.out.println("Loading default output spec (System.out)");
        }
        return outputConfig;
    }

    public void setOutputConfig(OutputConfig outputConfig) {
        this.outputConfig = outputConfig;
    }

    /*
    Using the record and output specs, build and assign the writers for each
    record.
    */
//    protected boolean mapOutputSpecs() {
//        boolean rtn = Boolean.TRUE;
//        if (getOutputConfig() != null) {
//            if (getOutputConfig().getDefault() != null) {
//                getOutputConfig().getDefault().setName(getSchema().getId());
//                outputMap.put(getSchema().getTitle(), getOutputConfig().getDefault());
//                // When filename in output spec if not set, use the 'record.id'
//                // TODO: file extension
//                if (getOutputConfig().getDefault() instanceof LocalFileOutput && ((LocalFileOutput) getOutputConfig().getDefault()).getFilename() == null) {
//                    // The id is set in the linking process and is not a serialized element
//                    ((LocalFileOutput) getOutputConfig().getDefault()).setFilename(getSchema().getId());
//                }
//                if (getOutputConfig().getDefault() instanceof DFSOutput && ((DFSOutput) getOutputConfig().getDefault()).getFilename() == null) {
//                    // The id is set in the linking process and is not a serialized element
//                    ((DFSOutput) getOutputConfig().getDefault()).setFilename(getSchema().getId());
//                }
////                if (getSchema().getRelationships() != null) {
////                    // Start processing relationships
////                    rtn = mapOutputSpecRelationships(getSchema().getRelationships(), getOutputConfig().getRelationships());
////                }
//            } else {
//                rtn = Boolean.FALSE;
//            }
//        } else {
//            rtn = Boolean.FALSE;
//        }
//        return rtn;
//    }

//    protected boolean mapOutputSpecRelationships(Map<String, Relationship> relationships, Map<String, OutputBase> outputRelationships) {
//        boolean rtn = Boolean.TRUE;
//        Set<String> relationshipNames = relationships.keySet();
//        for (String key : relationshipNames) {
//            Relationship relationship = relationships.get(key);
////            Schema record = relationship.getRecord();
//            for (int i = 1; i <= relationship.getCardinality().getRepeat(); i++) {
//                String keyPos = null;
//                if (relationship.getCardinality().getRepeat() == 1) {
//                    keyPos = key;
//                } else {
//                    keyPos = key + "_" + StringUtils.leftPad(Integer.toString(i), 4, "0");
//                }
//                OutputBase output = outputRelationships.get(keyPos);
//                if (output == null) {
//                    // Output Spec matching name not found.
//                    try {
//                        OutputBase spec = (OutputBase) getOutputConfig().getConfig().clone();
//                        if (spec instanceof LocalFileOutput) {
//                            ((LocalFileOutput) spec).setName(keyPos);
//                            ((LocalFileOutput) spec).setFilename(keyPos);
//                        }
//                        spec.setName(keyPos);
//                        outputMap.put(keyPos, spec);
////                        spec.link(record);
////                        System.out.println("Cloned 'default' spec for record: " + keyPos);
//                    } catch (CloneNotSupportedException cnse) {
//                        cnse.printStackTrace();
//                    }
//                } else {
//                    if (!output.isUsed()) {
//                        output.setName(keyPos);
//                        // When filename in output spec if not set, use the 'record.id'
//                        // TODO: file extension
//                        if (output instanceof LocalFileOutput && ((LocalFileOutput) output).getFilename() == null) {
//                            ((LocalFileOutput) output).setFilename(keyPos);
//                        }
//                        outputMap.put(keyPos, output);
//                        output.setUsed(Boolean.TRUE);
//                    } else {
////                        System.out.println("Output has been previous used, define a separate output for:" +
////                                record.getId());
//                        rtn = Boolean.FALSE;
//                    }
//                }
//            }
//            // If there are relationships, recurse and set them.
////            if (record.getRelationships() != null) {
////                if (!mapOutputSpecRelationships(record.getRelationships(), outputRelationships)) {
////                    rtn = Boolean.FALSE;
////                }
////            }
//        }
//        return rtn;
//    }

    public void init() {
        // Part of the initialization requires us to sweep through the schema and
        // link the parts of the schema together (hierarchy) so we can build complex
        // output streams with dependencies.
//        getSchema().link();
        // Link the parts of the output with the schema so we know where to
        // send records when they are produced.
//        boolean map = mapOutputSpecs();
//        System.out.println("Map Processing Successful: " + map);
        // Ensure the associations are all valid.
        DomainValidator dv = new DomainValidator();
        dv.setDomain(getDomain());
        if (!dv.validate()) {
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
//    protected boolean validate() {
//        boolean rtn = Boolean.TRUE;
//        if (this.getDomain() != null) {
//            if (!this.getDomain().validate()) {
//                rtn = Boolean.FALSE;
//            }
//        } else {
//            rtn = Boolean.FALSE;
//            System.err.println("Validation Issue: Set 'Record' in builder");
//        }
//        return rtn;
//    }

    /*
    Open the output spec IO channel(s) for writing.
     */
    protected void openOutput() throws IOException {
        // Initialize if null;
        OutputConfig oc = getOutputConfig();
        oc.getConfig().open(outputPrefix);
//        // Open specs
//        Set<String> outputKeys = outputMap.keySet();
//        for (String recordKey : outputKeys) {
//            outputMap.get(recordKey).open(outputPrefix);
//        }
    }

    /*
    Clean up after we're done by closing the IO channels
     */
    protected void closeOutput() throws IOException {
        OutputConfig oc = getOutputConfig();
        oc.getConfig().close();
    }

    /*
    Write a record, based on the schema, to the proper output path.
     */
    protected long write(ObjectNode node) throws IOException {
        OutputConfig oc = getOutputConfig();
        long length = oc.getConfig().write(node);
//        return output.write(record.getValueMap());
        return length;
    }

    public ObjectNode getRecord() throws TerminateException {
        ObjectNode schemaNode = mapper.createObjectNode();

        anchorSchema.getRecordNode(schemaNode);
        for (String relationshipStr : anchorSchema.getRelationships().keySet()) {
            Relationship relationship = anchorSchema.getRelationships().get(relationshipStr);
            final ArrayNode raNode = schemaNode.putArray(relationshipStr);
            if (relationship.getCardinality() != null) {
                int diff = relationship.getCardinality().getMax() - relationship.getCardinality().getMin();
                if (diff > 0) {
                    // Add as 0:n
                    int rCount = relationship.getCardinality().getMin() + getRandomizer().nextInt(diff);
                    for (int i = 0; i < rCount; i++) {
                        ObjectNode rNode = mapper.createObjectNode();
                        relationship.getToSchema().getRecordNode(rNode);
                        raNode.add(rNode);
                    }
                }
            } else {
                // Add as 1:1
                ObjectNode rNode = mapper.createObjectNode();
                relationship.getToSchema().getRecordNode(rNode);
                raNode.add(rNode);
            }
        }
        return schemaNode;
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
            // Run the loop until the counts/sizes are met.  If the count/size has been set to -1, then run indefinitely.
            do {
                try {
                    long writeSize = 0l;
                    ObjectNode schemaNode = getRecord();

                    writeSize = write(schemaNode);

                    runStatus[1] += writeSize;
                    if (localSize != -1) {
                        localSize -= writeSize;
                        if (localSize <= 0) {
                            setTerminationReason("Reach end of requested 'size'.");
                        }
                    }
                    // Increment the Run Count.
                    runStatus[0]++;

                    // If the local's.. weren't set to -1, decrement them
                    if (localCount != -1) {
                        localCount--;
                        if (localCount <= 0) {
                            setTerminationReason("Reach end of requested 'count'.");
                        }
                    }

                    if (loop % progressIndicatorCount == 0) {
                        System.out.printf("[ %d, %d, %d ]%n", loop, runStatus[0], runStatus[1]);
                    }
                } catch (TerminateException earlyTerminationException) {
                    // Early Termination due to a threshold
                    setTerminationReason("Early Termination: " + earlyTerminationException.getMessage());
                    localCount = 0;
                    localSize = 0;
                }
                loop++;
            } while ((localCount > 0 || localCount == -1) && (localSize > 0 || localSize == -1));
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
        return runStatus;
    }

}
