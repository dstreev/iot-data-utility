package com.streever.iot.data.utility.generator.fields;

/**
 * When identified, the control field is used to throw an
 * exception when the fields range.max element is reached.
 *
 * This exception can be caught by the application to signal
 * the end of processing.
 *
 * By design, this was used on the Date Field to stop processing
 * when the range max was reached. See the sample data config
 * 'date-terminate.yaml'.
 *
 */
public interface ControlField {
    boolean terminate();
    boolean isControlField();
    void setControlField(Boolean controlField);
}
