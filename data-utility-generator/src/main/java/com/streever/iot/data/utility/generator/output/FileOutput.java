package com.streever.iot.data.utility.generator.output;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.streever.iot.data.utility.generator.Record;

import java.io.*;
import java.text.DateFormat;
import java.text.FieldPosition;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

@JsonIgnoreProperties({"writeStream"})
public abstract class FileOutput extends OutputBase {
    private final String DEFAULT_TS_FORMAT = "yyyy-MM-dd HH-mm-ss";
    private DateFormat df = new SimpleDateFormat(DEFAULT_TS_FORMAT);
    private enum UniqueType { TIMESTAMP, UUID };
    private String filename;
    // Build sub-directories for the relationships
    private boolean dirForRelationship = true;
    // Control if output files are unique.
    private boolean unique = false;
    // Control unique TS format for output files.
    private String uniqueTimestampFormat = DEFAULT_TS_FORMAT;
    private UniqueType uniqueType = UniqueType.TIMESTAMP;
    private PrintStream writeStream;

    protected abstract String getExtension();

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public boolean isDirForRelationship() {
        return dirForRelationship;
    }

    public void setDirForRelationship(boolean dirForRelationship) {
        this.dirForRelationship = dirForRelationship;
    }

    public boolean isUnique() {
        return unique;
    }

    public void setUnique(boolean unique) {
        this.unique = unique;
    }

    public UniqueType getUniqueType() {
        return uniqueType;
    }

    public void setUniqueType(UniqueType uniqueType) {
        this.uniqueType = uniqueType;
    }

    public String getUniqueTimestampFormat() {
        return uniqueTimestampFormat;
    }

    public void setUniqueTimestampFormat(String uniqueTimestampFormat) {
        this.uniqueTimestampFormat = uniqueTimestampFormat;
        df = new SimpleDateFormat(this.uniqueTimestampFormat);
    }

    protected PrintStream getWriteStream() {
        return writeStream;
    }

    public void link(Record record) {
        filename = record.getId();
    }

    /*
    Open a file for writing
     */
    public boolean open(String prefix) {
        try {
            String file;
            String baseDir = null;
            String adjustedFilename = getFilename();
            if (unique) {
                switch (uniqueType) {
                    case TIMESTAMP:
                        adjustedFilename = adjustedFilename + "_" + df.format(new Date());
                        break;
                    case UUID:
                        adjustedFilename = adjustedFilename + "_" + UUID.randomUUID();
                        break;
                }
            }
            if (prefix != null) {
                if (dirForRelationship) {
                    baseDir = prefix + System.getProperty("file.separator") + getFilename();
                } else {
                    baseDir = prefix;
                }
                File prefixFile = new File(baseDir);

                if (!prefixFile.exists()) {
                    System.out.println("Creating directory [" + prefixFile + "] for output");
                    prefixFile.mkdirs();
                }
            } else {
                if (dirForRelationship) {
                    baseDir = getFilename();
                }
            }
            if (baseDir != null) {
                File baseDirFile = new File(baseDir);

                if (!baseDirFile.exists()) {
                    System.out.println("Creating directory [" + baseDirFile + "] for output");
                    baseDirFile.mkdirs();
                }

                file = baseDir + System.getProperty("file.separator") + adjustedFilename + "." + getExtension();
            } else {
                file = adjustedFilename + "." + getExtension();
            }
            writeStream = new PrintStream(new BufferedOutputStream(new FileOutputStream(file)), true);
            setOpen(true);
        } catch (FileNotFoundException fnfe) {
            return false;
        }
        return true;
    }

    public boolean close() {
        writeStream.close();
        setOpen(false);
        return true;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

}
