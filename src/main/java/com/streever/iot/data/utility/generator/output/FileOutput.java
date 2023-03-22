package com.streever.iot.data.utility.generator.output;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.streever.iot.data.utility.generator.Schema;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

@JsonIgnoreProperties({"writeStream", "fileSystem"})
public abstract class FileOutput extends OutputBase {

    protected enum UniqueType {TIMESTAMP, UUID};


    /*
    The format should NOT contain spaces to avoid filenames that won't be compatible on some
    filesystems.
     */
    protected final String DEFAULT_TS_FORMAT = "yyyy-MM-dd_HH-mm-ss";
    protected DateFormat df = new SimpleDateFormat(DEFAULT_TS_FORMAT);

    private String filename;

    // Control if output files are unique.
    private boolean unique = false;
    // Control unique TS format for output files.
    private String uniqueTimestampFormat = DEFAULT_TS_FORMAT;
    private UniqueType uniqueType = UniqueType.TIMESTAMP;
    private OutputStream writeStream;

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
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

    public void setWriteStream(OutputStream writeStream) {
        this.writeStream = writeStream;
    }

    protected OutputStream getWriteStream() {
        return writeStream;
    }

    public void link(Schema record) {
        filename = record.getId();
    }

    protected abstract boolean createDir(String directory) throws IOException;

    @Override
    protected abstract void writeLine(String line) throws IOException;

    protected abstract void openStream(String file) throws IOException;
    protected abstract void closeStream() throws IOException;
    /*
    Open a file for writing
     */
    public boolean open(String prefix) throws IOException {
        try {
            String file;
            String baseDir = null;
            String adjustedFilename = getFilename();
            if (isUnique()) {
                switch (getUniqueType()) {
                    case TIMESTAMP:
                        adjustedFilename = adjustedFilename + "_" + df.format(new Date());
                        break;
                    case UUID:
                        adjustedFilename = adjustedFilename + "_" + UUID.randomUUID();
                        break;
                }
            }
            if (prefix != null) {
//                if (isDirForRelationship()) {
//                    baseDir = prefix + System.getProperty("file.separator") + getName();
//                } else {
                    baseDir = prefix;
//                }
                createDir(baseDir);
            } else {
//                if (isDirForRelationship()) {
                    baseDir = getName();
//                }
            }
            if (baseDir != null) {
                createDir(baseDir);
                file = baseDir + System.getProperty("file.separator") + adjustedFilename + "." + getFormat().getExtension();
            } else {
                file = adjustedFilename + "." + getFormat().getExtension();
            }
            String fullFile = file;
            openStream(fullFile);
//            writeStream = new PrintStream(new BufferedOutputStream(new FileOutputStream(file)), true);
            setOpen(true);
        } catch (FileNotFoundException fnfe) {
            return false;
        }
        return true;
    }

    public boolean close() throws IOException {
        if (isOpen()) {
            //            getWriteStream().println(recLine);
            closeStream();
            setOpen(false);
        }
        return true;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        LocalFileOutput clone = (LocalFileOutput) super.clone();
//        clone.setDirForRelationship(new Boolean(isDirForRelationship()));
        if (getFilename() != null)
            clone.setFilename(new String(getFilename()));
        clone.setUnique(new Boolean(this.unique));
        if (getUniqueTimestampFormat() != null) {
            clone.setUniqueTimestampFormat(new String(getUniqueTimestampFormat()));
        }
        return clone;
    }

}
