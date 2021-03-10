package com.streever.iot.data.utility.generator.output;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.streever.iot.data.utility.generator.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

//import java.io.*;
import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

@JsonIgnoreProperties({"writeStream", "fileSystem"})
public class LocalFileOutput extends OutputBase {

    private enum UniqueType {TIMESTAMP, UUID};

    public static final String HADOOP_CONF_DIR = "HADOOP_CONF_DIR";
    private static final String[] HADOOP_CONF_FILES = {"core-site.xml", "hdfs-site.xml", "mapred-site.xml", "yarn-site.xml"};

    private final String DEFAULT_TS_FORMAT = "yyyy-MM-dd HH-mm-ss";
    private DateFormat df = new SimpleDateFormat(DEFAULT_TS_FORMAT);

    private FileSystem fileSystem;

    private String filename;

    // Build sub-directories for the relationships
    private boolean dirForRelationship = true;
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

    protected OutputStream getWriteStream() {
        return writeStream;
    }

    public void link(Schema record) {
        filename = record.getId();
    }

    protected boolean createDir(String directory) throws IOException {
        boolean rtn = false;
        File dirFile = new File(directory);
        if (!dirFile.exists()) {
            System.out.println("LOCAL Filesystem: Creating directory [" + directory + "] for output");
            rtn = dirFile.mkdirs();
        } else {
            // Already exists
            rtn = true;
        }
        // TODO: handle failures to createdir
        return rtn;
    }

    @Override
    protected void writeLine(String line) throws IOException {
        //            getWriteStream().println(recLine);
        ((PrintStream) writeStream).println(line);
    }

    protected void openStream(String file) throws IOException {
        //            getWriteStream().println(recLine);
        writeStream = new PrintStream(new BufferedOutputStream(new FileOutputStream(file)), true);
    }

    /*
    Open a file for writing
     */
    public boolean open(String prefix) throws IOException {
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
                createDir(baseDir);
            } else {
                if (dirForRelationship) {
                    baseDir = getFilename();
                }
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
            ((PrintStream) writeStream).close();
            setOpen(false);
        }
        return true;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        LocalFileOutput clone = (LocalFileOutput) super.clone();
        clone.setDirForRelationship(new Boolean(this.dirForRelationship));
        if (this.filename != null)
            clone.setFilename(new String(this.filename));
        clone.setUnique(new Boolean(this.unique));
        if (this.uniqueTimestampFormat != null) {
            clone.setUniqueTimestampFormat(new String(this.uniqueTimestampFormat));
        }
        return clone;
    }

}
