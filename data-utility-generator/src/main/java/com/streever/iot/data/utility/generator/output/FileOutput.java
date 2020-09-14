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
public class FileOutput extends OutputBase {

    public static final String HADOOP_CONF_DIR = "HADOOP_CONF_DIR";
    private static final String[] HADOOP_CONF_FILES = {"core-site.xml", "hdfs-site.xml", "mapred-site.xml", "yarn-site.xml"};

    private final String DEFAULT_TS_FORMAT = "yyyy-MM-dd HH-mm-ss";
    private DateFormat df = new SimpleDateFormat(DEFAULT_TS_FORMAT);

    private FileSystem fileSystem;

    private enum UniqueType {TIMESTAMP, UUID}

    ;

    public enum TargetFilesystem {
        /*
        Local Filesystem
         */
        LOCAL,
        /*
        Hadoop Compatible File System
         */
        HCFS
    }

    ;
    private String filename;
    // Build sub-directories for the relationships
    private TargetFilesystem targetFilesystem = TargetFilesystem.LOCAL;
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

    public void setFileSystem(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    public TargetFilesystem getTargetFilesystem() {
        return targetFilesystem;
    }

    public void setTargetFilesystem(TargetFilesystem targetFilesystem) {
        this.targetFilesystem = targetFilesystem;
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
        switch (targetFilesystem) {
            case LOCAL:
                File dirFile = new File(directory);
                if (!dirFile.exists()) {
                    System.out.println("LOCAL Filesystem: Creating directory [" + directory + "] for output");
                    rtn = dirFile.mkdirs();
                } else {
                    // Already exists
                    rtn = true;
                }
                break;
            case HCFS:
                // Relative Directories will be calculated from the users hdfs home directory.
                FileSystem fs = getFileSystem();
                Path dirPath = new Path(directory);
                if (!fs.exists(dirPath)) {
                    rtn = fs.mkdirs(dirPath);
                } else {
                    // Already exists
                    rtn = true;
                }
                break;
        }
        // TODO: handle failures to createdir
        return rtn;
    }

    @Override
    protected void writeLine(String line) throws IOException {
        switch (targetFilesystem) {
            case LOCAL:
                //            getWriteStream().println(recLine);
                ((PrintStream) writeStream).println(line);
                break;
            case HCFS:
                byte buffer[] = new byte[256];
                // Add newline
                String newLine = line + "\n";
                int bytesRead = newLine.length();

//                    while ((bytesRead = in.read(buffer)) > 0) {
                ((FSDataOutputStream) writeStream).write(newLine.getBytes(), 0, bytesRead);
//                    }
//                } catch (IOException e) {
//                    System.out.println("Error while copying file");
//                } finally {
//                    in.close();
//                    out.close();
//                }

                break;
        }
    }

    protected void openStream(String file) throws IOException {
        switch (targetFilesystem) {
            case LOCAL:
                //            getWriteStream().println(recLine);
                writeStream = new PrintStream(new BufferedOutputStream(new FileOutputStream(file)), true);
                break;
            case HCFS:
                FileSystem fs = getFileSystem();
                Path writeFile = new Path(file);
                if (fs.exists(writeFile)) {
                    System.out.println("Output file [" + file + "] already exists");
                    throw new IOException("Output file [" + file + "] already exists");
                } else {
                    writeStream = fs.create(writeFile);
                }
                break;
        }
                /*
                 try {
      FileSystem fs = FileSystem.get(conf);
      // Hadoop DFS Path - Input & Output file
      Path inFile = new Path(args[0]);
      Path outFile = new Path(args[1]);
      // Verification
      if (!fs.exists(inFile)) {
        System.out.println("Input file not found");
        throw new IOException("Input file not found");
      }
      if (fs.exists(outFile)) {
        System.out.println("Output file already exists");
        throw new IOException("Output file already exists");
      }

      // open and read from file
      FSDataInputStream in = fs.open(inFile);
      // Create file to write
      FSDataOutputStream out = fs.create(outFile);

      byte buffer[] = new byte[256];
      try {
        int bytesRead = 0;
        while ((bytesRead = in.read(buffer)) > 0) {
          out.write(buffer, 0, bytesRead);
          }
      } catch (IOException e) {
        System.out.println("Error while copying file");
      } finally {
        in.close();
        out.close();
      }
                 */

    }

    protected FileSystem getFileSystem() throws IOException {
        if (fileSystem == null) {
            // Get a value that over rides the default, if nothing then use default.
            String hadoopConfDirProp = System.getenv().getOrDefault(HADOOP_CONF_DIR, "/etc/hadoop/conf");

            // Set a default
            if (hadoopConfDirProp == null)
                hadoopConfDirProp = "/etc/hadoop/conf";

            System.out.println("Using '" + hadoopConfDirProp + "' for HDFS Configurations");

            Configuration config = new Configuration(true);

            File hadoopConfDir = new File(hadoopConfDirProp).getAbsoluteFile();
            for (String file : HADOOP_CONF_FILES) {
                File f = new File(hadoopConfDir, file);
                if (f.exists()) {
                    System.out.println("Found '" + file + "' in conf directory.  Added as configuration resource.");
                    config.addResource(new Path(f.getAbsolutePath()));
                }
            }

            // hadoop.security.authentication
            if (config.get("hadoop.security.authentication", "simple").equalsIgnoreCase("kerberos")) {
                UserGroupInformation.setConfiguration(config);
                System.out.println("Kerberos Connection.  User: [" + UserGroupInformation.getCurrentUser().getShortUserName() + "]");
            }
            try {
                fileSystem = FileSystem.get(config);
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
        return fileSystem;
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
            switch (targetFilesystem) {
                case LOCAL:
                    //            getWriteStream().println(recLine);
                    ((PrintStream) writeStream).close();
                    break;
                case HCFS:
                    ((FSDataOutputStream) writeStream).close();
                    break;
            }
            setOpen(false);
        }
        return true;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

}
