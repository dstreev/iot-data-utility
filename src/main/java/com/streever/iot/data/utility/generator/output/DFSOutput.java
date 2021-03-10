package com.streever.iot.data.utility.generator.output;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;

@JsonIgnoreProperties({"writeStream", "fileSystem"})
public class DFSOutput extends FileOutput {

    public static final String HADOOP_CONF_DIR = "HADOOP_CONF_DIR";
    private static final String[] HADOOP_CONF_FILES = {"core-site.xml", "hdfs-site.xml", "mapred-site.xml", "yarn-site.xml"};

    private FileSystem fileSystem;

    public void setFileSystem(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    @Override
    protected boolean createDir(String directory) throws IOException {
        boolean rtn = false;
        // Relative Directories will be calculated from the users hdfs home directory.
        FileSystem fs = getFileSystem();
        Path dirPath = new Path(directory);
        if (!fs.exists(dirPath)) {
            rtn = fs.mkdirs(dirPath);
        } else {
            // Already exists
            rtn = true;
        }
        // TODO: handle failures to createdir
        return rtn;
    }

    @Override
    protected void writeLine(String line) throws IOException {
        byte buffer[] = new byte[256];
        // Add newline
        String newLine = line + "\n";
        int bytesRead = newLine.length();

//                    while ((bytesRead = in.read(buffer)) > 0) {
        ((FSDataOutputStream) getWriteStream()).write(newLine.getBytes(), 0, bytesRead);
//                    }
//                } catch (IOException e) {
//                    System.out.println("Error while copying file");
//                } finally {
//                    in.close();
//                    out.close();
//                }

    }

    protected void openStream(String file) throws IOException {
        FileSystem fs = getFileSystem();
        Path writeFile = new Path(file);
        if (fs.exists(writeFile)) {
            System.out.println("Output file [" + file + "] already exists");
            throw new IOException("Output file [" + file + "] already exists");
        } else {
            setWriteStream(fs.create(writeFile));
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

    protected void closeStream() throws IOException {
        ((FSDataOutputStream) getWriteStream()).close();
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

    public boolean close() throws IOException {
        if (isOpen()) {
            closeStream();
            setOpen(false);
        }
        return true;
    }

}
