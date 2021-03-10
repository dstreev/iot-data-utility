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
public class LocalFileOutput extends FileOutput {

    @Override
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
        ((PrintStream) getWriteStream()).println(line);
    }

    @Override
    protected void openStream(String file) throws IOException {
        //            getWriteStream().println(recLine);
        setWriteStream(new PrintStream(new BufferedOutputStream(new FileOutputStream(file)), true));
    }

    @Override
    protected void closeStream() throws IOException {
        ((PrintStream) getWriteStream()).close();
    }





}
