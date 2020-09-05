package com.streever.iot.data.utility.generator.output;

import com.streever.iot.data.utility.generator.Record;

import java.io.*;

public abstract class FileOutput extends OutputBase {
    private String filename;
    private PrintStream writeStream;

    protected abstract String getExtension();

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
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
            if (prefix != null) {
                File prefixFile = new File(prefix);
                if (!prefixFile.exists()) {
                    prefixFile.mkdirs();
                }
                file = prefix + System.getProperty("file.separator") + getFilename() + "." + getExtension();
            } else {
                file = getFilename() + "." + getExtension();
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
