package com.streever.iot.data.utility.generator.fields;

import com.streever.iot.data.utility.generator.fields.support.Pool;

import java.io.*;

public abstract class ReferenceField<T> extends FieldBase<T> {

    private String file;
    private String delimiter;
    private int position = 0;

    protected Pool<T> pool;

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    protected abstract Pool<T> getPool();
//    public String getResource() {
//        return resource;
//    }
//
//    public void setResource(String resource) {
//        this.resource = resource;
//        this.file = null;
//    }

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
//        this.resource = null;
    }

    protected void buildPool() {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(file));
        } catch (FileNotFoundException fnfe) {
            InputStream in = this.getClass().getResourceAsStream(file);
            if (in == null)
                throw new RuntimeException("File not found: " + file);
            br = new BufferedReader(new InputStreamReader(in));
        }
        String line;
        try {
            while ((line = br.readLine()) != null) {
                // process the line.
                if (delimiter != null) {
                    String[] fields = line.split(delimiter);
                    getPool().getItems().add((T)fields[position]);
                } else {          
                    getPool().getItems().add((T) line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
        getPool().setInitialized(true);

    }

    @Override
    public T getNext() {
        T rtn = null;
        if (pool == null) {
            buildPool();
        }
        if (pool != null) {
            rtn = pool.getItem();
        }
        setLast(rtn);
        return rtn;
    }

}
