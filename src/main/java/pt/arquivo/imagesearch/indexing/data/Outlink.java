package pt.arquivo.imagesearch.indexing.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;

public class Outlink implements Serializable, Writable {


    private String surt;
    private String url;
    private String anchor;

    public Outlink() {
        this.surt = null;
        this.url = null;
        this.anchor = null;
    }

    public Outlink(String surt, String url, String anchor) {
        this.surt = surt;
        this.url = url;
        this.anchor = anchor;
    }

    public String getSurt() {
        return surt;
    }

    public void setSurt(String surt) {
        this.surt = surt;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getAnchor() {
        return anchor;
    }

    public void setAnchor(String anchor) {
        this.anchor = anchor;
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Outlink outlink = (Outlink) o;
        return outlink.surt.equals(this.surt) && outlink.anchor.equals(this.anchor);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(this);
        byte[] data = baos.toByteArray();
        int size = data.length;
        out.writeInt(size);
        out.write(data);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        byte[] data = new byte[size];
        in.readFully(data);
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        ObjectInputStream ois = new ObjectInputStream(bais);
        try {
            Outlink outlink = (Outlink) ois.readObject();
            this.surt = outlink.surt;
            this.url = outlink.url;
            this.anchor = outlink.anchor;

        } catch (ClassNotFoundException e) {
            System.err.println("Error reading TextDocumentData from Writable");
        }
    }

}


