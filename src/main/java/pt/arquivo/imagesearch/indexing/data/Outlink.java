package pt.arquivo.imagesearch.indexing.data;

import java.io.Serializable;

public class Outlink implements Serializable {


    private String surt;
    private String url;
    private String anchor;

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

}

