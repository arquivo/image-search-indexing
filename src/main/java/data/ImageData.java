package data;

public class ImageData {
    String image_hash_key;
    String tstamp;
    String url;
    String surt;
    String mime;
    String collection;
    String content_hash;
    String bytes64string;

    public ImageData(String image_hash_key, String tstamp, String url, String surt, String mime, String collection, String content_hash, String bytes64string) {
        this.image_hash_key = image_hash_key;
        this.tstamp = tstamp;
        this.url = url;
        this.surt = surt;
        this.mime = mime;
        this.collection = collection;
        this.content_hash = content_hash;
        this.bytes64string = bytes64string;
    }
}
