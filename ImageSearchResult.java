package hadoopImageParser;
import java.math.BigDecimal;


public class ImageSearchResult implements Comparable< ImageSearchResult > {
	
	String url;
	String width;
	String height;
	String alt;
	String title;
	String urlOriginal;
	String digest;
    String timestamp;
	String mime;
	String thumbnail;
	String longdesc;
	String imgBase64;
	BigDecimal nsfw;
	String pageTitle;
    public ImageSearchResult( ) { }
    
	public ImageSearchResult( String url, String width, String height, String alt, String title, String urlOriginal, String timestamp, String longdesc , String pageTitle ){
        this.url 			= url;
        this.width 			= width;
        this.height 		= height;
        this.alt 			= alt;
        this.title 			= title;
        this.urlOriginal 	= urlOriginal;
        this.timestamp 		= timestamp;
        this.longdesc		= longdesc;
        this.pageTitle		= pageTitle;
	}

	
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getWidth() {
		return width;
	}
	public void setWidth(String width) {
		this.width = width;
	}
	public String getHeight() {
		return height;
	}
	public void setHeight(String height) {
		this.height = height;
	}
	public String getAlt() {
		return alt;
	}
	public void setAlt(String alt) {
		this.alt = alt;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
    public String getTimestamp(){
        return timestamp;
    }
    public void setDigest(String digest) {
		this.digest = digest;
	}
	public String getDigest() {
		return digest;
	}
	public String getUrlOriginal() {
		return urlOriginal;
	}
	public void setUrlOriginal(String urlOriginal) {
		this.urlOriginal = urlOriginal;
	}
	public String getMime( ) {
		return mime;
	}
	public void setMime( String mime ) {
		this.mime = mime;
	}
	public String getThumbnail() {
		return thumbnail;
	}
	public void setThumbnail(String thumbnail) {
		this.thumbnail = thumbnail;
	}
	public String getLongdesc() {
		return longdesc;
	}
	public void setLongdesc(String longdesc) {
		this.longdesc = longdesc;
	}
	public BigDecimal getNsfw() {
		return nsfw;
	}
	public void setNsfw(BigDecimal nsfw) {
		this.nsfw = nsfw;
	}
	public String getImgBase64() {
		return imgBase64;
	}
	public void setImgBase64(String imgBase64) {
		this.imgBase64 = imgBase64;
	}
	public String getPageTitle() {
		return pageTitle;
	}
	public void setPageTitle(String pageTitle) {
		this.pageTitle = pageTitle;
	}

	@Override
	public boolean equals( Object o ) {
		
		if ( o == this ) {
            return true;
        }
        if ( !( o instanceof ImageSearchResult ) ) {
            return false;
        }
        ImageSearchResult other = ( ImageSearchResult ) o;
        /*if( this.digest.equals( other.digest ) )
        	System.out.println( " Equal this["+this.getDigest()+"]["+this.getUrl()+"] other["+other.getDigest()+"]["+other.getUrl()+"]\n\n" );*/
        return this.digest.equals( other.digest );
        
	}
	
	private long ConvertTimstampToLong(  ) {
		return Long.parseLong( this.getTimestamp( ) );
	}
	
	@Override
	public int hashCode() {
	     return this.digest.hashCode( );
	}
	
	@Override
	public int compareTo( ImageSearchResult another ) {
		return 1;
	}
		
}