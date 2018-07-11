/*Contains information about an image obtained from the CreateImageDB hadoop map*/
public class ImageSQLDTO{
	private byte[] content;
	private String mimeType;
	private String tstamp;
	
	public ImageSQLDTO(byte[] content, String mimeType, String tstamp){
		this.content = content;
		this.mimeType = mimeType;
		this.tstamp = tstamp;
	}
	public byte[] getContent(){
		return content;
	}
	public String getMimeType(){
		return mimeType;
	}
	public String getTstamp(){
		return tstamp;
	}
}