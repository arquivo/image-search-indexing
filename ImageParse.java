package hadoopImageParser;

import java.awt.Graphics2D;
import java.awt.GraphicsConfiguration;
import java.awt.GraphicsDevice;
import java.awt.GraphicsEnvironment;
import java.awt.Image;
import java.awt.geom.AffineTransform;
import java.awt.image.BufferedImage;
import java.awt.image.ImagingOpException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.metadata.IIOMetadata;
import javax.imageio.stream.ImageInputStream;
import javax.imageio.stream.ImageOutputStream;

import org.apache.commons.io.IOUtils;


public class ImageParse {
	
	public ImageParse( ) { }

	/**
	 * 
	 * @param img
	 * @param widthThumbnail 
	 * @param heightThumbnail
	 * @return
	 * @throws InterruptedException 
	 */
	public static ImageSearchResult getPropImage( String imageURL ) throws InterruptedException {
		ImageSearchResult img = new ImageSearchResult( );
		BufferedImage bimg;
		String base64String, 
			base64StringOriginal;	
		String type = null;
		URLConnection uc = null;
		MessageDigest digest = null;
		try {
	
			uc = new URL( imageURL ).openConnection( );
			uc.setConnectTimeout(10 * 1000);/*10 seconds*/
			//uc.setRequestMethod("GET");
			//uc.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US; rv:1.9.1.2) Gecko/20090729 Firefox/3.5.2 (.NET CLR 3.5.30729)");
			uc.connect();
					
			InputStream inImg =  uc.getInputStream( );
			bimg = ImageIO.read( inImg );
			type = getMimeType( uc );
			//digest = MessageDigest.getInstance( "SHA-256" );
			
			int width          	= bimg.getWidth( null );
			int height         	= bimg.getHeight( null );
			img.setMime( type );
			img.setHeight( Double.toString( height ) );
			img.setWidth( Double.toString( width ) );
			img.setUrl( imageURL );
			//byte[ ] bytesImgOriginal = IOUtils.toByteArray( new URL( imageURL ).openConnection( ) );
			//base64StringOriginal = Base64.encode( bytesImgOriginal );
			
			//calculate digest
			//digest.update( bytesImgOriginal );
			//byte byteDigest[ ] = digest.digest();
			//img.setDigest( convertByteArrayToHexString( byteDigest ) );
			
	        // Create a byte array output stream.
	        //base64String = Base64.encode( bytesImgOriginal );
			//bao.close( );

	
			return img;

		} catch ( MalformedURLException e ) {
			System.out.println( "[ImageParse][getPropImage] get image from url[" + img.getUrl( ) + "] error = " );
			return null;
		} catch ( IOException e ) {
			System.out.println( "[ImageParse][getPropImage] [" + img.getUrl( ) + "] e = " );
			return null;
		} catch( IllegalArgumentException e ) {
			System.out.println( "[ImageParse][getPropImage] [" + img.getUrl( ) + "] e = " );
			return null;
		} catch ( Exception e ) {
			 e.printStackTrace();
			 return null;
		}
		
	}
	


	
	/**
	 * convert the byte to hex format method (digest imgge)
	 * @param arrayBytes
	 * @return
	 */
	private static String convertByteArrayToHexString( byte[ ] byteData ) {
        StringBuffer hexString = new StringBuffer( );
    	for ( int i = 0 ; i < byteData.length ; i++ ) {
    		String hex = Integer.toHexString( 0xff & byteData[ i ] );
   	     	if( hex.length( ) == 1 ) hexString.append( '0' );
   	     	hexString.append( hex );
    	}
    	
    	return hexString.toString( );
	}
	
	/**
	 * Get mimetype from url
	 * @param uc
	 * @return
	 * @throws java.io.IOException
	 * @throws MalformedURLException
	 */
	public static String getMimeType( URLConnection uc ) throws java.io.IOException, MalformedURLException {
		return uc.getContentType( );
    }

	
	
	


}