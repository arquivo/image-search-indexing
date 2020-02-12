import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.imageio.IIOException;
import javax.imageio.ImageIO;

import data.ImageData;
import org.apache.log4j.Logger;
import org.imgscalr.Scalr;
import org.imgscalr.Scalr.Method;

import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;
import utils.WARCInformationParser;


public class ImageParse {

    //TODO: extract this variables to config files
    public static final int THUMB_HEIGHT = 200;
    public static final int THUMB_WIDTH = 200;
    public static final int MIN_WIDTH = 51;
    public static final int MIN_HEIGHT = 51;

    public static final int MAX_WIDTH = 15000;
    public static final int MAX_HEIGHT = 15000;

    public static final String DIGEST_ALGORITHM = "SHA-256";

    public static Logger logger = Logger.getLogger(WARCInformationParser.class);

    /**
     *
     * @param img
     * @param widthThumbnail
     * @param heightThumbnail
     * @return
     * @throws InterruptedException
     */
//	public static ImageSearchResult getPropImage( String imageURL ) throws InterruptedException {
//		ImageSearchResult img = new ImageSearchResult( );
//		BufferedImage bimg;
//		String base64String, 
//			base64StringOriginal;	
//		String type = null;
//		URLConnection uc = null;
//		MessageDigest digest = null;
//		BufferedImage scaledImg = null;
//		ByteArrayOutputStream bao = new ByteArrayOutputStream( );
//		try {
//			int thumbWidth 	= 200, thumbHeight = 200;
//			uc = new URL( imageURL ).openConnection( );
//			uc.setConnectTimeout(10 * 1000);/*10 seconds*/
//			//uc.setRequestMethod("GET");
//			//uc.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US; rv:1.9.1.2) Gecko/20090729 Firefox/3.5.2 (.NET CLR 3.5.30729)");
//			uc.connect();
//			digest = MessageDigest.getInstance( "SHA-256" );
//			InputStream inImg =  uc.getInputStream( );
//			bimg = ImageIO.read( inImg );
//			type = getMimeType( uc );
//			int width          	= bimg.getWidth( null );
//			int height         	= bimg.getHeight( null );
//			img.setMime( type );
//			img.setHeight( Double.toString( height ) );
//			img.setWidth( Double.toString( width ) );
//			img.setUrl( imageURL );
//
//			byte[ ] bytesImgOriginal = IOUtils.toByteArray( new URL( imageURL ).openStream( ) );
//			//calculate digest
//			digest.update( bytesImgOriginal );
//			byte byteDigest[ ] = digest.digest();
//			img.setDigest( convertByteArrayToHexString( byteDigest ) );
//
//			if( width < thumbWidth || height < thumbHeight )
//				scaledImg = bimg;
//			else {
//				
//				if( type.equals( "image/gif" )  ) {
//					
//					//byte[] output = getThumbnailGif( inImg , thumbWidth , thumbHeight );
//					// Create a byte array output stream.
//					if( imageURL == null )
//						return null;
//			        
//					base64StringOriginal = Base64.encode( bytesImgOriginal );
//					bao.close( );
//					img.setThumbnail( base64StringOriginal );
//					return img;
//
//				} else {
//					double thumbRatio = (double) thumbWidth / (double) thumbHeight;
//					double imageRatio = (double) width / (double) height;
//					if ( thumbRatio < imageRatio ) 
//						thumbHeight = (int)( thumbWidth / imageRatio );
//					else 
//						thumbWidth = (int)( thumbHeight * imageRatio );
//					
//					if( width < thumbWidth && height < thumbHeight ) {
//						thumbWidth  = width;
//						thumbHeight = height;
//					} else if( width < thumbWidth )
//						thumbWidth = width;
//					else if( height < thumbHeight )
//						thumbHeight = height;
//					
//					scaledImg = Scalr.resize( bimg, 
//												Method.QUALITY, 
//												Scalr.Mode.AUTOMATIC, 
//												thumbWidth, 
//												thumbHeight, 
//												Scalr.OP_ANTIALIAS ); //create thumbnail				
//				}
//			}
//
//
//			// Write to output stream
//	        ImageIO.write( scaledImg , type.substring( 6 ) , bao );
//	        bao.flush( );
//	        
//	        // Create a byte array output stream.
//	        base64String = Base64.encode( bao.toByteArray( ) );
//	        bao.close( );
//	        img.setThumbnail( base64String );
//	
//			return img;
//
//		} catch ( MalformedURLException e ) {
//			e.printStackTrace( );
//			return null;
//		} catch ( IOException e ) {
//			e.printStackTrace( );
//			return null;
//		} catch( IllegalArgumentException e ) {
//			e.printStackTrace( );
//			return null;
//		} catch ( Exception e ) {
//			 e.printStackTrace();
//			 return null;
//		}
//		
//	}

    /**
     * get sha-256 digest string of a given string data
     *
     * @param data
     * @return
     */
    public static String hash256(String data) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance(DIGEST_ALGORITHM);
        md.update(data.getBytes());
        return convertByteArrayToHexString(md.digest());
    }


    /**
     * convert the byte to hex format method (digest imgge)
     *
     * @param byteData
     * @return
     */
    private static String convertByteArrayToHexString(byte[] byteData) {
        StringBuffer hexString = new StringBuffer();
        for (int i = 0; i < byteData.length; i++) {
            String hex = Integer.toHexString(0xff & byteData[i]);
            if (hex.length() == 1) hexString.append('0');
            hexString.append(hex);
        }

        return hexString.toString();
    }

    /**
     * Get mimetype from url
     *
     * @param imgSQLDTO
     * @return
     * @throws java.io.IOException
     * @throws MalformedURLException
     */
//	public static String getMimeType( URLConnection uc ) throws java.io.IOException, MalformedURLException {
//		return uc.getContentType( );
//    }
    public static ImageSearchResult getPropImage(ImageSQLDTO imgSQLDTO) {
        ImageSearchResult img = new ImageSearchResult();
        BufferedImage bimg;
        String base64String,
                base64StringOriginal;
        String type = null;
        MessageDigest digest = null;
        BufferedImage scaledImg = null;
        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        try {
            int thumbWidth = THUMB_WIDTH, thumbHeight = THUMB_HEIGHT;

            digest = MessageDigest.getInstance(DIGEST_ALGORITHM);
            InputStream inImg = new ByteArrayInputStream(imgSQLDTO.getContent());
            bimg = ImageIO.read(inImg);
            type = imgSQLDTO.getMimeType();
            int width = bimg.getWidth(null);
            int height = bimg.getHeight(null);

            if (width < MIN_WIDTH || height < MIN_HEIGHT) {
                /*Do not index very small images*/
                return null;
            }

            img.setMime(type);
            img.setHeight(Double.toString(height));
            img.setWidth(Double.toString(width));
            /*img.setUrl( imageURL );*/

            byte[] bytesImgOriginal = imgSQLDTO.getContent();
            //calculate digest
            digest.update(bytesImgOriginal);
            byte byteDigest[] = digest.digest();
            img.setDigest(convertByteArrayToHexString(byteDigest));

            if (width < thumbWidth || height < thumbHeight)
                scaledImg = bimg;
            else {

                if (type.equals("image/gif")) {

                    //byte[] output = getThumbnailGif( inImg , thumbWidth , thumbHeight );
                    // Create a byte array output stream.
					/*if( imageURL == null )
						return null;*/

                    base64StringOriginal = Base64.encode(bytesImgOriginal);
                    bao.close();
                    img.setThumbnail(base64StringOriginal);
                    return img;

                } else {
                    double thumbRatio = (double) thumbWidth / (double) thumbHeight;
                    double imageRatio = (double) width / (double) height;
                    if (thumbRatio < imageRatio)
                        thumbHeight = (int) (thumbWidth / imageRatio);
                    else
                        thumbWidth = (int) (thumbHeight * imageRatio);

                    if (width < thumbWidth && height < thumbHeight) {
                        thumbWidth = width;
                        thumbHeight = height;
                    } else if (width < thumbWidth)
                        thumbWidth = width;
                    else if (height < thumbHeight)
                        thumbHeight = height;

                    scaledImg = Scalr.resize(bimg,
                            Method.QUALITY,
                            Scalr.Mode.AUTOMATIC,
                            thumbWidth,
                            thumbHeight,
                            Scalr.OP_ANTIALIAS); //create thumbnail
                }
            }


            // Write to output stream
            ImageIO.write(scaledImg, type.substring(6), bao);
            bao.flush();

            // Create a byte array output stream.
            base64String = Base64.encode(bao.toByteArray());
            bao.close();
            img.setThumbnail(base64String);

            return img;

        } catch (MalformedURLException e) {
            e.printStackTrace();
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            return null;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }


    public static ImageData getPropImage(ImageData img) {


        try {

            String base64String;
            String mimeType = img.getMimeDetected();

            MessageDigest digest = null;
            BufferedImage scaledImg = null;

            Dimension imgDimensions = WARCInformationParser.getImageDimensions(img);

            //error getting dimensions; probably invalid or unsupported image type
            if (imgDimensions == null)
                return null;

            int width = imgDimensions.width;
            int height = imgDimensions.height;


            img.setHeight(height);
            img.setWidth(width);

            //To not do further processing, it will be ignored at the next stage
            //This is only returning a correct value to collect statistics in the map job
            if (width < MIN_WIDTH || height < MIN_HEIGHT || (height * width > MAX_HEIGHT * MAX_WIDTH))
                return img;

            digest = MessageDigest.getInstance(DIGEST_ALGORITHM);

            byte[] bytesImgOriginal = img.getBytesArray();

            //calculate digest
            digest.update(bytesImgOriginal);
            byte[] byteDigest = digest.digest();
            img.setContentHash(convertByteArrayToHexString(byteDigest));

            // avoid reading gifs, as they will not be resized
            if (mimeType.equals("image/gif")) {
                img.setBytes(bytesImgOriginal);
                return img;
            }

            BufferedImage bimg;
            InputStream inImg = new ByteArrayInputStream(img.getBytesArray());
            bimg = ImageIO.read(inImg);
            if (width < THUMB_WIDTH || height < THUMB_HEIGHT) {
                scaledImg = bimg;
            } else {

                int thumbWidth = THUMB_WIDTH, thumbHeight = THUMB_HEIGHT;
                double thumbRatio = (double) thumbWidth / (double) thumbHeight;
                double imageRatio = (double) width / (double) height;
                if (thumbRatio < imageRatio)
                    thumbHeight = (int) (thumbWidth / imageRatio);
                else
                    thumbWidth = (int) (thumbHeight * imageRatio);

                if (width < thumbWidth && height < thumbHeight) {
                    thumbWidth = width;
                    thumbHeight = height;
                } else if (width < thumbWidth)
                    thumbWidth = width;
                else if (height < thumbHeight)
                    thumbHeight = height;

                scaledImg = Scalr.resize(bimg,
                        Method.QUALITY,
                        Scalr.Mode.AUTOMATIC,
                        thumbWidth,
                        thumbHeight,
                        Scalr.OP_ANTIALIAS); //create thumbnail
            }

            ByteArrayOutputStream bao = new ByteArrayOutputStream();

            // Write to output stream
            ImageIO.write(scaledImg, mimeType.substring(6), bao);
            bao.flush();

            // Create a byte array output stream.
            base64String = Base64.encode(bao.toByteArray());
            bao.close();
            img.setBytes(base64String);

            return img;

        } catch (
                NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (
                IOException e) {
            logger.error(String.format("Error loading image url: %s with error message %s", img.getURLWithTimestamp(), e.getMessage()));
        }
        return null;
    }

}

