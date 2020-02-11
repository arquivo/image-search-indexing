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
import org.imgscalr.Scalr;
import org.imgscalr.Scalr.Method;

import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;


public class ImageParse {

    //TODO: extract this variables to config files
    public static final int THUMB_HEIGHT = 200;
    public static final int THUMB_WIDTH = 200;
    public static final int MIN_WIDTH = 51;
    public static final int MIN_HEIGHT = 51;
    public static final String DIGEST_ALGORITHM = "SHA-256";

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
        BufferedImage bimg;
        String base64String;
        //TODO: check if we should trust the mime type or try to guess from file bytes
        String mimeType = img.getMimeReported();
        MessageDigest digest = null;
        BufferedImage scaledImg = null;
        ByteArrayOutputStream bao = new ByteArrayOutputStream();


        try {
            int thumbWidth = THUMB_WIDTH, thumbHeight = THUMB_HEIGHT;

            digest = MessageDigest.getInstance(DIGEST_ALGORITHM);
            InputStream inImg = new ByteArrayInputStream(img.getBytesArray());
            bimg = ImageIO.read(inImg);

            int width = bimg.getWidth(null);
            int height = bimg.getHeight(null);

            img.setHeight(height);
            img.setWidth(width);

            //To not do further processing, it will be ignored at the next stage
            //This is only returning a correct value to collect statistics in the map job
            if (width < MIN_WIDTH || height < MIN_HEIGHT) {
                return img;
            }

            byte[] bytesImgOriginal = img.getBytesArray();
            //calculate digest
            digest.update(bytesImgOriginal);
            byte byteDigest[] = digest.digest();
            img.setContentHash(convertByteArrayToHexString(byteDigest));

            if (width < thumbWidth || height < thumbHeight)
                scaledImg = bimg;
            else {
                if (mimeType.equals("image/gif")) {

                    //byte[] output = getThumbnailGif( inImg , thumbWidth , thumbHeight );
                    // Create a byte array output stream.
					/*if( imageURL == null )
						return null;*/
                    bao.close();
                    img.setBytes(bytesImgOriginal);
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
            //TODO: check if writing in the "original" format (type.substring(6)) works or whether everything should be converted to PNG
            ImageIO.write(scaledImg, mimeType.substring(6), bao);
            bao.flush();

            // Create a byte array output stream.
            base64String = Base64.encode(bao.toByteArray());
            bao.close();
            img.setBytes(base64String);

            return img;

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (IIOException e) {
            /*
            avax.imageio.IIOException: Unsupported Image Type
        at java.desktop/com.sun.imageio.plugins.jpeg.JPEGImageReader.readInternal(JPEGImageReader.java:1139)
        at java.desktop/com.sun.imageio.plugins.jpeg.JPEGImageReader.read(JPEGImageReader.java:1110)
        at java.desktop/javax.imageio.ImageIO.read(ImageIO.java:1468)
        at java.desktop/javax.imageio.ImageIO.read(ImageIO.java:1363)
        at ImageParse.getPropImage(ImageParse.java:302)
        at FullImageIndexer$Map.saveImageMetadata(FullImageIndexer.java:114)
        at FullImageIndexer$Map.createImageDB(FullImageIndexer.java:183)
        at FullImageIndexer$Map.lambda$map$1(FullImageIndexer.java:317)
        at ImageSearchIndexingUtil.readArcRecords(ImageSearchIndexingUtil.java:56)
        at FullImageIndexer$Map.map(FullImageIndexer.java:314)
        at FullImageIndexer$Map.map(FullImageIndexer.java:71)
        at org.apache.hadoop.mapreduce.Mapper.run(Mapper.java:146)
        at org.apache.hadoop.mapred.MapTask.runNewMapper(MapTask.java:799)
        at org.apache.hadoop.mapred.MapTask.run(MapTask.java:347)
        at org.apache.hadoop.mapred.LocalJobRunner$Job$MapTaskRunnable.run(LocalJobRunner.java:271)
        at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515)
        at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)
        at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
        at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
        at java.base/java.lang.Thread.run(Thread.java:834)

             */
            System.err.println(String.format("%s %s %s", img.getUrl(), img.getMimeReported(), img.getMimeDetected()));
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
