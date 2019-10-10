package blob;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.UUID;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import java.util.Date;

public class S3SDKSample {
    final static String accessKeyId = "dd5b6b3b90954dbd9dc0c50f8c2c17ec";
    final static String secretAccessKey = "SjccfXAbzzrq7uJD7Bi7oIVwf1gYS3oyU";
    final static String endpoint = "http://61.219.26.12:8080";
//    final static String accessKeyId = " jianfengsa";
//    final static String secretAccessKey = "6P3ZRd7N709APoFGBysY969zWmnKR0kPHYZMqp3huyaeYTt7jUoyMkl2nRp4+WZ3F+eSQoQ3v3sgzZXlXgvpww==";
//    final static String endpoint = "https://jianfengsa.blob.wise-paas.com";
    final static String bucketName = "androidbsp";

    final static String upfile = "/home/gangqiangsun/bspStore/1.5M/rom3420/rom-3420-a1.ota.zip";
    final static String key = upfile;
    final static String dlfile = "/home/gangqiangsun/bspStore/1.5M/download/rom-3420-a1.ota.zip";

    final static String upfile2 = "/home/gangqiangsun/bspStore/1.5M/rom7421/rom-7421-a1.ota.zip";
    final static String key2 = upfile2;
    final static String dlfile2 = "/home/gangqiangsun/bspStore/1.5M/download/rom-7421-a1.ota.zip";

    public static void main(String[] args) throws IOException {
        S3Client s3Client;
        try{
            s3Client = S3Client.getInstance(accessKeyId, secretAccessKey, endpoint);
        } catch (Exception ex){
            ex.printStackTrace();
            return;
        }
        if(s3Client.isBucketExit(bucketName)){
            s3Client.deleteBucket(bucketName);
        }
        try{
            Thread.sleep(100000);
        }catch (InterruptedException e){
            e.printStackTrace();
        }

        if(!s3Client.isBucketExit(bucketName)){
            s3Client.createBucket(bucketName);
        }

        System.out.println("isBucketExit:"+s3Client.isBucketExit(bucketName));
        s3Client.listObjects(bucketName);

        Thread1 thread1 = new Thread1();
        thread1.start();

        Thread2 thread2 = new Thread2();
        thread2.start();
    }

    static class Thread1 extends Thread{
        @Override
        public void run() {
            S3Client s3Client;
            try{
                s3Client = S3Client.getInstance(accessKeyId, secretAccessKey, endpoint);
            } catch (Exception ex){
                ex.printStackTrace();
                return;
            }

            boolean ret = false;
            Date date;
            ret = s3Client.isObjectExit(bucketName, key);
            if(ret == true){
                System.out.println(key + " already exists");
            }else {
                date = new Date();
                System.out.println("td1, before upload:" + date);
                ret = s3Client.uploadFilePut(upfile, bucketName, key);
                date = new Date();
                System.out.println("td1, after upload:" + date + ", return: " + ret);
            }
//            try{
//                Thread.sleep(3000);
//            }catch (InterruptedException e){
//                e.printStackTrace();
//            }
            s3Client.listObjects(bucketName);

            ret = s3Client.downloadFileFromBucket(bucketName, key, dlfile);
            date = new Date();
            System.out.println("td1, after downloadFileFromBucket:" + date + ", return: " + ret);
            return;
        }
    }

    static class Thread2 extends Thread{
        @Override
        public void run() {
            S3Client s3Client;
            try{
                s3Client = S3Client.getInstance(accessKeyId, secretAccessKey, endpoint);
            } catch (Exception ex){
                ex.printStackTrace();
                return;
            }

            boolean ret = false;
            Date date = null;
            ret = s3Client.isObjectExit(bucketName, key2);
            if(ret == true){
                System.out.println(key2 + " already exists");
            }else {
                date = new Date();
                System.out.println("td2, before upload:" + date);
                ret = s3Client.uploadFileMulPart(upfile2, bucketName, key2);
                date = new Date();
                System.out.println("td2, after upload:" + date + ", return: " + ret);
            }
//            try{
//                Thread.sleep(3000);
//            }catch (InterruptedException e){
//                e.printStackTrace();
//            }
            s3Client.listObjects(bucketName);

            ret = s3Client.downloadFileFromBucket(bucketName, key2, dlfile2);
            date = new Date();
            System.out.println("td2, after downloadFileFromBucket:" + date + ", return: " + ret);
            return;
        }
    }
}