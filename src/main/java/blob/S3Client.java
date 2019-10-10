package blob;


import java.io.*;
import java.lang.management.ManagementFactory;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.util.Md5Utils;

import javax.net.ssl.HttpsURLConnection;

public class S3Client {
    private AmazonS3         s3;
    private TransferManager     tx;
    private static S3Client instance = null;
    private S3Client(String accessKeyId, String secretAccessKey, String endpoint){
        ClientConfiguration config = new ClientConfiguration();
        // SDK default using https, here i using HTTP
        config.withProtocol(Protocol.HTTPS);
        // SDK default using v4 sign, here using v2  sign
        config.setSignerOverride("S3SignerType");

        AWSCredentials credentials = null;
        System.out.println("accessKeyId:"+accessKeyId+"##secretAccessKsy:"+secretAccessKey);
        credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);
        this.s3 = new AmazonS3Client(credentials, config);
        this.s3.setEndpoint(endpoint);
        //##########https
//        config.setMaxErrorRetry(0);
//        config.setConnectionTimeout(5000);
//        this.s3 = AmazonS3ClientBuilder.standard()
//                .withCredentials(new AWSStaticCredentialsProvider(
//                        new BasicAWSCredentials(accessKeyId,secretAccessKey )
//                )).withClientConfiguration(config)
//                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint,"" ))
//                .withPathStyleAccessEnabled(true).build();
        this.tx = new TransferManager(this.s3);
    }
    public static S3Client getInstance(String accessKeyId, String secretAccessKey, String endpoint) {
        if (instance == null) {
            synchronized (S3Client.class) {
                if (instance == null) {
                    instance = new S3Client(accessKeyId, secretAccessKey, endpoint);
                }
            }
        }
        return instance;
    }
    public boolean isBucketExit(String bucketName){
        return this.s3.doesBucketExistV2(bucketName);
    }

    public boolean createBucket(String bucketName) {
        if(this.s3.doesBucketExistV2(bucketName) == true) {
            return false;
        }
        System.out.println("creating " + bucketName + " ...");
        this.s3.createBucket(bucketName);
        System.out.println(bucketName + " has been created!");
        return true;
    }
    public long getObjectSize(String bucketName, String key){
        long ret = -1;
        ObjectListing objectListing = this.s3.listObjects(bucketName);

        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
            if(objectSummary.getKey().equals(key))
                return objectSummary.getSize();
        }
        return ret;
    }

    public ObjectListing listObjects(String bucketName) {
        System.out.println("Listing objects of " + bucketName);
        ObjectListing objectListing = this.s3.listObjects(bucketName);
        int objectNum = 0;
        for(S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
            System.out.println(" - " + objectSummary.getKey() + " " + objectSummary.getSize() + " " + objectSummary.getETag());
            objectNum ++;
        }
        System.out.println("total " + objectNum + " object(s).");
        if(objectNum == 0)
            return null;
        else
            return objectListing;
    }

    public boolean isObjectExit(String bucketName, String key) {
        int len = key.length();
        ObjectListing objectListing = this.s3.listObjects(bucketName);
        String s = new String();
        for(S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
            s = objectSummary.getKey();
            int slen = s.length();
            if(len == slen) {
                int i;
                for(i=0;i<len;i++) if(s.charAt(i) != key.charAt(i)) break;
                if(i == len) return true;
            }
        }
        return false;
    }

    public String showContentOfAnObject(String bucketName, String key) {
        S3Object object = this.s3.getObject(new GetObjectRequest(bucketName, key));
        InputStream input = object.getObjectContent();
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        String content = null;
        try {
            while (true) {
                String line = reader.readLine();
                if (line == null)
                    break;
                System.out.println("    " + line);
                content += line;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return content;
    }

    public void listBuckets() {
        System.out.println("Listing buckets");
        int bucketNum = 0;
        for(Bucket bucket : this.s3.listBuckets()) {
            System.out.println(" - " + bucket.getName());
            bucketNum ++;
        }
        System.out.println("total " + bucketNum + " bucket(s).");
    }

    public boolean deleteBucket(String bucketName) {
        if(this.s3.doesBucketExist(bucketName) == false) {
            System.out.println(bucketName + " does not exists!");
            return false;
        }
        System.out.println("deleting " + bucketName + " ...");
        ObjectListing objectListing = this.s3.listObjects(bucketName);
        for(S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
            String key = objectSummary.getKey();
            this.s3.deleteObject(bucketName, key);
        }
        this.s3.deleteBucket(bucketName);
        System.out.println(bucketName + " has been deleted!");
        return true;
    }

    public boolean deleteObject(String bucketName, String key) {
        if(this.s3.doesBucketExist(bucketName) == false) {
            System.out.println(bucketName + " does not exists!");
            return false;
        }
        this.s3.deleteObject(bucketName, key);
        return true;
    }

    public void deleteObjectsWithPrefix(String bucketName, String prefix) {
        if(this.s3.doesBucketExist(bucketName) == false) {
            System.out.println(bucketName + " does not exists!");
            return;
        }
        System.out.println("deleting " + prefix +"* in " + bucketName + " ...");
        int pre_len = prefix.length();
        ObjectListing objectListing = this.s3.listObjects(bucketName);
        for(S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
            String key = objectSummary.getKey();
            int len = key.length();
            if(len < pre_len) continue;
            int i;
            for(i=0;i<pre_len;i++)
                if(key.charAt(i) != prefix.charAt(i))
                    break;
            if(i < pre_len) continue;
            this.s3.deleteObject(bucketName, key);
        }
        System.out.println("All " + prefix + "* deleted!");
    }

    // using Multipart Upload operation and ETag is "the MD5 hexdigest of each part’s MD5 digest concatenated together"
    public boolean uploadFileMulPart(String path, String bucketName, String key) {
        File fileToUpload = new File(path);
        if(fileToUpload.exists() == false) {
            System.out.println(path + " not exists!");
            return false;
        }
        PutObjectRequest request = new PutObjectRequest(bucketName, key, fileToUpload);
        Upload upload = this.tx.upload(request);
        while((int)upload.getProgress().getPercentTransferred() < 100) {
            try {
                Thread.sleep(1000);
                System.out.println(ManagementFactory.getRuntimeMXBean().getName()+"#uploadFileToBucket process: " + upload.getProgress().getPercentTransferred());
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        try {
            upload.waitForUploadResult();
            System.out.println(path + " upload succeed!");
            return true;
        }catch(InterruptedException e){
            e.printStackTrace();

        }
        return false;
    }
    // using PUT operation and ETag is normal md5 value
    public boolean uploadFilePut(String path, String bucketName, String key) {
        try {
            PutObjectRequest request = new PutObjectRequest(bucketName, key, new File(path));
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentType("plain/text");
            metadata.addUserMetadata("x-amz-meta-title", "someTitle");
            request.setMetadata(metadata);
            this.s3.putObject(request);
            return true;
        } catch (AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it, so it returned an error response.
            e.printStackTrace();
            return false;
        } catch (SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            e.printStackTrace();
            return false;
        }
    }

    public boolean downloadFileFromBucket(String bucketName,String key,String targetFilePath){
        S3Object object = this.s3.getObject(new GetObjectRequest(bucketName,key));
        if(object != null){
            System.out.println("Content-Type: " + object.getObjectMetadata().getContentType());
            InputStream input = null;
            FileOutputStream fileOutputStream = null;
            byte[] data = null;
            try {
                input = object.getObjectContent();
                data = new byte[input.available()];
                int len = 0;
                fileOutputStream = new FileOutputStream(targetFilePath);
                while ((len = input.read(data)) != -1) {
                    fileOutputStream.write(data, 0, len);
                }
                System.out.println("download and save to " + targetFilePath + " succeed!");
                return true;
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }finally{
                if(fileOutputStream != null){
                    try {
                        fileOutputStream.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                if(input != null){
                    try {
                        input.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        else {
            return false;
        }
    }
    public String getLocalMd5(String targetFilePath) {
        File f = new File(targetFilePath);
        String md5Str;
        byte[] md5Byte;
        try {
            md5Byte = Md5Utils.computeMD5Hash(f);
            System.out.println("td1, md5 = " + md5Byte);
            return new String(md5Byte);
        } catch (FileNotFoundException ex){
            ex.printStackTrace();
            return null;
        } catch (IOException ex){
            ex.printStackTrace();
            return null;
        }
    }

}