
package com.qds.snapshot.aws;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.*;
import com.qds.snapshot.xml.xsd.Snapshot;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.commons.io.FilenameUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by uc0010 on 3/13/2017.
 */
public class AwsS3Util {

    public static final String PROFILE_NAME = "profile";
    public static final String BUCKET_NAME = "bucket";

    private static AwsS3Util  instance = new AwsS3Util();

    AWSCredentials credentials = null;
    AmazonS3 s3 = null;

    private AwsS3Util() {
        try {
            credentials = new ProfileCredentialsProvider(PROFILE_NAME).getCredentials();
            s3 = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials)).build();

        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                            "Please make sure that your credentials file is at the correct " +
                            "location (~/.aws/credentials), and is in valid format.",
                    e);
        }
    }

    public static AwsS3Util getInstance() {
        return instance;
    }


    public void saveSnapshot(File file){
        PutObjectRequest putRequest =  new PutObjectRequest(BUCKET_NAME, FilenameUtils.getName(file.getName()),file);

        // Request server-side encryption.
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        putRequest.setMetadata(objectMetadata);

        s3.putObject(putRequest);
    }

    /**
     * Deletes the Snapshot from S3 Bucket
     * @param keys
     */
    public void deleteSnapshot(List<String> keys){

        // Multi-object delete by specifying only keys (no version ID).
        DeleteObjectsRequest multiObjectDeleteRequest = new DeleteObjectsRequest(
                BUCKET_NAME).withQuiet(false);
        List<DeleteObjectsRequest.KeyVersion> justKeys = new ArrayList<DeleteObjectsRequest.KeyVersion>();
        for (String key : keys) {
            justKeys.add(new DeleteObjectsRequest.KeyVersion(key));
        }
        multiObjectDeleteRequest.setKeys(justKeys);
        // Execute DeleteObjects - Amazon S3 add delete marker for each object
        // deletion. The objects no disappear from your bucket (verify).
        DeleteObjectsResult delObjRes = null;
        try {
            delObjRes = s3.deleteObjects(multiObjectDeleteRequest);
            System.out.format("Successfully deleted all the %s items.\n", delObjRes.getDeletedObjects().size());
        } catch (MultiObjectDeleteException mode) {
            System.out.format("%s \n", mode.getMessage());
            System.out.format("No. of objects successfully deleted = %s\n", mode.getDeletedObjects().size());
            System.out.format("No. of objects failed to delete = %s\n", mode.getErrors().size());
            System.out.format("Printing error data...\n");
        }
    }



    /**
     * get the persisted XML from s3.
     *
     * @param externalId
     * @return
     * @throws Exception
     */
    public String fetchS3Object(String externalId, String snapBatchId, String environment) throws Exception {
        String xmlFile = null;
        try{
        GetObjectRequest getObjectRequest = new GetObjectRequest(BUCKET_NAME,getKeyName(externalId,snapBatchId,environment));
        S3Object s3object = s3.getObject(getObjectRequest);
        System.out.println("Content-Type: "  +
                s3object.getObjectMetadata().getContentType());
        xmlFile = displayTextInputStream(s3object.getObjectContent());
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which" +
                    " means your request made it " +
                    "to Amazon S3, but was rejected with an error response" +
                    " for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means"+
                    " the client encountered " +
                    "an internal error while trying to " +
                    "communicate with S3, " +
                    "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
        return xmlFile;
    }

    /**
     *
     * @param externalId
     * @param snapBatchId
     * @param environment
     * @return
     */
    public String getKeyName(String externalId, String snapBatchId, String environment) {
        return externalId + "_" + snapBatchId + "_" + environment ;
    }

    /**
     *
     * @param input
     * @return
     * @throws IOException
     */
    private String displayTextInputStream(InputStream input)
            throws IOException {

        BufferedReader reader = new BufferedReader(new
                InputStreamReader(input));
        StringBuilder out = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            out.append(line);
        }
        String xmlFile = out.toString();
        reader.close();

        return xmlFile;
    }
}

