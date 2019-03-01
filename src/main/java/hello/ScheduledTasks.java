
package hello;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.MultipleFileDownload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import com.amazonaws.AmazonServiceException;


@Component
public class ScheduledTasks {

    private static final Logger log = LoggerFactory.getLogger(ScheduledTasks.class);

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");


 /*   @Scheduled(fixedRate = 5000)
    public void reportCurrentTime() {
        log.info("The time is now {}", dateFormat.format(new Date()));
    }
*/

    @Scheduled(fixedRate = 40000)
    public void reportCurrentTime() throws IOException, InterruptedException {
        String bucketName = "test.protect.usage";
        log.info("Checking for bucket??", bucketName);
        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.EU_WEST_1).build();
        try {

            String unProcessedPrefix = "unprocessed/";

            ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName).withPrefix(unProcessedPrefix);
            ListObjectsV2Result result;

            result = s3.listObjectsV2(req);
            List<S3ObjectSummary> objects = result.getObjectSummaries();
            for (S3ObjectSummary os: objects) {
                log.info("* " + os.getKey());


            }


            String unprocessedFile = "unprocessed/testObject.txt";
            String processedFile = "processed/testObject.txt";



            /*S3Object s3object = s3.getObject(bucketName, unprocessedFile);
            log.info("retreived file");

            s3.copyObject(bucketName, unprocessedFile, bucketName, processedFile);

            //s3.deleteObject(bucketName, unprocessedFile);


            S3ObjectInputStream inputStream = s3object.getObjectContent();


            log.info("Movingfile to batch import folder:", s3object.getKey());
            FileUtils.copyInputStreamToFile(inputStream, new File("/home/ec2-user/protect/downloadedFile.txt"));

            log.info("Completed succesfully");*/



           /* TransferManager transferManager = TransferManagerBuilder.standard().build();

            try {
                MultipleFileDownload multipleFileDownload = transferManager.downloadDirectory(
                        bucketName, unProcessedPrefix, new File("/home/ec2-user/protect/"));
                multipleFileDownload.waitForCompletion();
            } catch (AmazonServiceException e) {
                log.info("Error transfering multiple files");
                System.exit(1);
            }*/
        } catch (AmazonServiceException e) {
            log.info("Exception: ", e);
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }
    }

}