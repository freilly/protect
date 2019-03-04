
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
import java.util.regex.*;


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
        String bucketName = "test.protect.usage"; //TODO correct bucket name



        log.info("Checking for bucket??" + bucketName);
        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.EU_WEST_1).build();
        try {

            String unProcessedPrefix = "unprocessed/";


            ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName).withPrefix(unProcessedPrefix).withStartAfter(unProcessedPrefix);
            ListObjectsV2Result result;

            result = s3.listObjectsV2(req);
            List<S3ObjectSummary> objects = result.getObjectSummaries();
            for (S3ObjectSummary os: objects) {
                String unProcessedFile = os.getKey();
                if(isFileNameFormatCorrect(unProcessedFile.replace("unprocessed/", ""))){
                    String processedFile =  unProcessedFile.replace("unprocessed/", "processed/");
                    log.info("* " + unProcessedFile);
                    log.info("*2 " + processedFile);


                    S3Object s3object = s3.getObject(bucketName, unProcessedFile);
                    log.info("retrieved file");

                    S3ObjectInputStream inputStream = s3object.getObjectContent();


                    log.info("Moving file to batch import folder:", unProcessedFile);
                    FileUtils.copyInputStreamToFile(inputStream, new File("/home/ec2-user/protect/" + processedFile));
                    log.info("Completed Download succesfully");


                    log.info("Copying " + unProcessedFile + " to processed directory");
                    s3.copyObject(bucketName, os.getKey(), bucketName, processedFile);

                    log.info("Deleting " + unProcessedFile + "from unprocessed directory");
                    s3.deleteObject(bucketName, os.getKey());
                }else{
                    log.info("File name does not match naming convention:" + unProcessedFile);
                }

            }

        } catch (AmazonServiceException e) {
            log.info("Exception: ", e);
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }
    }

    private boolean isFileNameFormatCorrect(String filename){
        return Pattern.matches("([A-Z])\\w+_Usage_([12]\\d{3}(0[1-9]|1[0-2])(0[1-9]|[12]\\d|3[01]))_[1-9]+.csv", filename);
    }

}