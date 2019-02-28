
package hello;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
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

    @Scheduled(fixedRate = 5000)
    public void reportCurrentTime() throws IOException {
        log.info("The time is now1 {}", dateFormat.format(new Date()));
        String bucketName = "protectUsage";

        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.SA_EAST_1).build();
        try {

            S3Object s3object = s3.getObject("protectUsage", "filetest.txt");
            S3ObjectInputStream inputStream = s3object.getObjectContent();
            FileUtils.copyInputStreamToFile(inputStream, new File("/Users/user/Desktop/hello.txt"));


        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }
    }

}