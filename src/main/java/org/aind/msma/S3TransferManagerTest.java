package org.aind.msma;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.transfer.s3.*;

import java.io.File;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class S3TransferManagerTest
{
    private static class TransferArgs implements Serializable
    {

        private static final long serialVersionUID = 6916709343246503328L;

        @Option(name = "-i", aliases = { "--inputFolder" }, required = true, usage = "The input directory.")
        private String inputFolder;

        @Option(name = "-b", aliases = { "--bucket"}, required = true, usage = "Destination s3 bucket.")
        private String bucket;

        @Option(name = "-p", aliases = { "--prefix" }, required = true, usage = "Destination prefix.")
        private String prefix;

        @Option(name = "-r", aliases = { "--region" }, required = false, usage = "AWS region")
        private String region = "us-west-2";

        @Option(name = "-t", aliases = {"--targetThroughput"}, required = false, usage = "Target throughput in Gb/s")
        private double targetThroughput = 10.0;

        @Option(name = "-m", aliases = {"--minPartSize"}, required = false, usage = "Minimum part size (in bytes)")
        private long minPartSize = 8388640L;

        @Option(name = "-s", aliases = {"--startIndex"}, required = false, usage = "Start index for files to upload (inclusive)")
        private int startIndex = 0;

        @Option(name = "-e", aliases = {"--endIndex"}, required = false, usage = "End index for files to upload (exclusive)")
        private int endIndex = -1;

        private boolean parsedSuccessfully = false;

        public TransferArgs(final String... args) throws IllegalArgumentException
        {
            final CmdLineParser parser = new CmdLineParser( this );
            try
            {
                parser.parseArgument( args );
                parsedSuccessfully = true;
            }
            catch ( final CmdLineException e )
            {
                System.err.println( e.getMessage() );
                parser.printUsage( System.err );
            }
        }
    }

    public static void main(final String[] args)
    {
        final TransferArgs parsedArgs = new TransferArgs( args );
        if ( !parsedArgs.parsedSuccessfully )
            throw new IllegalArgumentException( "argument format mismatch" );

        System.out.println( "Starting transfer..." );

        final S3TransferManager transferManager =
                S3TransferManager.builder()
                        .s3ClientConfiguration(cfg -> cfg.region(Region.of(parsedArgs.region))
                                .targetThroughputInGbps(parsedArgs.targetThroughput)
                                .minimumPartSizeInBytes(parsedArgs.minPartSize))
                        .build();

        final File folder = new File(parsedArgs.inputFolder);
        List<File> fileList = Arrays.stream(Objects.requireNonNull(folder.listFiles()))
                .filter(File::isFile)
                .collect(Collectors.toList());

        final int start = parsedArgs.startIndex;
        final int end = parsedArgs.endIndex == -1 ? fileList.size() : parsedArgs.endIndex;
        Collections.sort(fileList);
        fileList = fileList.subList(start, end);

        final List<FileUpload> uploadList = new ArrayList<>();
        for (final File file : fileList)
        {
            final FileUpload upload = transferManager.uploadFile(u -> u.source(file)
                    .putObjectRequest(p -> p.bucket(parsedArgs.bucket).key(parsedArgs.prefix + "/" + file.getName())));
            uploadList.add(upload);
        }

        // wait for the uploads to finish
        final Long t0 = System.currentTimeMillis();
        for (final FileUpload upload : uploadList) {
            upload.completionFuture().join();
        }
        final Long t1 = System.currentTimeMillis();

//        final DirectoryUpload directoryUpload =
//                transferManager.uploadDirectory(UploadDirectoryRequest.builder()
//                        .sourceDirectory(Paths.get(parsedArgs.inputFolder))
//                        .bucket(parsedArgs.bucket)
//                        .prefix(parsedArgs.prefix)
//                        .build());

//        final Long t0 = System.currentTimeMillis();
//        // Wait for the transfer to complete
//        final CompletedDirectoryUpload completedDirectoryUpload = directoryUpload.completionFuture().join();
//        final Long t1 = System.currentTimeMillis();

        System.out.println("transfer took " + ((t1 - t0) / 1000) + "s");

        // Print out the failed uploads
//        completedDirectoryUpload.failedTransfers().forEach(System.out::println);
    }

}
