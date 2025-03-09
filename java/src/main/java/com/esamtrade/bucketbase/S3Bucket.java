package com.esamtrade.bucketbase;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class S3Bucket extends BaseBucket {

    protected S3Client s3Client;
    protected S3AsyncClient s3AsyncClient;
    protected String bucketName;


    public S3Bucket(S3Client s3Client, S3AsyncClient s3AsyncClient, String bucketName) {
        this.s3Client = s3Client;
        this.s3AsyncClient = s3AsyncClient;
        this.bucketName = bucketName;
    }

    public S3Bucket(String endpoint, String accessKey, String secretKey, String bucketName) {
        this(createS3Client(endpoint, accessKey, secretKey), createS3AsyncClient(endpoint, accessKey, secretKey), bucketName);
    }

    private static S3Client createS3Client(String endpoint, String accessKey, String secretKey) {
        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(accessKey, secretKey);
        return S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .region(Region.US_EAST_1) // Default region, can be parameterized if needed
                .forcePathStyle(true)
                .build();
    }

    private static S3AsyncClient createS3AsyncClient(String endpoint, String accessKey, String secretKey) {
        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(accessKey, secretKey);
        return S3AsyncClient.builder()
                .endpointOverride(URI.create(endpoint))
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .region(Region.US_EAST_1) // Default region, can be parameterized if needed
                .forcePathStyle(true)
                .build();
    }

    @Override
    public void putObject(PurePosixPath name, byte[] content) {
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(name.toString())
                .build();
        s3Client.putObject(request, RequestBody.fromBytes(content));
    }

    @Override
    public void putObjectStream(PurePosixPath name, InputStream stream) {
        String _name = validateName(name);
        try {
            uploadLargeStream(_name, stream);
        } catch (Exception e) {
            throw new RuntimeException("Failed to upload object: " + _name, e);
        } finally {
            s3AsyncClient.close();
        }
    }


    private void uploadLargeStream(String key, InputStream inputStream) {
        int partSize = 5 * 1024 * 1024; // 5 MB
        List<CompletedPart> completedParts = new ArrayList<>();
        byte[] buffer = new byte[partSize];
        int bytesRead;
        int partNumber = 1;

        // 1. Initiate the multipart upload
        CreateMultipartUploadRequest createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
        CreateMultipartUploadResponse response = s3AsyncClient.createMultipartUpload(createMultipartUploadRequest).join();
        String uploadId = response.uploadId();

        try {
            // 2. Read the input stream and upload each part
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                byte[] bytesToUpload = (bytesRead < partSize) ? java.util.Arrays.copyOf(buffer, bytesRead) : buffer;
                UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .uploadId(uploadId)
                        .partNumber(partNumber)
                        .contentLength((long) bytesRead)
                        .build();
                AsyncRequestBody requestBody = AsyncRequestBody.fromBytes(bytesToUpload);
                CompletableFuture<UploadPartResponse> uploadPartResponse = s3AsyncClient.uploadPart(uploadPartRequest, requestBody);
                completedParts.add(CompletedPart.builder()
                        .partNumber(partNumber)
                        .eTag(uploadPartResponse.join().eTag())
                        .build());
                partNumber++;
            }

            // 3. Complete the multipart upload
            CompleteMultipartUploadRequest completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .uploadId(uploadId)
                    .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
                    .build();
            s3AsyncClient.completeMultipartUpload(completeMultipartUploadRequest).join();
        } catch (Exception e) {
            // Abort the multipart upload in case of failure
            AbortMultipartUploadRequest abortMultipartUploadRequest = AbortMultipartUploadRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .uploadId(uploadId)
                    .build();
            s3AsyncClient.abortMultipartUpload(abortMultipartUploadRequest).join();
            throw new RuntimeException("Failed to upload object: " + key, e);
        }
    }


    @Override
    public byte[] getObject(PurePosixPath name) throws IOException {
        try {
            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(name.toString())
                    .build();
            return s3Client.getObjectAsBytes(request).asByteArray();
        } catch (NoSuchKeyException e) {
            throw new FileNotFoundException("Object " + name + " not found in S3 bucket " + bucketName);
        }
    }

    @Override
    public ObjectStream getObjectStream(PurePosixPath name) throws IOException {
        try {
            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(name.toString())
                    .build();
            InputStream inputStream = s3Client.getObject(request, ResponseTransformer.toInputStream());
            return new ObjectStream(inputStream, name.toString());
        } catch (NoSuchKeyException e) {
            throw new FileNotFoundException("Object " + name + " not found in S3 bucket " + bucketName);
        }
    }

    @Override
    public List<PurePosixPath> listObjects(PurePosixPath prefix) {
        splitPrefix(prefix); // validate prefix
        List<PurePosixPath> result = new ArrayList<>();
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(prefix.toString())
                .build();

        ListObjectsV2Response response = s3Client.listObjectsV2(request);
        for (S3Object object : response.contents()) {
            result.add(new PurePosixPath(object.key()));
        }
        return result;
    }

    @Override
    public ShallowListing shallowListObjects(PurePosixPath prefix) {
        splitPrefix(prefix); // validate prefix
        List<PurePosixPath> objects = new ArrayList<>();
        List<PurePosixPath> prefixes = new ArrayList<>();

        String continuationToken = null;
        do {
            ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .prefix(prefix.toString())
                    .delimiter(SEP);

            if (continuationToken != null) {
                requestBuilder.continuationToken(continuationToken);
            }

            ListObjectsV2Response response = s3Client.listObjectsV2(requestBuilder.build());

            for (S3Object object : response.contents()) {
                objects.add(new PurePosixPath(object.key()));
            }

            for (CommonPrefix commonPrefix : response.commonPrefixes()) {
                prefixes.add(new PurePosixPath(commonPrefix.prefix()));
            }

            continuationToken = response.nextContinuationToken();
        } while (continuationToken != null);

        return new ShallowListing(objects, prefixes);
    }

    @Override
    public boolean exists(PurePosixPath name) {
        String _name = validateName(name);
        try {
            HeadObjectRequest request = HeadObjectRequest.builder()
                    .bucket(bucketName)
                    .key(_name)
                    .build();
            s3Client.headObject(request);
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        }
    }

    /**
     * Removes multiple objects from the S3 bucket.
     *
     * <p>**Note:** Amazon S3 requires the `Content-MD5` header for all Multi-Object Delete requests to ensure
     * data integrity. When interacting with S3-compatible storage solutions like MinIO, omitting this header
     * can result in a `400 Bad Request` error indicating a missing `Content-Md5` header.
     *
     * <p>This ensures compatibility with MinIO and similar services that enforce the presence of the `Content-MD5` header.
     * More info: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
     *
     * @param names List of object paths to be removed.
     * @return List of errors encountered during the deletion process.
     */
    @Override
    public List<DeleteError> removeObjects(List<PurePosixPath> names) {
        Set<String> namesSet = names.stream()
                .map(BaseBucket::validateName)
                .collect(Collectors.toSet());
        List<ObjectIdentifier> keys = names.stream()
                .map(name -> ObjectIdentifier.builder()
                        .key(name.toString())
                        .build())
                .collect(Collectors.toList());

        List<DeleteError> errors = new ArrayList<>();
        DeleteObjectsResponse response;
        if (!keys.isEmpty()) {
            try {
                Delete delete = Delete.builder().objects(keys).build();

                DeleteObjectsRequest request = DeleteObjectsRequest.builder()
                        .bucket(bucketName)
                        .delete(delete)
                        .build();

                response = s3Client.deleteObjects(request);
            } catch (S3Exception e) {
                if (e.statusCode() == 400 && e.getMessage().contains("Content-Md5")) {
                    // Explicitly adding the Content-MD5 header for compatibility with MinIO, so manually constructing the XML payload
                    StringBuilder xmlBuilder = new StringBuilder();
                    xmlBuilder.append("<Delete>");
                    for (ObjectIdentifier key : keys) {
                        xmlBuilder.append("<Object><Key>")
                                .append(key.key())
                                .append("</Key></Object>");
                    }
                    xmlBuilder.append("</Delete>");
                    String xmlPayload = xmlBuilder.toString();

                    // Compute MD5 checksum
                    String contentMd5;
                    try {
                        MessageDigest md = MessageDigest.getInstance("MD5");
                        byte[] md5Bytes = md.digest(xmlPayload.getBytes(StandardCharsets.UTF_8));
                        contentMd5 = Base64.getEncoder().encodeToString(md5Bytes);
                    } catch (NoSuchAlgorithmException e2) {
                        throw new RuntimeException("MD5 algorithm not found", e2);
                    }

                    // Create Delete object
                    Delete delete = Delete.builder().objects(keys).build();

                    // Create DeleteObjectsRequest with Content-MD5 header
                    DeleteObjectsRequest request = DeleteObjectsRequest.builder()
                            .bucket(bucketName)
                            .delete(delete)
                            .overrideConfiguration(o -> o.putHeader("Content-MD5", contentMd5))
                            .build();

                    // Perform the delete operation
                    response = s3Client.deleteObjects(request);
                } else {
                    throw e;
                }
            }

            // Process the response
            for (DeletedObject deleted : response.deleted()) {
                if (!namesSet.contains(deleted.key())) {
                    errors.add(new DeleteError("Object not found: " + deleted.key()));
                }
            }
        }
        return errors;
    }
}