/*
 * Copyright 2010-2020 Redgate Software Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.flywaydb.core.internal.resource.s3;

import com.amazonaws.SdkClientException;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.flywaydb.core.api.Location;
import org.flywaydb.core.api.logging.Log;
import org.flywaydb.core.api.logging.LogFactory;
import org.flywaydb.core.internal.resource.LoadableResource;

public class AwsS3Scanner {

  private static final Log LOG = LogFactory.getLog(AwsS3Scanner.class);

  private final Charset encoding;

  /**
   * Creates a new AWS S3 scanner.
   *
   * @param encoding The encoding to use.
   */
  public AwsS3Scanner(Charset encoding) {
    this.encoding = encoding;
  }

  /**
   * Scans S3 for the resources, see {@link DefaultAwsRegionProviderChain} for details on how the AWS region is
   * discovered. The format of the path is expected to be {@code s3:{bucketName}/{optional prefix}}.
   *
   * @param location The location in S3 to start searching. Subdirectories are also searched.
   * @return The resources that were found.
   */
  public Collection<LoadableResource> scanForResources(final Location location) {
    String bucketName = getBucketName(location);
    String prefix = getPrefix(bucketName, location.getPath());
    AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
    try {
      ListObjectsV2Result listObjectResult = s3Client.listObjectsV2(bucketName, prefix);
      return getLoadableResources(listObjectResult);
    } catch (SdkClientException e) {
      LOG.warn("Skipping s3 location:" + bucketName + prefix + " due to error: " + e.getMessage());
      return Collections.emptyList();
    }
  }

  private Collection<LoadableResource> getLoadableResources(final ListObjectsV2Result listObjectResult) {
    List<S3ObjectSummary> objectSummaries = listObjectResult.getObjectSummaries();
    Set<LoadableResource> resources = new TreeSet<>();
    for (S3ObjectSummary objectSummary : objectSummaries) {
      LOG.debug("Found Amazon S3 resource: " +
          objectSummary.getBucketName().concat("/").concat(objectSummary.getKey()));
      resources.add(new AwsS3Resource(objectSummary, encoding));
    }
    return resources;
  }

  private String getPrefix(String bucketName, String path) {
    String relativePathToBucket = path.substring(bucketName.length());
    if (relativePathToBucket.startsWith("/")) {
      relativePathToBucket = relativePathToBucket.substring(1);
    }
    if (relativePathToBucket.isEmpty()) {
      return null;
    }
    return relativePathToBucket;
  }

  private String getBucketName(final Location location) {
    return location.getPath().substring(0, location.getPath().indexOf("/"));
  }
}
