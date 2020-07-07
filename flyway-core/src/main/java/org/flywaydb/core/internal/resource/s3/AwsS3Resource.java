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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.charset.Charset;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.logging.Log;
import org.flywaydb.core.api.logging.LogFactory;
import org.flywaydb.core.internal.resource.LoadableResource;

public class AwsS3Resource extends LoadableResource {

  private static final Log LOG = LogFactory.getLog(AwsS3Resource.class);

  private S3ObjectSummary s3ObjectSummary;
  private Charset encoding;

  public AwsS3Resource(S3ObjectSummary s3ObjectSummary, Charset encoding) {
    this.s3ObjectSummary = s3ObjectSummary;
    this.encoding = encoding;
  }

  @Override
  public Reader read() {
    AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
    try {
      S3Object o = s3.getObject(s3ObjectSummary.getBucketName(), s3ObjectSummary.getKey());
      S3ObjectInputStream s3is = o.getObjectContent();
      return Channels.newReader(Channels.newChannel(s3is), encoding.name());
    } catch (AmazonServiceException e) {
      LOG.error(e.getErrorMessage(), e);
      throw new FlywayException("Failed to get object from s3: " + e.getErrorMessage(), e);
    }
  }

  @Override
  public String getAbsolutePath() {
    return s3ObjectSummary.getBucketName().concat("/").concat(s3ObjectSummary.getKey());
  }

  @Override
  public String getAbsolutePathOnDisk() {
    return getAbsolutePath();
  }

  /**
   * @return The filename of this resource, without the path.
   */
  @Override
  public String getFilename() {
    return s3ObjectSummary.getKey().substring(s3ObjectSummary.getKey().lastIndexOf('/') + 1);
  }

  @Override
  public String getRelativePath() {
    return getAbsolutePath();
  }
}