/**
 * Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file
 * except in compliance with the License. A copy of the License is located at
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "LICENSE.TXT" file accompanying this file. This file is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.dynamodb.exportformat;

import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import java.util.Map;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.gson.Gson;


class ExportRecordWriter implements RecordWriter<NullWritable, DynamoDBItemWritable> {

  private static final String UTF_8 = "UTF-8";
  private static final byte[] NEWLINE;

  static {
    try {
      NEWLINE = "\n".getBytes(UTF_8);
    } catch (UnsupportedEncodingException uee) {
      throw new IllegalArgumentException("can't find " + UTF_8 + " encoding");
    }
  }

  private final DataOutputStream out;

  public ExportRecordWriter(DataOutputStream out) throws IOException {
    this.out = out;
  }

  // NOTE: Data transformation step happens here
  // Adding new attribute last_timestamp_range_id, which is <last_timestamp>::<video_guid>
  @Override
  public synchronized void write(NullWritable key, DynamoDBItemWritable value) throws IOException {
    
    Map<String, AttributeValue> dynamoDBItem = value.getItem();
    String last_timestamp_value = "";
    String video_guid_value = "";
    
    // For each attribute in the dynamo item
    for (Map.Entry<String, AttributeValue> entry : dynamoDBItem.entrySet()) {
      String entryKey = entry.getKey();
      if (entryKey.equals("last_timestamp")) {
        last_timestamp_value = entry.getValue().getN();
      }
      if (entryKey.equals("video_guid")) {
        video_guid_value = entry.getValue().getS();
      }
    }
    String last_timestamp_range_id = last_timestamp_value + "::" + video_guid_value;
    dynamoDBItem.put("last_timestamp_range_id", new AttributeValue(last_timestamp_range_id));
    value.setItem(dynamoDBItem);
    
    out.write(value.writeStream().getBytes(UTF_8));
    out.write(NEWLINE);
  }

  @Override
  public synchronized void close(Reporter reporter) throws IOException {
    out.close();
  }

}
