/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.tosfs.object.tos;

import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.TOSClientConfiguration;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.TOSV2ClientBuilder;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.TosClientException;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.TosException;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.TosServerException;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.auth.Credential;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.auth.Credentials;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.TOSV2;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.comm.HttpStatus;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.comm.common.ACLType;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.internal.RequestOptionsBuilder;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.acl.GetObjectAclOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.acl.PutObjectAclInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.acl.PutObjectAclOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.CreateBucketInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.CreateBucketOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.CreateBucketV2Input;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.CreateBucketV2Output;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.DeleteBucketCORSInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.DeleteBucketCORSOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.DeleteBucketCustomDomainInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.DeleteBucketCustomDomainOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.DeleteBucketEncryptionInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.DeleteBucketEncryptionOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.DeleteBucketInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.DeleteBucketInventoryInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.DeleteBucketInventoryOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.DeleteBucketLifecycleInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.DeleteBucketLifecycleOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.DeleteBucketMirrorBackInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.DeleteBucketMirrorBackOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.DeleteBucketOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.DeleteBucketPolicyInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.DeleteBucketPolicyOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.DeleteBucketRealTimeLogInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.DeleteBucketRealTimeLogOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.DeleteBucketRenameInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.DeleteBucketRenameOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.DeleteBucketReplicationInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.DeleteBucketReplicationOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.DeleteBucketTaggingInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.DeleteBucketTaggingOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.DeleteBucketWebsiteInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.DeleteBucketWebsiteOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.GetBucketACLInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.GetBucketACLOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.GetBucketCORSInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.GetBucketCORSOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.GetBucketEncryptionInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.GetBucketEncryptionOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.GetBucketInventoryInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.GetBucketInventoryOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.GetBucketLifecycleInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.GetBucketLifecycleOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.GetBucketLocationInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.GetBucketLocationOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.GetBucketMirrorBackInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.GetBucketMirrorBackOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.GetBucketNotificationInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.GetBucketNotificationOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.GetBucketNotificationType2Input;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.GetBucketNotificationType2Output;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.GetBucketPolicyInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.GetBucketPolicyOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.GetBucketRealTimeLogInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.GetBucketRealTimeLogOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.GetBucketRenameInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.GetBucketRenameOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.GetBucketReplicationInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.GetBucketReplicationOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.GetBucketTaggingInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.GetBucketTaggingOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.GetBucketVersioningInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.GetBucketVersioningOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.GetBucketWebsiteInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.GetBucketWebsiteOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.HeadBucketOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.HeadBucketV2Input;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.HeadBucketV2Output;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.ListBucketCustomDomainInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.ListBucketCustomDomainOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.ListBucketInventoryInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.ListBucketInventoryOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.ListBucketsInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.ListBucketsOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.ListBucketsV2Input;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.ListBucketsV2Output;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.PutBucketACLInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.PutBucketACLOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.PutBucketCORSInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.PutBucketCORSOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.PutBucketCustomDomainInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.PutBucketCustomDomainOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.PutBucketEncryptionInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.PutBucketEncryptionOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.PutBucketInventoryInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.PutBucketInventoryOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.PutBucketLifecycleInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.PutBucketLifecycleOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.PutBucketMirrorBackInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.PutBucketMirrorBackOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.PutBucketNotificationInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.PutBucketNotificationOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.PutBucketNotificationType2Input;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.PutBucketNotificationType2Output;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.PutBucketPolicyInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.PutBucketPolicyOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.PutBucketRealTimeLogInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.PutBucketRealTimeLogOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.PutBucketRenameInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.PutBucketRenameOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.PutBucketReplicationInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.PutBucketReplicationOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.PutBucketStorageClassInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.PutBucketStorageClassOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.PutBucketTaggingInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.PutBucketTaggingOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.PutBucketVersioningInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.PutBucketVersioningOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.PutBucketWebsiteInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.bucket.PutBucketWebsiteOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.AbortMultipartUploadInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.AbortMultipartUploadOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.AppendObjectInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.AppendObjectOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.CompleteMultipartUploadInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.CompleteMultipartUploadOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.CompleteMultipartUploadV2Input;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.CompleteMultipartUploadV2Output;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.CopyObjectOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.CopyObjectV2Input;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.CopyObjectV2Output;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.CreateMultipartUploadInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.CreateMultipartUploadOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.DeleteMultiObjectsInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.DeleteMultiObjectsOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.DeleteMultiObjectsV2Input;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.DeleteMultiObjectsV2Output;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.DeleteObjectInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.DeleteObjectOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.DeleteObjectTaggingInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.DeleteObjectTaggingOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.DownloadFileInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.DownloadFileOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.FetchObjectInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.FetchObjectOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.GetFetchTaskInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.GetFetchTaskOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.GetFileStatusInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.GetFileStatusOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.GetObjectACLV2Input;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.GetObjectACLV2Output;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.GetObjectOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.GetObjectTaggingInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.GetObjectTaggingOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.GetObjectToFileInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.GetObjectToFileOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.GetObjectV2Input;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.GetObjectV2Output;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.GetSymlinkInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.GetSymlinkOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.HeadObjectOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.HeadObjectV2Input;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.HeadObjectV2Output;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.ListMultipartUploadsInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.ListMultipartUploadsOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.ListMultipartUploadsV2Input;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.ListMultipartUploadsV2Output;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.ListObjectVersionsInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.ListObjectVersionsOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.ListObjectVersionsV2Input;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.ListObjectVersionsV2Output;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.ListObjectsInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.ListObjectsOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.ListObjectsType2Input;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.ListObjectsType2Output;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.ListObjectsV2Input;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.ListObjectsV2Output;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.ListPartsInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.ListPartsOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.ListUploadedPartsInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.ListUploadedPartsOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.ObjectMetaRequestOptions;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.PreSignedPolicyURLInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.PreSignedPolicyURLOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.PreSignedPostSignatureInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.PreSignedPostSignatureOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.PreSignedURLInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.PreSignedURLOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.PreSingedPolicyURLInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.PreSingedPolicyURLOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.PutFetchTaskInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.PutFetchTaskOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.PutObjectACLInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.PutObjectACLOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.PutObjectFromFileInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.PutObjectFromFileOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.PutObjectInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.PutObjectOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.PutObjectTaggingInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.PutObjectTaggingOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.PutSymlinkInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.PutSymlinkOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.RenameObjectInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.RenameObjectOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.RestoreObjectInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.RestoreObjectOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.ResumableCopyObjectInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.ResumableCopyObjectOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.SetObjectMetaInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.SetObjectMetaOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.UploadFileInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.UploadFileOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.UploadFileV2Input;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.UploadFileV2Output;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.UploadPartCopyInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.UploadPartCopyOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.UploadPartCopyV2Input;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.UploadPartCopyV2Output;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.UploadPartFromFileInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.UploadPartFromFileOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.UploadPartInput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.UploadPartOutput;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.UploadPartV2Input;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.model.object.UploadPartV2Output;
import org.apache.hadoop.fs.tosfs.shaded.com.volcengine.tos.transport.TransportConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.tosfs.object.InputStreamProvider;
import org.apache.hadoop.fs.tosfs.object.Part;
import org.apache.hadoop.fs.tosfs.util.RetryableUtils;
import org.apache.hadoop.thirdparty.com.google.common.base.Throwables;
import org.apache.hadoop.thirdparty.com.google.common.io.CountingInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import javax.net.ssl.SSLException;

public class DelegationClient implements TOSV2 {

  private static final Logger LOG = LoggerFactory.getLogger(DelegationClient.class);

  private final Credentials provider;
  private final TOSClientConfiguration config;
  private int maxRetryTimes;
  private TOSV2 client;
  private volatile Credential usedCredential;
  private final List<String> nonRetryable409ErrorCodes;

  protected DelegationClient(
      TOSClientConfiguration configuration, int maxRetryTimes, List<String> nonRetryable409ErrorCodes) {
    this.config = configuration;
    this.maxRetryTimes = maxRetryTimes;
    this.provider = configuration.getCredentials();
    this.usedCredential = provider.credential();
    this.client = new TOSV2ClientBuilder().build(configuration);
    this.nonRetryable409ErrorCodes = nonRetryable409ErrorCodes;
  }

  @VisibleForTesting
  void setClient(TOSV2 client) {
    this.client = client;
  }

  public TOSV2 client() {
    return client;
  }

  @VisibleForTesting
  void setMaxRetryTimes(int maxRetryTimes) {
    this.maxRetryTimes = maxRetryTimes;
  }

  public int maxRetryTimes() {
    return maxRetryTimes;
  }

  public TOSClientConfiguration config() {
    return config;
  }

  public Credential usedCredential() {
    return usedCredential;
  }

  @Override
  public CreateBucketV2Output createBucket(String bucket) throws TosException {
    return retry(() -> client.createBucket(bucket));
  }

  @Override
  public CreateBucketV2Output createBucket(CreateBucketV2Input input) throws TosException {
    return retry(() -> client.createBucket(input));
  }

  @Override
  public HeadBucketV2Output headBucket(HeadBucketV2Input input) throws TosException {
    return retry(() -> client.headBucket(input));
  }

  @Override
  public DeleteBucketOutput deleteBucket(DeleteBucketInput input) throws TosException {
    return retry(() -> client.deleteBucket(input));
  }

  @Override
  public ListBucketsV2Output listBuckets(ListBucketsV2Input input) throws TosException {
    return retry(() -> client.listBuckets(input));
  }

  @Override
  public CreateBucketOutput createBucket(CreateBucketInput input) throws TosException {
    return retry(() -> client.createBucket(input));
  }

  @Override
  public HeadBucketOutput headBucket(String bucket) throws TosException {
    return retry(() -> client.headBucket(bucket));
  }

  @Override
  public DeleteBucketOutput deleteBucket(String bucket) throws TosException {
    return retry(() -> client.deleteBucket(bucket));
  }

  @Override
  public ListBucketsOutput listBuckets(ListBucketsInput input) throws TosException {
    return retry(() -> client.listBuckets(input));
  }

  @Override
  public PutBucketPolicyOutput putBucketPolicy(String bucket, String policy) throws TosException {
    return retry(() -> client.putBucketPolicy(bucket, policy));
  }

  @Override
  public PutBucketPolicyOutput putBucketPolicy(PutBucketPolicyInput input) throws TosException {
    return retry(() -> client.putBucketPolicy(input));
  }

  @Override
  public GetBucketPolicyOutput getBucketPolicy(String bucket) throws TosException {
    return retry(() -> client.getBucketPolicy(bucket));
  }

  @Override
  public GetBucketPolicyOutput getBucketPolicy(GetBucketPolicyInput input) throws TosException {
    return retry(() -> client.getBucketPolicy(input));
  }

  @Override
  public DeleteBucketPolicyOutput deleteBucketPolicy(String bucket) throws TosException {
    return retry(() -> client.deleteBucketPolicy(bucket));
  }

  @Override
  public GetObjectOutput getObject(String bucket, String objectKey,
      RequestOptionsBuilder... builders) throws TosException {
    return retry(() -> client.getObject(bucket, objectKey, builders));
  }

  @Override
  public HeadObjectOutput headObject(String bucket, String objectKey,
      RequestOptionsBuilder... builders) throws TosException {
    return retry(() -> client.headObject(bucket, objectKey, builders));
  }

  @Override
  public DeleteObjectOutput deleteObject(String bucket, String objectKey,
      RequestOptionsBuilder... builders) throws TosException {
    return retry(() -> client.deleteObject(bucket, objectKey, builders));
  }

  @Override
  public DeleteMultiObjectsOutput deleteMultiObjects(
      String bucket,
      DeleteMultiObjectsInput input,
      RequestOptionsBuilder... builders)
      throws TosException {
    return retry(() -> client.deleteMultiObjects(bucket, input, builders));
  }

  @Override
  public PutObjectOutput putObject(
      String bucket, String objectKey, InputStream inputStream,
      RequestOptionsBuilder... builders)
      throws TosException {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public UploadFileOutput uploadFile(
      String bucket, UploadFileInput input,
      RequestOptionsBuilder... builders) throws TosException {
    return retry(() -> client.uploadFile(bucket, input, builders));
  }

  @Override
  public AppendObjectOutput appendObject(
      String bucket, String objectKey, InputStream content, long offset,
      RequestOptionsBuilder... builders)
      throws TosException {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public SetObjectMetaOutput setObjectMeta(String bucket, String objectKey, RequestOptionsBuilder... builders)
      throws TosException {
    return retry(() -> client.setObjectMeta(bucket, objectKey, builders));
  }

  @Override
  public ListObjectsOutput listObjects(String bucket, ListObjectsInput input) throws TosException {
    return retry(() -> client.listObjects(bucket, input));
  }

  @Override
  public ListObjectVersionsOutput listObjectVersions(String bucket, ListObjectVersionsInput input) throws TosException {
    return retry(() -> client.listObjectVersions(bucket, input));
  }

  @Override
  public CopyObjectOutput copyObject(
      String bucket, String srcObjectKey, String dstObjectKey,
      RequestOptionsBuilder... builders)
      throws TosException {
    return retry(() -> client.copyObject(bucket, srcObjectKey, dstObjectKey, builders));
  }

  @Override
  public CopyObjectOutput copyObjectTo(
      String bucket, String dstBucket, String dstObjectKey,
      String srcObjectKey,
      RequestOptionsBuilder... builders)
      throws TosException {
    return retry(() -> client.copyObjectTo(bucket, dstBucket, dstObjectKey, srcObjectKey, builders));
  }

  @Override
  public CopyObjectOutput copyObjectFrom(
      String bucket, String srcBucket, String srcObjectKey, String dstObjectKey,
      RequestOptionsBuilder... builders)
      throws TosException {
    return retry(() -> client.copyObjectFrom(bucket, srcBucket, srcObjectKey, dstObjectKey, builders));
  }

  @Override
  public UploadPartCopyOutput uploadPartCopy(
      String bucket, UploadPartCopyInput input,
      RequestOptionsBuilder... builders) throws TosException {
    return retry(() -> client.uploadPartCopy(bucket, input, builders));
  }

  @Override
  public PutObjectAclOutput putObjectAcl(String bucket, PutObjectAclInput input) throws TosException {
    return retry(() -> client.putObjectAcl(bucket, input));
  }

  @Override
  public GetObjectAclOutput getObjectAcl(
      String bucket, String objectKey,
      RequestOptionsBuilder... builders)
      throws TosException {
    return retry(() -> client.getObjectAcl(bucket, objectKey, builders));
  }

  @Override
  public CreateMultipartUploadOutput createMultipartUpload(
      String bucket, String objectKey,
      RequestOptionsBuilder... builders)
      throws TosException {
    return retry(() -> client.createMultipartUpload(bucket, objectKey, builders));
  }

  @Override
  public UploadPartOutput uploadPart(
      String bucket, UploadPartInput input,
      RequestOptionsBuilder... builders)
      throws TosException {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public CompleteMultipartUploadOutput completeMultipartUpload(
      String bucket,
      CompleteMultipartUploadInput input)
      throws TosException {
    return retry(() -> client.completeMultipartUpload(bucket, input));
  }

  @Override
  public AbortMultipartUploadOutput abortMultipartUpload(
      String bucket,
      AbortMultipartUploadInput input)
      throws TosException {
    return retry(() -> client.abortMultipartUpload(bucket, input));
  }

  @Override
  public ListUploadedPartsOutput listUploadedParts(
      String bucket,
      ListUploadedPartsInput input,
      RequestOptionsBuilder... builders)
      throws TosException {
    return retry(() -> client.listUploadedParts(bucket, input, builders));
  }

  @Override
  public ListMultipartUploadsOutput listMultipartUploads(
      String bucket,
      ListMultipartUploadsInput input)
      throws TosException {
    return retry(() -> client.listMultipartUploads(bucket, input));
  }

  @Override
  public String preSignedURL(
      String httpMethod, String bucket, String objectKey, Duration ttl,
      RequestOptionsBuilder... builders)
      throws TosException {
    return retry(() -> client.preSignedURL(httpMethod, bucket, objectKey, ttl, builders));
  }

  @Override
  public DeleteBucketPolicyOutput deleteBucketPolicy(DeleteBucketPolicyInput input)
      throws TosException {
    return retry(() -> client.deleteBucketPolicy(input));
  }

  @Override
  public PutBucketCORSOutput putBucketCORS(PutBucketCORSInput input)
      throws TosException {
    return retry(() -> client.putBucketCORS(input));
  }

  @Override
  public GetBucketCORSOutput getBucketCORS(GetBucketCORSInput input)
      throws TosException {
    return retry(() -> client.getBucketCORS(input));
  }

  @Override
  public DeleteBucketCORSOutput deleteBucketCORS(DeleteBucketCORSInput input)
      throws TosException {
    return retry(() -> client.deleteBucketCORS(input));
  }

  @Override
  public PutBucketStorageClassOutput putBucketStorageClass(PutBucketStorageClassInput input)
      throws TosException {
    return retry(() -> client.putBucketStorageClass(input));
  }

  @Override
  public GetBucketLocationOutput getBucketLocation(GetBucketLocationInput input)
      throws TosException {
    return retry(() -> client.getBucketLocation(input));
  }

  @Override
  public PutBucketLifecycleOutput putBucketLifecycle(PutBucketLifecycleInput input)
      throws TosException {
    return retry(() -> client.putBucketLifecycle(input));
  }

  @Override
  public GetBucketLifecycleOutput getBucketLifecycle(GetBucketLifecycleInput input)
      throws TosException {
    return retry(() -> client.getBucketLifecycle(input));
  }

  @Override
  public DeleteBucketLifecycleOutput deleteBucketLifecycle(DeleteBucketLifecycleInput input)
      throws TosException {
    return retry(() -> client.deleteBucketLifecycle(input));
  }

  @Override
  public PutBucketMirrorBackOutput putBucketMirrorBack(PutBucketMirrorBackInput input)
      throws TosException {
    return retry(() -> client.putBucketMirrorBack(input));
  }

  @Override
  public GetBucketMirrorBackOutput getBucketMirrorBack(GetBucketMirrorBackInput input)
      throws TosException {
    return retry(() -> client.getBucketMirrorBack(input));
  }

  @Override
  public DeleteBucketMirrorBackOutput deleteBucketMirrorBack(DeleteBucketMirrorBackInput input)
      throws TosException {
    return retry(() -> client.deleteBucketMirrorBack(input));
  }

  @Override
  public PutBucketReplicationOutput putBucketReplication(PutBucketReplicationInput input)
      throws TosException {
    return retry(() -> client.putBucketReplication(input));
  }

  @Override
  public GetBucketReplicationOutput getBucketReplication(GetBucketReplicationInput input)
      throws TosException {
    return retry(() -> client.getBucketReplication(input));
  }

  @Override
  public DeleteBucketReplicationOutput deleteBucketReplication(DeleteBucketReplicationInput input)
      throws TosException {
    return retry(() -> client.deleteBucketReplication(input));
  }

  @Override
  public PutBucketVersioningOutput putBucketVersioning(PutBucketVersioningInput input)
      throws TosException {
    return retry(() -> client.putBucketVersioning(input));
  }

  @Override
  public GetBucketVersioningOutput getBucketVersioning(GetBucketVersioningInput input)
      throws TosException {
    return retry(() -> client.getBucketVersioning(input));
  }

  @Override
  public PutBucketWebsiteOutput putBucketWebsite(PutBucketWebsiteInput input)
      throws TosException {
    return retry(() -> client.putBucketWebsite(input));
  }

  @Override
  public GetBucketWebsiteOutput getBucketWebsite(GetBucketWebsiteInput input)
      throws TosException {
    return retry(() -> client.getBucketWebsite(input));
  }

  @Override
  public DeleteBucketWebsiteOutput deleteBucketWebsite(DeleteBucketWebsiteInput input)
      throws TosException {
    return retry(() -> client.deleteBucketWebsite(input));
  }

  @Override
  public PutBucketNotificationOutput putBucketNotification(PutBucketNotificationInput input)
      throws TosException {
    return retry(() -> client.putBucketNotification(input));
  }

  @Override
  public GetBucketNotificationOutput getBucketNotification(GetBucketNotificationInput input)
      throws TosException {
    return retry(() -> client.getBucketNotification(input));
  }

  @Override
  public PutBucketNotificationType2Output putBucketNotificationType2(PutBucketNotificationType2Input input)
      throws TosException {
    return retry(() -> client.putBucketNotificationType2(input));
  }

  @Override
  public GetBucketNotificationType2Output getBucketNotificationType2(GetBucketNotificationType2Input input)
      throws TosException {
    return retry(() -> client.getBucketNotificationType2(input));
  }

  @Override
  public PutBucketCustomDomainOutput putBucketCustomDomain(PutBucketCustomDomainInput input)
      throws TosException {
    return retry(() -> client.putBucketCustomDomain(input));
  }

  @Override
  public ListBucketCustomDomainOutput listBucketCustomDomain(ListBucketCustomDomainInput input)
      throws TosException {
    return retry(() -> client.listBucketCustomDomain(input));
  }

  @Override
  public DeleteBucketCustomDomainOutput deleteBucketCustomDomain(DeleteBucketCustomDomainInput input)
      throws TosException {
    return retry(() -> client.deleteBucketCustomDomain(input));
  }

  @Override
  public PutBucketRealTimeLogOutput putBucketRealTimeLog(PutBucketRealTimeLogInput input)
      throws TosException {
    return retry(() -> client.putBucketRealTimeLog(input));
  }

  @Override
  public GetBucketRealTimeLogOutput getBucketRealTimeLog(GetBucketRealTimeLogInput input)
      throws TosException {
    return retry(() -> client.getBucketRealTimeLog(input));
  }

  @Override
  public DeleteBucketRealTimeLogOutput deleteBucketRealTimeLog(DeleteBucketRealTimeLogInput input)
      throws TosException {
    return retry(() -> deleteBucketRealTimeLog(input));
  }

  @Override
  public PutBucketACLOutput putBucketACL(PutBucketACLInput input) throws TosException {
    return retry(() -> client.putBucketACL(input));
  }

  @Override
  public GetBucketACLOutput getBucketACL(GetBucketACLInput input) throws TosException {
    return retry(() -> client.getBucketACL(input));
  }

  @Override
  public PutBucketRenameOutput putBucketRename(PutBucketRenameInput input) throws TosException {
    return retry(() -> client.putBucketRename(input));
  }

  @Override
  public GetBucketRenameOutput getBucketRename(GetBucketRenameInput input) throws TosException {
    return retry(() -> client.getBucketRename(input));
  }

  @Override
  public DeleteBucketRenameOutput deleteBucketRename(DeleteBucketRenameInput input) throws TosException {
    return retry(() -> client.deleteBucketRename(input));
  }

  @Override
  public PutBucketEncryptionOutput putBucketEncryption(PutBucketEncryptionInput input) throws TosException {
    return retry(() -> client.putBucketEncryption(input));
  }

  @Override
  public GetBucketEncryptionOutput getBucketEncryption(GetBucketEncryptionInput input) throws TosException {
    return retry(() -> client.getBucketEncryption(input));
  }

  @Override
  public DeleteBucketEncryptionOutput deleteBucketEncryption(DeleteBucketEncryptionInput input) throws TosException {
    return retry(() -> client.deleteBucketEncryption(input));
  }

  @Override
  public PutBucketTaggingOutput putBucketTagging(PutBucketTaggingInput input) throws TosException {
    return retry(() -> client.putBucketTagging(input));
  }

  @Override
  public GetBucketTaggingOutput getBucketTagging(GetBucketTaggingInput input) throws TosException {
    return retry(() -> client.getBucketTagging(input));
  }

  @Override
  public DeleteBucketTaggingOutput deleteBucketTagging(DeleteBucketTaggingInput input) throws TosException {
    return retry(() -> client.deleteBucketTagging(input));
  }

  @Override
  public PutBucketInventoryOutput putBucketInventory(PutBucketInventoryInput input)
      throws TosException {
    return retry(() -> client.putBucketInventory(input));
  }

  @Override
  public GetBucketInventoryOutput getBucketInventory(GetBucketInventoryInput input) throws TosException {
    return retry(() -> client.getBucketInventory(input));
  }

  @Override
  public ListBucketInventoryOutput listBucketInventory(ListBucketInventoryInput input) throws TosException {
    return retry(() -> client.listBucketInventory(input));
  }

  @Override
  public DeleteBucketInventoryOutput deleteBucketInventory(DeleteBucketInventoryInput input) throws TosException {
    return retry(() -> client.deleteBucketInventory(input));
  }

  @Override
  public GetObjectV2Output getObject(GetObjectV2Input input) throws TosException {
    return retry(() -> client.getObject(input));
  }

  @Override
  public GetObjectToFileOutput getObjectToFile(GetObjectToFileInput input) throws TosException {
    return retry(() -> client.getObjectToFile(input));
  }

  @Override
  public GetFileStatusOutput getFileStatus(GetFileStatusInput input) throws TosException {
    return retry(() -> client.getFileStatus(input));
  }

  @Override
  public UploadFileV2Output uploadFile(UploadFileV2Input input) throws TosException {
    return retry(() -> client.uploadFile(input));
  }

  @Override
  public DownloadFileOutput downloadFile(DownloadFileInput input) throws TosException {
    return retry(() -> client.downloadFile(input));
  }

  @Override
  public ResumableCopyObjectOutput resumableCopyObject(ResumableCopyObjectInput input)
      throws TosException {
    return retry(() -> client.resumableCopyObject(input));
  }

  @Override
  public HeadObjectV2Output headObject(HeadObjectV2Input input) throws TosException {
    return retry(() -> client.headObject(input));
  }

  @Override
  public DeleteObjectOutput deleteObject(DeleteObjectInput input) throws TosException {
    return retry(() -> client.deleteObject(input));
  }

  @Override
  public DeleteMultiObjectsV2Output deleteMultiObjects(DeleteMultiObjectsV2Input input)
      throws TosException {
    return retry(() -> client.deleteMultiObjects(input));
  }

  public PutObjectOutput put(
      String bucket, String key, InputStreamProvider streamProvider,
      long contentLength, ACLType aclType) {
    return retry(() -> client.putObject(newPutObjectRequest(bucket, key, streamProvider, contentLength, aclType)));
  }

  private PutObjectInput newPutObjectRequest(
      String bucket,
      String key,
      InputStreamProvider streamProvider,
      long contentLength,
      ACLType aclType) {

    return PutObjectInput.builder()
        .bucket(bucket)
        .key(key)
        .content(streamProvider.newStream())
        .contentLength(contentLength)
        .options(new ObjectMetaRequestOptions()
            .setAclType(aclType))
        .build();
  }

  public AppendObjectOutput appendObject(String bucket, String key, InputStreamProvider streamProvider,
      long offset, long contentLength, String originalCrc64, ACLType aclType) {
    // originalCrc64 is needed when appending data to object. It should be the object's crc64 checksum if the object
    // exists, and null if the object doesn't exist.
    return retry(() -> client.appendObject(
        newAppendObjectRequest(bucket, key, streamProvider, offset, contentLength, originalCrc64, aclType)));
  }

  private AppendObjectInput newAppendObjectRequest(
      String bucket,
      String key,
      InputStreamProvider streamProvider,
      long offset,
      long contentLength,
      String preCrc64ecma,
      ACLType aclType) {
    return AppendObjectInput.builder()
        .bucket(bucket)
        .key(key)
        .content(streamProvider.newStream())
        .offset(offset)
        .contentLength(contentLength)
        .preHashCrc64ecma(preCrc64ecma)
        .options(new ObjectMetaRequestOptions()
            .setAclType(aclType))
        .build();
  }

  @Override
  public PutObjectOutput putObject(PutObjectInput input) throws TosException {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public PutObjectFromFileOutput putObjectFromFile(PutObjectFromFileInput input)
      throws TosException {
    return retry(() -> client.putObjectFromFile(input));
  }

  @Override
  public AppendObjectOutput appendObject(AppendObjectInput input)
      throws TosException {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public SetObjectMetaOutput setObjectMeta(SetObjectMetaInput input)
      throws TosException {
    return retry(() -> client.setObjectMeta(input));
  }

  @Override
  public ListObjectsV2Output listObjects(ListObjectsV2Input input)
      throws TosException {
    return retry(() -> client.listObjects(input));
  }

  @Override
  public ListObjectsType2Output listObjectsType2(ListObjectsType2Input input)
      throws TosException {
    return retry(() -> client.listObjectsType2(input));
  }

  @Override
  public ListObjectVersionsV2Output listObjectVersions(ListObjectVersionsV2Input input)
      throws TosException {
    return retry(() -> client.listObjectVersions(input));
  }

  @Override
  public CopyObjectV2Output copyObject(CopyObjectV2Input input)
      throws TosException {
    return retry(() -> client.copyObject(input));
  }

  @Override
  public UploadPartCopyV2Output uploadPartCopy(UploadPartCopyV2Input input)
      throws TosException {
    return retry(() -> client.uploadPartCopy(input));
  }

  @Override
  public PutObjectACLOutput putObjectAcl(PutObjectACLInput input)
      throws TosException {
    return retry(() -> client.putObjectAcl(input));
  }

  @Override
  public GetObjectACLV2Output getObjectAcl(GetObjectACLV2Input input)
      throws TosException {
    return retry(() -> client.getObjectAcl(input));
  }

  @Override
  public PutObjectTaggingOutput putObjectTagging(PutObjectTaggingInput input)
      throws TosException {
    return retry(() -> client.putObjectTagging(input));
  }

  @Override
  public GetObjectTaggingOutput getObjectTagging(GetObjectTaggingInput input)
      throws TosException {
    return retry(() -> client.getObjectTagging(input));
  }

  @Override
  public DeleteObjectTaggingOutput deleteObjectTagging(DeleteObjectTaggingInput input)
      throws TosException {
    return retry(() -> client.deleteObjectTagging(input));
  }

  @Override
  public FetchObjectOutput fetchObject(FetchObjectInput input) throws TosException {
    return retry(() -> client.fetchObject(input));
  }

  @Override
  public PutFetchTaskOutput putFetchTask(PutFetchTaskInput input) throws TosException {
    return retry(() -> client.putFetchTask(input));
  }

  @Override
  public GetFetchTaskOutput getFetchTask(GetFetchTaskInput input) throws TosException {
    return retry(() -> client.getFetchTask(input));
  }

  @Override
  public CreateMultipartUploadOutput createMultipartUpload(CreateMultipartUploadInput input)
      throws TosException {
    return retry(() -> client.createMultipartUpload(input));
  }

  public Part uploadPart(
      String bucket,
      String key,
      String uploadId,
      int partNum,
      InputStreamProvider streamProvider,
      long contentLength,
      ACLType aclType) {
    return retry(() -> {
      InputStream in = streamProvider.newStream();
      CountingInputStream countedIn = new CountingInputStream(in);
      UploadPartV2Input request = UploadPartV2Input.builder()
          .bucket(bucket)
          .key(key)
          .partNumber(partNum)
          .uploadID(uploadId)
          .content(countedIn)
          .contentLength(contentLength)
          .options(new ObjectMetaRequestOptions()
              .setAclType(aclType))
          .build();
      UploadPartV2Output output = client.uploadPart(request);
      return new Part(output.getPartNumber(), countedIn.getCount(), output.getEtag());
    });
  }

  @Override
  public UploadPartV2Output uploadPart(UploadPartV2Input input) throws TosException {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public UploadPartFromFileOutput uploadPartFromFile(UploadPartFromFileInput input)
      throws TosException {
    return retry(() -> client.uploadPartFromFile(input));
  }

  @Override
  public CompleteMultipartUploadV2Output completeMultipartUpload(CompleteMultipartUploadV2Input input)
      throws TosException {
    return retry(() -> client.completeMultipartUpload(input));
  }

  @Override
  public AbortMultipartUploadOutput abortMultipartUpload(AbortMultipartUploadInput input)
      throws TosException {
    return retry(() -> client.abortMultipartUpload(input));
  }

  @Override
  public ListPartsOutput listParts(ListPartsInput input) throws TosException {
    return retry(() -> client.listParts(input));
  }

  @Override
  public ListMultipartUploadsV2Output listMultipartUploads(ListMultipartUploadsV2Input input)
      throws TosException {
    return retry(() -> client.listMultipartUploads(input));
  }

  @Override
  public RenameObjectOutput renameObject(RenameObjectInput input) throws TosException {
    return retry(() -> client.renameObject(input));
  }

  @Override
  public RestoreObjectOutput restoreObject(RestoreObjectInput input) throws TosException {
    return retry(() -> client.restoreObject(input));
  }

  @Override
  public PutSymlinkOutput putSymlink(PutSymlinkInput input) throws TosException {
    return retry(() -> client.putSymlink(input));
  }

  @Override
  public GetSymlinkOutput getSymlink(GetSymlinkInput input) throws TosException {
    return retry(() -> client.getSymlink(input));
  }

  @Override
  public PreSignedURLOutput preSignedURL(PreSignedURLInput input) throws TosException {
    return retry(() -> client.preSignedURL(input));
  }

  @Override
  public PreSignedPostSignatureOutput preSignedPostSignature(PreSignedPostSignatureInput input)
      throws TosException {
    return retry(() -> client.preSignedPostSignature(input));
  }

  @Override
  public PreSingedPolicyURLOutput preSingedPolicyURL(PreSingedPolicyURLInput input)
      throws TosException {
    return retry(() -> client.preSingedPolicyURL(input));
  }

  @Override
  public PreSignedPolicyURLOutput preSignedPolicyURL(PreSignedPolicyURLInput input) throws TosException {
    return retry(() -> client.preSignedPolicyURL(input));
  }

  @Override
  public void changeCredentials(Credentials credentials) {
    retry(() -> {
      client.changeCredentials(credentials);
      return null;
    });
  }

  @Override
  public void changeRegionAndEndpoint(String region, String endpoint) {
    retry(() -> {
      client.changeRegionAndEndpoint(region, endpoint);
      return null;
    });
  }

  @Override
  public void changeTransportConfig(TransportConfig config) {
    retry(() -> {
      client.changeTransportConfig(config);
      return null;
    });
  }

  @Override
  public boolean refreshEndpointRegion(String s, String s1) {
    return retry(() -> refreshEndpointRegion(s, s1));
  }

  @Override
  public boolean refreshCredentials(String s, String s1, String s2) {
    return retry(() -> refreshCredentials(s, s1, s2));
  }

  @Override
  public void close() throws IOException {
    client.close();
  }

  private void refresh() throws TosException {
    Credential credential = provider.credential();
    if (credentialIsChanged(credential)) {
      synchronized (this) {
        if (credentialIsChanged(credential)) {
          client.changeCredentials(provider);
          usedCredential = credential;
        }
      }
    }
  }

  private boolean credentialIsChanged(Credential credential) {
    return !Objects.equals(credential.getAccessKeyId(), usedCredential.getAccessKeyId())
        || !Objects.equals(credential.getAccessKeySecret(), usedCredential.getAccessKeySecret())
        || !Objects.equals(credential.getSecurityToken(), usedCredential.getSecurityToken());
  }

  private <T> T retry(Callable<T> callable) {
    int attempt = 0;
    while (true) {
      attempt++;
      try {
        refresh();
        return callable.call();
      } catch (TosException e) {
        if (attempt >= maxRetryTimes) {
          LOG.error("Retry exhausted after {} times.", maxRetryTimes);
          throw e;
        }
        if (isRetryableException(e, nonRetryable409ErrorCodes)) {
          LOG.warn("Retry TOS request in the {} times, error: {}", attempt,
              Throwables.getRootCause(e).getMessage());
          try {
            // last time does not need to sleep
            Thread.sleep(RetryableUtils.backoff(attempt));
          } catch (InterruptedException ex) {
            throw new TosClientException("tos: request interrupted.", ex);
          }
        } else {
          throw e;
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @VisibleForTesting
  static boolean isRetryableException(TosException e, List<String> nonRetryable409ErrorCodes) {
    return e.getStatusCode() >= HttpStatus.INTERNAL_SERVER_ERROR
        || e.getStatusCode() == HttpStatus.TOO_MANY_REQUESTS
        || e.getCause() instanceof SocketException
        || e.getCause() instanceof UnknownHostException
        || e.getCause() instanceof SSLException
        || e.getCause() instanceof SocketTimeoutException
        || e.getCause() instanceof InterruptedException
        || isRetryableTosClientException(e)
        || isRetryableTosServerException(e, nonRetryable409ErrorCodes);
  }

  private static boolean isRetryableTosClientException(TosException e) {
    return e instanceof TosClientException
        && e.getCause() instanceof IOException
        && !(e.getCause() instanceof EOFException);
  }

  private static boolean isRetryableTosServerException(TosException e, List<String> nonRetryable409ErrorCodes) {
    return e instanceof TosServerException
        && e.getStatusCode() == HttpStatus.CONFLICT
        && isRetryableTosConflictException((TosServerException) e, nonRetryable409ErrorCodes);
  }

  private static boolean isRetryableTosConflictException(TosServerException e, List<String> nonRetryableCodes) {
    String errorCode = e.getEc();
    return StringUtils.isEmpty(errorCode) || !nonRetryableCodes.contains(errorCode);
  }
}

