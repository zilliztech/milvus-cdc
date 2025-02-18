from minio import Minio
from minio.error import S3Error

class MinioSyncer:
    """MinioSyncer provides methods to sync data between Minio buckets.
    
    Supports:
    1. Syncing entire folders with sync_folder()
    2. Syncing specific files with sync_files()
    """
    def __init__(self, src_endpoint, src_access_key, src_secret_key,
                 dst_endpoint=None, dst_access_key=None, dst_secret_key=None,
                 secure=False):
        """Initialize MinioSyncer with source and destination Minio credentials.
        If destination credentials are not provided, they will be same as source.
        """
        self.src_client = Minio(
            src_endpoint,
            access_key=src_access_key,
            secret_key=src_secret_key,
            secure=secure
        )
        
        # If destination credentials not provided, use same as source
        dst_endpoint = dst_endpoint or src_endpoint
        dst_access_key = dst_access_key or src_access_key
        dst_secret_key = dst_secret_key or src_secret_key
        
        self.dst_client = Minio(
            dst_endpoint,
            access_key=dst_access_key,
            secret_key=dst_secret_key,
            secure=secure
        )
    
    def _compare_objects(self, src_stat, dst_stat):
        """Compare source and destination objects using metadata.
        
        Args:
            src_stat: Source object stats from stat_object()
            dst_stat: Destination object stats from stat_object()
            
        Returns:
            bool: True if objects are identical, False if they need sync
        """
        # Compare size
        if src_stat.size != dst_stat.size:
            return False
            
        # Compare etag (MD5 hash)
        if src_stat.etag != dst_stat.etag:
            return False
            
        # Compare last modified time
        if src_stat.last_modified > dst_stat.last_modified:
            return False
            
        return True

    def sync_folder(self, src_bucket, dst_bucket, folder_path):
        """Sync a folder from source bucket to destination bucket.
        
        Args:
            src_bucket (str): Source bucket name
            dst_bucket (str): Destination bucket name
            folder_path (str): Folder path to sync (without leading/trailing slash)
            
        Returns:
            tuple: (success_count, error_count, skipped_count)
        """
        # Ensure folder_path doesn't start/end with slash
        folder_path = folder_path.strip('/')
        if folder_path:
            folder_path += '/'
            
        success_count = 0
        error_count = 0
        skipped_count = 0
        total_objects = 0
        
        try:
            # First count total objects for progress tracking
            print(f"\nScanning objects in {src_bucket}/{folder_path}...")
            objects = list(self.src_client.list_objects(src_bucket, prefix=folder_path, recursive=True))
            total_objects = len(objects)
            print(f"Found {total_objects} objects to scan")
            
            # Now process each object
            for i, obj in enumerate(objects, 1):
                try:
                    print(f"\nProcessing [{i}/{total_objects}] {obj.object_name}")
                    
                    # Get source object stats
                    src_stat = self.src_client.stat_object(src_bucket, obj.object_name)
                    print(f"Source size: {src_stat.size/1024/1024:.2f} MB")
                    
                    # Check if object exists in destination with same metadata
                    try:
                        dst_stat = self.dst_client.stat_object(dst_bucket, obj.object_name)
                        if self._compare_objects(src_stat, dst_stat):
                            print(f"Object {obj.object_name} already synced (identical metadata)")
                            skipped_count += 1
                            continue
                        else:
                            print(f"Object {obj.object_name} exists but needs update")
                    except S3Error as e:
                        if 'NoSuchKey' in str(e) or 'Not Found' in str(e):
                            print(f"Object {obj.object_name} not found in destination")
                        else:
                            raise
                    
                    # Get object data from source and upload
                    print(f"Uploading to {dst_bucket}/{obj.object_name}...")
                    data = self.src_client.get_object(src_bucket, obj.object_name)
                    
                    self.dst_client.put_object(
                        bucket_name=dst_bucket,
                        object_name=obj.object_name,
                        data=data,
                        length=src_stat.size,
                        content_type=src_stat.content_type,
                        metadata=src_stat.metadata
                    )
                    success_count += 1
                    print(f"Successfully synced {obj.object_name}")
                    print(f"Progress: {success_count + error_count + skipped_count}/{total_objects} files processed")
                    
                except S3Error as e:
                    print(f"Error syncing object {obj.object_name}: {str(e)}")
                    error_count += 1
                    continue
                    
        except S3Error as e:
            print(f"Error listing objects in folder {folder_path}: {str(e)}")
            return 0, 1, 0
            
        print("\nSync Summary:")
        print(f"Total objects: {total_objects}")
        print(f"Successfully synced: {success_count}")
        print(f"Skipped (already synced): {skipped_count}")
        print(f"Failed to sync: {error_count}")
        return success_count, error_count, skipped_count

    def sync_files(self, src_bucket, dst_bucket, files):
        """Sync specific files from source bucket to destination bucket.
        
        Args:
            src_bucket (str): Source bucket name
            dst_bucket (str): Destination bucket name
            files (list): List of file paths to sync
            
        Returns:
            tuple: (success_count, error_count, skipped_count)
        """
        success_count = 0
        error_count = 0
        skipped_count = 0
        total_files = len(files)
        
        print(f"\nPreparing to sync {total_files} files...")
        
        for i, file_path in enumerate(files, 1):
            try:
                print(f"\nProcessing [{i}/{total_files}] {file_path}")
                
                # Get source object stats
                src_stat = self.src_client.stat_object(src_bucket, file_path)
                print(f"Source size: {src_stat.size/1024/1024:.2f} MB")
                
                # Check if object exists in destination with same metadata
                try:
                    dst_stat = self.dst_client.stat_object(dst_bucket, file_path)
                    if self._compare_objects(src_stat, dst_stat):
                        print(f"File {file_path} already synced (identical metadata)")
                        skipped_count += 1
                        continue
                    else:
                        print(f"File {file_path} exists but needs update")
                except S3Error as e:
                    if 'NoSuchKey' in str(e) or 'Not Found' in str(e):
                        print(f"File {file_path} not found in destination")
                    else:
                        raise
                
                # Get object data from source and upload
                print(f"Uploading to {dst_bucket}/{file_path}...")
                data = self.src_client.get_object(src_bucket, file_path)
                
                self.dst_client.put_object(
                    bucket_name=dst_bucket,
                    object_name=file_path,
                    data=data,
                    length=src_stat.size,
                    content_type=src_stat.content_type,
                    metadata=src_stat.metadata
                )
                success_count += 1
                print(f"Successfully synced {file_path}")
                print(f"Progress: {success_count + error_count + skipped_count}/{total_files} files processed")
                
            except S3Error as e:
                print(f"Error syncing file {file_path}: {str(e)}")
                error_count += 1
                continue
        
        print("\nSync Summary:")
        print(f"Total files: {total_files}")
        print(f"Successfully synced: {success_count}")
        print(f"Skipped (already synced): {skipped_count}")
        print(f"Failed to sync: {error_count}")
        return success_count, error_count, skipped_count