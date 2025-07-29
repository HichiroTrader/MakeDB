#!/usr/bin/env python3
"""
Backup and restore utilities for RithmicDataCollector
Provides automated backup, restore, and data archival capabilities
"""

import asyncio
import gzip
import json
import logging
import os
import shutil
import subprocess
import tarfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any
import psycopg2
from psycopg2.extras import RealDictCursor
import boto3
from botocore.exceptions import ClientError

class DatabaseBackup:
    """Database backup and restore operations"""
    
    def __init__(self, db_config: Dict[str, str], backup_dir: str = 'backups'):
        self.db_config = db_config
        self.backup_dir = Path(backup_dir)
        self.backup_dir.mkdir(exist_ok=True)
        self.logger = logging.getLogger(__name__)
        
    def create_backup(self, backup_name: Optional[str] = None) -> str:
        """Create a full database backup using pg_dump"""
        if not backup_name:
            backup_name = f"rithmic_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        backup_file = self.backup_dir / f"{backup_name}.sql"
        compressed_file = self.backup_dir / f"{backup_name}.sql.gz"
        
        try:
            # Create pg_dump command
            cmd = [
                'pg_dump',
                '-h', self.db_config['host'],
                '-p', str(self.db_config['port']),
                '-U', self.db_config['user'],
                '-d', self.db_config['database'],
                '--verbose',
                '--no-password',
                '--format=custom',
                '--compress=9',
                '-f', str(backup_file)
            ]
            
            # Set password via environment variable
            env = os.environ.copy()
            env['PGPASSWORD'] = self.db_config['password']
            
            self.logger.info(f"Starting database backup: {backup_name}")
            
            # Execute pg_dump
            result = subprocess.run(
                cmd,
                env=env,
                capture_output=True,
                text=True,
                timeout=3600  # 1 hour timeout
            )
            
            if result.returncode != 0:
                raise Exception(f"pg_dump failed: {result.stderr}")
            
            # Compress the backup file
            with open(backup_file, 'rb') as f_in:
                with gzip.open(compressed_file, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            # Remove uncompressed file
            backup_file.unlink()
            
            # Get file size
            file_size = compressed_file.stat().st_size
            
            self.logger.info(f"Backup completed: {compressed_file} ({file_size / 1024 / 1024:.1f} MB)")
            
            return str(compressed_file)
            
        except Exception as e:
            self.logger.error(f"Backup failed: {e}")
            # Clean up partial files
            for file_path in [backup_file, compressed_file]:
                if file_path.exists():
                    file_path.unlink()
            raise
    
    def restore_backup(self, backup_file: str, target_db: Optional[str] = None) -> bool:
        """Restore database from backup file"""
        backup_path = Path(backup_file)
        
        if not backup_path.exists():
            raise FileNotFoundError(f"Backup file not found: {backup_file}")
        
        target_database = target_db or self.db_config['database']
        
        try:
            # If compressed, decompress first
            if backup_path.suffix == '.gz':
                temp_file = backup_path.with_suffix('')
                with gzip.open(backup_path, 'rb') as f_in:
                    with open(temp_file, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                restore_file = temp_file
            else:
                restore_file = backup_path
            
            # Create pg_restore command
            cmd = [
                'pg_restore',
                '-h', self.db_config['host'],
                '-p', str(self.db_config['port']),
                '-U', self.db_config['user'],
                '-d', target_database,
                '--verbose',
                '--no-password',
                '--clean',
                '--if-exists',
                str(restore_file)
            ]
            
            # Set password via environment variable
            env = os.environ.copy()
            env['PGPASSWORD'] = self.db_config['password']
            
            self.logger.info(f"Starting database restore to {target_database}")
            
            # Execute pg_restore
            result = subprocess.run(
                cmd,
                env=env,
                capture_output=True,
                text=True,
                timeout=3600  # 1 hour timeout
            )
            
            # Clean up temporary file
            if backup_path.suffix == '.gz' and restore_file.exists():
                restore_file.unlink()
            
            if result.returncode != 0:
                self.logger.warning(f"pg_restore completed with warnings: {result.stderr}")
            
            self.logger.info(f"Restore completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Restore failed: {e}")
            return False
    
    def create_incremental_backup(self, since: datetime) -> str:
        """Create incremental backup of data since specified time"""
        backup_name = f"incremental_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        backup_file = self.backup_dir / f"{backup_name}.json.gz"
        
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    data = {}
                    
                    # Export tick data since specified time
                    cursor.execute("""
                        SELECT * FROM tick_data 
                        WHERE timestamp > %s 
                        ORDER BY timestamp
                    """, (since,))
                    data['tick_data'] = [dict(row) for row in cursor.fetchall()]
                    
                    # Export level2 data since specified time
                    cursor.execute("""
                        SELECT * FROM level2_data 
                        WHERE timestamp > %s 
                        ORDER BY timestamp
                    """, (since,))
                    data['level2_data'] = [dict(row) for row in cursor.fetchall()]
                    
                    # Export symbol metadata
                    cursor.execute("SELECT * FROM symbol_metadata")
                    data['symbol_metadata'] = [dict(row) for row in cursor.fetchall()]
                    
                    # Add metadata
                    data['backup_info'] = {
                        'type': 'incremental',
                        'since': since.isoformat(),
                        'created_at': datetime.now().isoformat(),
                        'record_counts': {
                            'tick_data': len(data['tick_data']),
                            'level2_data': len(data['level2_data']),
                            'symbol_metadata': len(data['symbol_metadata'])
                        }
                    }
            
            # Write compressed JSON
            with gzip.open(backup_file, 'wt', encoding='utf-8') as f:
                json.dump(data, f, default=str, indent=2)
            
            file_size = backup_file.stat().st_size
            record_count = sum(data['backup_info']['record_counts'].values())
            
            self.logger.info(
                f"Incremental backup completed: {backup_file} "
                f"({file_size / 1024 / 1024:.1f} MB, {record_count} records)"
            )
            
            return str(backup_file)
            
        except Exception as e:
            self.logger.error(f"Incremental backup failed: {e}")
            if backup_file.exists():
                backup_file.unlink()
            raise
    
    def list_backups(self) -> List[Dict[str, Any]]:
        """List available backup files"""
        backups = []
        
        for backup_file in self.backup_dir.glob('*.sql.gz'):
            stat = backup_file.stat()
            backups.append({
                'name': backup_file.name,
                'path': str(backup_file),
                'size': stat.st_size,
                'created': datetime.fromtimestamp(stat.st_mtime),
                'type': 'full'
            })
        
        for backup_file in self.backup_dir.glob('*.json.gz'):
            stat = backup_file.stat()
            backups.append({
                'name': backup_file.name,
                'path': str(backup_file),
                'size': stat.st_size,
                'created': datetime.fromtimestamp(stat.st_mtime),
                'type': 'incremental'
            })
        
        return sorted(backups, key=lambda x: x['created'], reverse=True)
    
    def cleanup_old_backups(self, keep_days: int = 30, keep_count: int = 10):
        """Clean up old backup files"""
        backups = self.list_backups()
        cutoff_date = datetime.now() - timedelta(days=keep_days)
        
        # Keep recent backups and a minimum count
        to_delete = []
        for i, backup in enumerate(backups):
            if i >= keep_count and backup['created'] < cutoff_date:
                to_delete.append(backup)
        
        for backup in to_delete:
            try:
                Path(backup['path']).unlink()
                self.logger.info(f"Deleted old backup: {backup['name']}")
            except Exception as e:
                self.logger.error(f"Failed to delete backup {backup['name']}: {e}")
        
        if to_delete:
            self.logger.info(f"Cleaned up {len(to_delete)} old backups")

class S3Backup:
    """AWS S3 backup operations"""
    
    def __init__(self, aws_config: Dict[str, str]):
        self.aws_config = aws_config
        self.logger = logging.getLogger(__name__)
        
        # Initialize S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_config.get('access_key_id'),
            aws_secret_access_key=aws_config.get('secret_access_key'),
            region_name=aws_config.get('region', 'us-east-1')
        )
        
        self.bucket_name = aws_config['bucket_name']
        self.prefix = aws_config.get('prefix', 'rithmic-backups/')
    
    def upload_backup(self, local_file: str, s3_key: Optional[str] = None) -> str:
        """Upload backup file to S3"""
        local_path = Path(local_file)
        
        if not s3_key:
            s3_key = f"{self.prefix}{local_path.name}"
        
        try:
            self.logger.info(f"Uploading {local_file} to s3://{self.bucket_name}/{s3_key}")
            
            # Upload with metadata
            extra_args = {
                'Metadata': {
                    'source': 'rithmic-data-collector',
                    'created': datetime.now().isoformat(),
                    'original_name': local_path.name
                }
            }
            
            self.s3_client.upload_file(
                str(local_path),
                self.bucket_name,
                s3_key,
                ExtraArgs=extra_args
            )
            
            self.logger.info(f"Upload completed: s3://{self.bucket_name}/{s3_key}")
            return s3_key
            
        except ClientError as e:
            self.logger.error(f"S3 upload failed: {e}")
            raise
    
    def download_backup(self, s3_key: str, local_file: str) -> bool:
        """Download backup file from S3"""
        try:
            self.logger.info(f"Downloading s3://{self.bucket_name}/{s3_key} to {local_file}")
            
            self.s3_client.download_file(
                self.bucket_name,
                s3_key,
                local_file
            )
            
            self.logger.info(f"Download completed: {local_file}")
            return True
            
        except ClientError as e:
            self.logger.error(f"S3 download failed: {e}")
            return False
    
    def list_s3_backups(self) -> List[Dict[str, Any]]:
        """List backup files in S3"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=self.prefix
            )
            
            backups = []
            for obj in response.get('Contents', []):
                backups.append({
                    'key': obj['Key'],
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'],
                    'storage_class': obj.get('StorageClass', 'STANDARD')
                })
            
            return sorted(backups, key=lambda x: x['last_modified'], reverse=True)
            
        except ClientError as e:
            self.logger.error(f"Failed to list S3 backups: {e}")
            return []
    
    def cleanup_s3_backups(self, keep_days: int = 90):
        """Clean up old S3 backups"""
        backups = self.list_s3_backups()
        cutoff_date = datetime.now(backups[0]['last_modified'].tzinfo) - timedelta(days=keep_days)
        
        to_delete = [backup for backup in backups if backup['last_modified'] < cutoff_date]
        
        if not to_delete:
            self.logger.info("No old S3 backups to clean up")
            return
        
        # Delete objects in batches
        for i in range(0, len(to_delete), 1000):  # S3 delete limit is 1000 objects
            batch = to_delete[i:i+1000]
            delete_objects = [{'Key': backup['key']} for backup in batch]
            
            try:
                response = self.s3_client.delete_objects(
                    Bucket=self.bucket_name,
                    Delete={'Objects': delete_objects}
                )
                
                deleted_count = len(response.get('Deleted', []))
                self.logger.info(f"Deleted {deleted_count} old S3 backups")
                
            except ClientError as e:
                self.logger.error(f"Failed to delete S3 backups: {e}")

class DataArchiver:
    """Data archival operations for old data"""
    
    def __init__(self, db_config: Dict[str, str], archive_dir: str = 'archives'):
        self.db_config = db_config
        self.archive_dir = Path(archive_dir)
        self.archive_dir.mkdir(exist_ok=True)
        self.logger = logging.getLogger(__name__)
    
    def archive_old_data(self, older_than_days: int = 30) -> Dict[str, int]:
        """Archive data older than specified days"""
        cutoff_date = datetime.now() - timedelta(days=older_than_days)
        archive_name = f"archive_{cutoff_date.strftime('%Y%m%d')}"
        archive_file = self.archive_dir / f"{archive_name}.tar.gz"
        
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    # Get old data
                    cursor.execute("""
                        SELECT * FROM tick_data 
                        WHERE timestamp < %s 
                        ORDER BY timestamp
                    """, (cutoff_date,))
                    old_tick_data = [dict(row) for row in cursor.fetchall()]
                    
                    cursor.execute("""
                        SELECT * FROM level2_data 
                        WHERE timestamp < %s 
                        ORDER BY timestamp
                    """, (cutoff_date,))
                    old_level2_data = [dict(row) for row in cursor.fetchall()]
                    
                    if not old_tick_data and not old_level2_data:
                        self.logger.info("No old data to archive")
                        return {'tick_data': 0, 'level2_data': 0}
                    
                    # Create archive data structure
                    archive_data = {
                        'archive_info': {
                            'created_at': datetime.now().isoformat(),
                            'cutoff_date': cutoff_date.isoformat(),
                            'record_counts': {
                                'tick_data': len(old_tick_data),
                                'level2_data': len(old_level2_data)
                            }
                        },
                        'tick_data': old_tick_data,
                        'level2_data': old_level2_data
                    }
                    
                    # Create temporary JSON file
                    temp_json = self.archive_dir / f"{archive_name}.json"
                    with open(temp_json, 'w') as f:
                        json.dump(archive_data, f, default=str, indent=2)
                    
                    # Create compressed archive
                    with tarfile.open(archive_file, 'w:gz') as tar:
                        tar.add(temp_json, arcname=f"{archive_name}.json")
                    
                    # Remove temporary file
                    temp_json.unlink()
                    
                    # Delete archived data from database
                    cursor.execute("DELETE FROM tick_data WHERE timestamp < %s", (cutoff_date,))
                    deleted_ticks = cursor.rowcount
                    
                    cursor.execute("DELETE FROM level2_data WHERE timestamp < %s", (cutoff_date,))
                    deleted_level2 = cursor.rowcount
                    
                    conn.commit()
                    
                    self.logger.info(
                        f"Archived {deleted_ticks} tick records and {deleted_level2} level2 records "
                        f"to {archive_file}"
                    )
                    
                    return {'tick_data': deleted_ticks, 'level2_data': deleted_level2}
                    
        except Exception as e:
            self.logger.error(f"Data archival failed: {e}")
            if archive_file.exists():
                archive_file.unlink()
            raise
    
    def restore_archived_data(self, archive_file: str) -> bool:
        """Restore data from archive file"""
        archive_path = Path(archive_file)
        
        if not archive_path.exists():
            raise FileNotFoundError(f"Archive file not found: {archive_file}")
        
        try:
            # Extract archive
            with tarfile.open(archive_path, 'r:gz') as tar:
                tar.extractall(self.archive_dir)
            
            # Find extracted JSON file
            json_file = None
            for extracted_file in self.archive_dir.glob('*.json'):
                if extracted_file.stem in archive_path.stem:
                    json_file = extracted_file
                    break
            
            if not json_file:
                raise Exception("No JSON data file found in archive")
            
            # Load archived data
            with open(json_file, 'r') as f:
                archive_data = json.load(f)
            
            # Restore to database
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor() as cursor:
                    # Restore tick data
                    for record in archive_data['tick_data']:
                        cursor.execute("""
                            INSERT INTO tick_data 
                            (timestamp, symbol, price, volume, direction, trade_type, exchange)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT DO NOTHING
                        """, (
                            record['timestamp'], record['symbol'], record['price'],
                            record['volume'], record['direction'], record['trade_type'],
                            record['exchange']
                        ))
                    
                    # Restore level2 data
                    for record in archive_data['level2_data']:
                        cursor.execute("""
                            INSERT INTO level2_data 
                            (timestamp, symbol, update_type, bids, asks, depth)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT DO NOTHING
                        """, (
                            record['timestamp'], record['symbol'], record['update_type'],
                            json.dumps(record['bids']), json.dumps(record['asks']),
                            record['depth']
                        ))
                    
                    conn.commit()
            
            # Clean up extracted file
            json_file.unlink()
            
            restored_counts = archive_data['archive_info']['record_counts']
            self.logger.info(
                f"Restored {restored_counts['tick_data']} tick records and "
                f"{restored_counts['level2_data']} level2 records from {archive_file}"
            )
            
            return True
            
        except Exception as e:
            self.logger.error(f"Archive restore failed: {e}")
            return False

class BackupManager:
    """Main backup management class"""
    
    def __init__(self, config_path: str = 'config.json'):
        self.config = self.load_config(config_path)
        self.logger = self.setup_logging()
        
        # Initialize backup components
        self.db_backup = DatabaseBackup(self.config['database'])
        
        # Initialize S3 backup if configured
        self.s3_backup = None
        if self.config.get('aws', {}).get('enabled', False):
            self.s3_backup = S3Backup(self.config['aws'])
        
        # Initialize archiver
        self.archiver = DataArchiver(self.config['database'])
    
    def load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from file"""
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading config: {e}")
            return {}
    
    def setup_logging(self) -> logging.Logger:
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('backup.log'),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger(__name__)
    
    def run_full_backup(self, upload_to_s3: bool = True) -> str:
        """Run complete backup process"""
        try:
            # Create database backup
            backup_file = self.db_backup.create_backup()
            
            # Upload to S3 if configured
            if upload_to_s3 and self.s3_backup:
                self.s3_backup.upload_backup(backup_file)
            
            # Clean up old local backups
            self.db_backup.cleanup_old_backups()
            
            # Clean up old S3 backups
            if self.s3_backup:
                self.s3_backup.cleanup_s3_backups()
            
            return backup_file
            
        except Exception as e:
            self.logger.error(f"Full backup failed: {e}")
            raise
    
    def run_incremental_backup(self, since_hours: int = 24) -> str:
        """Run incremental backup"""
        since = datetime.now() - timedelta(hours=since_hours)
        return self.db_backup.create_incremental_backup(since)
    
    def run_data_archival(self, older_than_days: int = 30) -> Dict[str, int]:
        """Run data archival process"""
        return self.archiver.archive_old_data(older_than_days)
    
    async def run_scheduled_backup(self, interval_hours: int = 24):
        """Run scheduled backup process"""
        self.logger.info(f"Starting scheduled backup with {interval_hours}h interval")
        
        while True:
            try:
                # Run full backup
                backup_file = self.run_full_backup()
                self.logger.info(f"Scheduled backup completed: {backup_file}")
                
                # Wait for next backup
                await asyncio.sleep(interval_hours * 3600)
                
            except KeyboardInterrupt:
                self.logger.info("Scheduled backup stopped by user")
                break
            except Exception as e:
                self.logger.error(f"Scheduled backup failed: {e}")
                await asyncio.sleep(3600)  # Wait 1 hour before retry

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='RithmicDataCollector Backup Manager')
    parser.add_argument('--config', default='config.json', help='Configuration file path')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Full backup command
    backup_parser = subparsers.add_parser('backup', help='Create full backup')
    backup_parser.add_argument('--no-s3', action='store_true', help='Skip S3 upload')
    
    # Incremental backup command
    inc_parser = subparsers.add_parser('incremental', help='Create incremental backup')
    inc_parser.add_argument('--hours', type=int, default=24, help='Hours since last backup')
    
    # Restore command
    restore_parser = subparsers.add_parser('restore', help='Restore from backup')
    restore_parser.add_argument('backup_file', help='Backup file to restore')
    restore_parser.add_argument('--target-db', help='Target database name')
    
    # Archive command
    archive_parser = subparsers.add_parser('archive', help='Archive old data')
    archive_parser.add_argument('--days', type=int, default=30, help='Archive data older than N days')
    
    # List command
    list_parser = subparsers.add_parser('list', help='List available backups')
    list_parser.add_argument('--s3', action='store_true', help='List S3 backups')
    
    # Scheduled backup command
    scheduled_parser = subparsers.add_parser('scheduled', help='Run scheduled backups')
    scheduled_parser.add_argument('--interval', type=int, default=24, help='Backup interval in hours')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    manager = BackupManager(args.config)
    
    if args.command == 'backup':
        backup_file = manager.run_full_backup(upload_to_s3=not args.no_s3)
        print(f"Backup created: {backup_file}")
    
    elif args.command == 'incremental':
        backup_file = manager.run_incremental_backup(args.hours)
        print(f"Incremental backup created: {backup_file}")
    
    elif args.command == 'restore':
        success = manager.db_backup.restore_backup(args.backup_file, args.target_db)
        if success:
            print("Restore completed successfully")
        else:
            print("Restore failed")
    
    elif args.command == 'archive':
        counts = manager.run_data_archival(args.days)
        print(f"Archived {counts['tick_data']} tick records and {counts['level2_data']} level2 records")
    
    elif args.command == 'list':
        if args.s3 and manager.s3_backup:
            backups = manager.s3_backup.list_s3_backups()
            print("S3 Backups:")
            for backup in backups:
                print(f"  {backup['key']} ({backup['size'] / 1024 / 1024:.1f} MB) - {backup['last_modified']}")
        else:
            backups = manager.db_backup.list_backups()
            print("Local Backups:")
            for backup in backups:
                print(f"  {backup['name']} ({backup['size'] / 1024 / 1024:.1f} MB) - {backup['created']}")
    
    elif args.command == 'scheduled':
        asyncio.run(manager.run_scheduled_backup(args.interval))

if __name__ == '__main__':
    main()