#!/usr/bin/env python3
"""
Monitoring and alerting system for RithmicDataCollector
Provides health checks, performance monitoring, and alerting capabilities
"""

import asyncio
import json
import logging
import os
import psutil
import redis
import smtplib
import time
from datetime import datetime, timedelta
from email.mime.text import MimeText
from email.mime.multipart import MimeMultipart
from typing import Dict, List, Optional, Any
import psycopg2
from psycopg2.extras import RealDictCursor
import requests

class SystemMonitor:
    """System resource monitoring"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
    def get_cpu_usage(self) -> float:
        """Get current CPU usage percentage"""
        return psutil.cpu_percent(interval=1)
    
    def get_memory_usage(self) -> Dict[str, float]:
        """Get memory usage statistics"""
        memory = psutil.virtual_memory()
        return {
            'total': memory.total / (1024**3),  # GB
            'available': memory.available / (1024**3),  # GB
            'used': memory.used / (1024**3),  # GB
            'percentage': memory.percent
        }
    
    def get_disk_usage(self, path: str = '/') -> Dict[str, float]:
        """Get disk usage statistics"""
        try:
            disk = psutil.disk_usage(path)
            return {
                'total': disk.total / (1024**3),  # GB
                'used': disk.used / (1024**3),  # GB
                'free': disk.free / (1024**3),  # GB
                'percentage': (disk.used / disk.total) * 100
            }
        except Exception as e:
            self.logger.error(f"Error getting disk usage: {e}")
            return {'total': 0, 'used': 0, 'free': 0, 'percentage': 0}
    
    def get_network_stats(self) -> Dict[str, int]:
        """Get network I/O statistics"""
        net_io = psutil.net_io_counters()
        return {
            'bytes_sent': net_io.bytes_sent,
            'bytes_recv': net_io.bytes_recv,
            'packets_sent': net_io.packets_sent,
            'packets_recv': net_io.packets_recv
        }
    
    def get_process_info(self, process_name: str) -> Optional[Dict[str, Any]]:
        """Get information about a specific process"""
        for proc in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_percent', 'create_time']):
            try:
                if process_name.lower() in proc.info['name'].lower():
                    return {
                        'pid': proc.info['pid'],
                        'name': proc.info['name'],
                        'cpu_percent': proc.info['cpu_percent'],
                        'memory_percent': proc.info['memory_percent'],
                        'uptime': time.time() - proc.info['create_time']
                    }
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        return None

class DatabaseMonitor:
    """Database monitoring and health checks"""
    
    def __init__(self, db_config: Dict[str, str]):
        self.db_config = db_config
        self.logger = logging.getLogger(__name__)
    
    def get_connection(self):
        """Get database connection"""
        return psycopg2.connect(
            host=self.db_config['host'],
            port=self.db_config['port'],
            database=self.db_config['database'],
            user=self.db_config['user'],
            password=self.db_config['password']
        )
    
    def check_connection(self) -> bool:
        """Check if database is accessible"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute('SELECT 1')
                    return True
        except Exception as e:
            self.logger.error(f"Database connection failed: {e}")
            return False
    
    def get_table_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for main tables"""
        stats = {}
        
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    # Get tick_data stats
                    cursor.execute("""
                        SELECT 
                            COUNT(*) as total_records,
                            COUNT(DISTINCT symbol) as unique_symbols,
                            MIN(timestamp) as earliest_record,
                            MAX(timestamp) as latest_record,
                            pg_size_pretty(pg_total_relation_size('tick_data')) as table_size
                        FROM tick_data
                    """)
                    stats['tick_data'] = dict(cursor.fetchone())
                    
                    # Get level2_data stats
                    cursor.execute("""
                        SELECT 
                            COUNT(*) as total_records,
                            COUNT(DISTINCT symbol) as unique_symbols,
                            MIN(timestamp) as earliest_record,
                            MAX(timestamp) as latest_record,
                            pg_size_pretty(pg_total_relation_size('level2_data')) as table_size
                        FROM level2_data
                    """)
                    stats['level2_data'] = dict(cursor.fetchone())
                    
                    # Get recent activity (last hour)
                    cursor.execute("""
                        SELECT 
                            COUNT(*) as recent_ticks
                        FROM tick_data 
                        WHERE timestamp > NOW() - INTERVAL '1 hour'
                    """)
                    stats['recent_activity'] = dict(cursor.fetchone())
                    
        except Exception as e:
            self.logger.error(f"Error getting table stats: {e}")
            
        return stats
    
    def get_connection_stats(self) -> Dict[str, Any]:
        """Get database connection statistics"""
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT 
                            count(*) as total_connections,
                            count(*) FILTER (WHERE state = 'active') as active_connections,
                            count(*) FILTER (WHERE state = 'idle') as idle_connections
                        FROM pg_stat_activity
                        WHERE datname = current_database()
                    """)
                    return dict(cursor.fetchone())
        except Exception as e:
            self.logger.error(f"Error getting connection stats: {e}")
            return {}
    
    def check_table_health(self) -> Dict[str, bool]:
        """Check health of main tables"""
        health = {}
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Check if tables exist and have recent data
                    tables = ['tick_data', 'level2_data', 'symbol_metadata']
                    
                    for table in tables:
                        try:
                            cursor.execute(f"""
                                SELECT COUNT(*) 
                                FROM {table} 
                                WHERE timestamp > NOW() - INTERVAL '5 minutes'
                            """)
                            count = cursor.fetchone()[0]
                            health[table] = count > 0
                        except Exception:
                            health[table] = False
                            
        except Exception as e:
            self.logger.error(f"Error checking table health: {e}")
            
        return health

class RedisMonitor:
    """Redis monitoring and health checks"""
    
    def __init__(self, redis_config: Dict[str, Any]):
        self.redis_config = redis_config
        self.logger = logging.getLogger(__name__)
        
    def get_client(self):
        """Get Redis client"""
        return redis.Redis(
            host=self.redis_config['host'],
            port=self.redis_config['port'],
            db=self.redis_config['db'],
            decode_responses=True
        )
    
    def check_connection(self) -> bool:
        """Check if Redis is accessible"""
        try:
            client = self.get_client()
            client.ping()
            return True
        except Exception as e:
            self.logger.error(f"Redis connection failed: {e}")
            return False
    
    def get_info(self) -> Dict[str, Any]:
        """Get Redis server information"""
        try:
            client = self.get_client()
            info = client.info()
            return {
                'version': info.get('redis_version'),
                'uptime': info.get('uptime_in_seconds'),
                'connected_clients': info.get('connected_clients'),
                'used_memory': info.get('used_memory_human'),
                'total_commands_processed': info.get('total_commands_processed'),
                'keyspace_hits': info.get('keyspace_hits'),
                'keyspace_misses': info.get('keyspace_misses')
            }
        except Exception as e:
            self.logger.error(f"Error getting Redis info: {e}")
            return {}
    
    def get_queue_stats(self) -> Dict[str, int]:
        """Get statistics for Redis queues"""
        try:
            client = self.get_client()
            return {
                'symbol_requests': client.llen('symbol_requests'),
                'active_symbols': client.scard('active_symbols')
            }
        except Exception as e:
            self.logger.error(f"Error getting queue stats: {e}")
            return {}

class APIMonitor:
    """API endpoint monitoring"""
    
    def __init__(self, api_base_url: str):
        self.api_base_url = api_base_url.rstrip('/')
        self.logger = logging.getLogger(__name__)
    
    def check_health(self) -> bool:
        """Check API health endpoint"""
        try:
            response = requests.get(f"{self.api_base_url}/health", timeout=10)
            return response.status_code == 200 and response.json().get('status') == 'healthy'
        except Exception as e:
            self.logger.error(f"API health check failed: {e}")
            return False
    
    def check_endpoints(self) -> Dict[str, bool]:
        """Check various API endpoints"""
        endpoints = {
            '/health': 'GET',
            '/api/stats': 'GET',
            '/api/symbols': 'GET'
        }
        
        results = {}
        
        for endpoint, method in endpoints.items():
            try:
                if method == 'GET':
                    response = requests.get(f"{self.api_base_url}{endpoint}", timeout=10)
                    results[endpoint] = response.status_code == 200
                else:
                    results[endpoint] = False  # Not implemented
            except Exception as e:
                self.logger.error(f"Error checking {endpoint}: {e}")
                results[endpoint] = False
        
        return results
    
    def measure_response_time(self, endpoint: str = '/health') -> Optional[float]:
        """Measure API response time"""
        try:
            start_time = time.time()
            response = requests.get(f"{self.api_base_url}{endpoint}", timeout=10)
            end_time = time.time()
            
            if response.status_code == 200:
                return (end_time - start_time) * 1000  # milliseconds
            else:
                return None
        except Exception as e:
            self.logger.error(f"Error measuring response time: {e}")
            return None

class AlertManager:
    """Alert management and notification system"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.alert_history = []
        
    def send_email_alert(self, subject: str, message: str, recipients: List[str]):
        """Send email alert"""
        if not self.config.get('email', {}).get('enabled', False):
            return
        
        try:
            smtp_config = self.config['email']
            
            msg = MimeMultipart()
            msg['From'] = smtp_config['from']
            msg['To'] = ', '.join(recipients)
            msg['Subject'] = f"[RithmicDataCollector] {subject}"
            
            msg.attach(MimeText(message, 'plain'))
            
            server = smtplib.SMTP(smtp_config['smtp_server'], smtp_config['smtp_port'])
            if smtp_config.get('use_tls', True):
                server.starttls()
            
            if smtp_config.get('username') and smtp_config.get('password'):
                server.login(smtp_config['username'], smtp_config['password'])
            
            server.send_message(msg)
            server.quit()
            
            self.logger.info(f"Email alert sent: {subject}")
            
        except Exception as e:
            self.logger.error(f"Failed to send email alert: {e}")
    
    def send_webhook_alert(self, alert_data: Dict[str, Any]):
        """Send webhook alert"""
        if not self.config.get('webhook', {}).get('enabled', False):
            return
        
        try:
            webhook_url = self.config['webhook']['url']
            
            payload = {
                'timestamp': datetime.now().isoformat(),
                'service': 'RithmicDataCollector',
                'alert': alert_data
            }
            
            response = requests.post(webhook_url, json=payload, timeout=10)
            response.raise_for_status()
            
            self.logger.info(f"Webhook alert sent: {alert_data['type']}")
            
        except Exception as e:
            self.logger.error(f"Failed to send webhook alert: {e}")
    
    def check_alert_conditions(self, metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Check if any alert conditions are met"""
        alerts = []
        thresholds = self.config.get('thresholds', {})
        
        # CPU usage alert
        if metrics.get('system', {}).get('cpu_usage', 0) > thresholds.get('cpu_usage', 90):
            alerts.append({
                'type': 'high_cpu_usage',
                'severity': 'warning',
                'message': f"CPU usage is {metrics['system']['cpu_usage']:.1f}%",
                'value': metrics['system']['cpu_usage']
            })
        
        # Memory usage alert
        memory_usage = metrics.get('system', {}).get('memory', {}).get('percentage', 0)
        if memory_usage > thresholds.get('memory_usage', 90):
            alerts.append({
                'type': 'high_memory_usage',
                'severity': 'warning',
                'message': f"Memory usage is {memory_usage:.1f}%",
                'value': memory_usage
            })
        
        # Disk usage alert
        disk_usage = metrics.get('system', {}).get('disk', {}).get('percentage', 0)
        if disk_usage > thresholds.get('disk_usage', 85):
            alerts.append({
                'type': 'high_disk_usage',
                'severity': 'warning',
                'message': f"Disk usage is {disk_usage:.1f}%",
                'value': disk_usage
            })
        
        # Database connection alert
        if not metrics.get('database', {}).get('connection_healthy', True):
            alerts.append({
                'type': 'database_connection_failed',
                'severity': 'critical',
                'message': 'Database connection is not healthy',
                'value': False
            })
        
        # Redis connection alert
        if not metrics.get('redis', {}).get('connection_healthy', True):
            alerts.append({
                'type': 'redis_connection_failed',
                'severity': 'critical',
                'message': 'Redis connection is not healthy',
                'value': False
            })
        
        # API health alert
        if not metrics.get('api', {}).get('health_check', True):
            alerts.append({
                'type': 'api_health_failed',
                'severity': 'critical',
                'message': 'API health check failed',
                'value': False
            })
        
        # Data freshness alert
        recent_ticks = metrics.get('database', {}).get('recent_activity', {}).get('recent_ticks', 0)
        if recent_ticks < thresholds.get('min_recent_ticks', 100):
            alerts.append({
                'type': 'low_data_activity',
                'severity': 'warning',
                'message': f"Only {recent_ticks} ticks in the last hour",
                'value': recent_ticks
            })
        
        return alerts
    
    def process_alerts(self, alerts: List[Dict[str, Any]]):
        """Process and send alerts"""
        for alert in alerts:
            # Check if this alert was recently sent to avoid spam
            recent_alert = any(
                a['type'] == alert['type'] and 
                (datetime.now() - a['timestamp']).seconds < 300  # 5 minutes
                for a in self.alert_history
            )
            
            if not recent_alert:
                # Send email alert
                recipients = self.config.get('email', {}).get('recipients', [])
                if recipients:
                    self.send_email_alert(
                        f"{alert['severity'].upper()}: {alert['type']}",
                        alert['message'],
                        recipients
                    )
                
                # Send webhook alert
                self.send_webhook_alert(alert)
                
                # Record alert
                alert['timestamp'] = datetime.now()
                self.alert_history.append(alert)
                
                # Keep only recent alerts in history
                cutoff = datetime.now() - timedelta(hours=1)
                self.alert_history = [
                    a for a in self.alert_history 
                    if a['timestamp'] > cutoff
                ]

class ComprehensiveMonitor:
    """Main monitoring class that coordinates all monitoring components"""
    
    def __init__(self, config_path: str = 'config.json'):
        self.config = self.load_config(config_path)
        self.logger = self.setup_logging()
        
        # Initialize monitors
        self.system_monitor = SystemMonitor()
        self.db_monitor = DatabaseMonitor(self.config['database'])
        self.redis_monitor = RedisMonitor(self.config['redis'])
        self.api_monitor = APIMonitor(f"http://localhost:{self.config['api']['port']}")
        self.alert_manager = AlertManager(self.config.get('monitoring', {}))
        
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
                logging.FileHandler('monitoring.log'),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger(__name__)
    
    def collect_metrics(self) -> Dict[str, Any]:
        """Collect all system metrics"""
        metrics = {
            'timestamp': datetime.now().isoformat(),
            'system': {
                'cpu_usage': self.system_monitor.get_cpu_usage(),
                'memory': self.system_monitor.get_memory_usage(),
                'disk': self.system_monitor.get_disk_usage(),
                'network': self.system_monitor.get_network_stats()
            },
            'database': {
                'connection_healthy': self.db_monitor.check_connection(),
                'table_stats': self.db_monitor.get_table_stats(),
                'connection_stats': self.db_monitor.get_connection_stats(),
                'table_health': self.db_monitor.check_table_health()
            },
            'redis': {
                'connection_healthy': self.redis_monitor.check_connection(),
                'info': self.redis_monitor.get_info(),
                'queue_stats': self.redis_monitor.get_queue_stats()
            },
            'api': {
                'health_check': self.api_monitor.check_health(),
                'endpoints': self.api_monitor.check_endpoints(),
                'response_time': self.api_monitor.measure_response_time()
            }
        }
        
        # Add recent activity to main metrics
        if 'recent_activity' in metrics['database']['table_stats']:
            metrics['database']['recent_activity'] = metrics['database']['table_stats']['recent_activity']
        
        return metrics
    
    def run_monitoring_cycle(self):
        """Run one monitoring cycle"""
        try:
            self.logger.info("Starting monitoring cycle")
            
            # Collect metrics
            metrics = self.collect_metrics()
            
            # Check for alerts
            alerts = self.alert_manager.check_alert_conditions(metrics)
            
            # Process alerts
            if alerts:
                self.logger.warning(f"Found {len(alerts)} alerts")
                self.alert_manager.process_alerts(alerts)
            
            # Log summary
            self.logger.info(f"Monitoring cycle completed. System healthy: {len(alerts) == 0}")
            
            return metrics, alerts
            
        except Exception as e:
            self.logger.error(f"Error in monitoring cycle: {e}")
            return {}, []
    
    async def run_continuous_monitoring(self, interval: int = 60):
        """Run continuous monitoring"""
        self.logger.info(f"Starting continuous monitoring with {interval}s interval")
        
        while True:
            try:
                metrics, alerts = self.run_monitoring_cycle()
                
                # Save metrics to file for historical analysis
                metrics_file = f"metrics_{datetime.now().strftime('%Y%m%d')}.jsonl"
                with open(metrics_file, 'a') as f:
                    f.write(json.dumps(metrics) + '\n')
                
                await asyncio.sleep(interval)
                
            except KeyboardInterrupt:
                self.logger.info("Monitoring stopped by user")
                break
            except Exception as e:
                self.logger.error(f"Error in continuous monitoring: {e}")
                await asyncio.sleep(interval)

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='RithmicDataCollector Monitoring')
    parser.add_argument('--config', default='config.json', help='Configuration file path')
    parser.add_argument('--interval', type=int, default=60, help='Monitoring interval in seconds')
    parser.add_argument('--once', action='store_true', help='Run monitoring once and exit')
    
    args = parser.parse_args()
    
    monitor = ComprehensiveMonitor(args.config)
    
    if args.once:
        metrics, alerts = monitor.run_monitoring_cycle()
        print(json.dumps(metrics, indent=2))
        if alerts:
            print(f"\nAlerts: {len(alerts)}")
            for alert in alerts:
                print(f"  - {alert['type']}: {alert['message']}")
    else:
        asyncio.run(monitor.run_continuous_monitoring(args.interval))

if __name__ == '__main__':
    main()