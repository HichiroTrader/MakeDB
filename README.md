# RithmicDataCollector

A comprehensive real-time data collection system for Rithmic trading platform that captures tick data and Level 2 market depth, stores it in PostgreSQL, and provides a REST API for data access.

## Features

- **Real-time Data Collection**: Collects tick data and Level 2 market depth using `async_rithmic`
- **Flexible Symbol Management**: Support for multiple symbols with dynamic subscription
- **PostgreSQL Storage**: Efficient data storage with optimized indexes
- **REST API**: Flask-based API for data retrieval and symbol management
- **Docker Deployment**: Complete containerized solution with Docker Compose
- **NGINX Reverse Proxy**: Production-ready setup with security headers and rate limiting
- **Redis Integration**: Inter-service communication for dynamic symbol subscriptions
- **Monitoring**: Health checks and statistics endpoints

## Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   NGINX     │────│  Flask API  │────│ PostgreSQL  │
│ (Port 80)   │    │ (Port 5000) │    │ (Port 5432) │
└─────────────┘    └─────────────┘    └─────────────┘
                           │                   │
                           │                   │
                    ┌─────────────┐    ┌─────────────┐
                    │    Redis    │    │  Collector  │
                    │ (Port 6379) │────│   Service   │
                    └─────────────┘    └─────────────┘
                                              │
                                              │
                                    ┌─────────────┐
                                    │   Rithmic   │
                                    │  Platform   │
                                    └─────────────┘
```

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Rithmic trading account with API access
- Git (for cloning the repository)

### 1. Clone and Setup

```bash
git clone <repository-url>
cd RithmicDataCollector

# Copy environment template
cp .env.example .env
```

### 2. Configure Environment

⚠️ **IMPORTANT**: You need valid Rithmic credentials to use this system.

## Rithmic Credentials Setup

⚠️ **IMPORTANT**: The demo credentials in `.env` are NOT valid and will result in "permission denied" errors.

### Getting Valid Rithmic Credentials

1. **Sign up for Rithmic Demo Account**:
   - Visit: https://yyy3.rithmic.com/
   - Click "Sign Up" for a demo account
   - Fill out the registration form
   - Wait for account approval (usually 1-2 business days)

2. **Alternative - Contact Rithmic Directly**:
   - Email: support@rithmic.com
   - Phone: +1 (312) 476-9800
   - Request API access for development/testing

3. **Update Environment Variables**:
   Once you have valid credentials, update your `.env` file:
   ```bash
   # Required: Rithmic API Credentials
   RITHMIC_USER=your_actual_username
   RITHMIC_PASSWORD=your_actual_password
   RITHMIC_SYSTEM_NAME=Rithmic Paper Trading
   RITHMIC_APP_NAME=RithmicDataCollector
   RITHMIC_APP_VERSION=1.0
   RITHMIC_URL=wss://rituz00100.rithmic.com:443

   # Optional: Customize symbols (comma-separated)
   SYMBOLS=ESZ23,CLZ23,NQZ23,YMZ23,RTY23

   # Optional: Database password
   DB_PASSWORD=your_secure_password
   ```

### Testing Your Credentials

After updating credentials, test the connection:
```bash
# Copy test script to container
docker cp test_connection.py rithmic_collector:/app/

# Run connection test
docker-compose exec collector python test_connection.py
```

### Common Issues

- **Error Code 13 (Permission Denied)**: Invalid credentials or account not approved
- **Connection Timeout**: Check URL and network connectivity
- **Invalid System Name**: Try "Rithmic Paper Trading" instead of "Rithmic Test"

### 3. Deploy with Docker Compose

```bash
# Start all services
docker-compose up -d

# Check service status
docker-compose ps

# View logs
docker-compose logs -f collector
docker-compose logs -f api
```

### 4. Verify Installation

```bash
# Health check
curl http://localhost/health

# Check available symbols
curl http://localhost/api/symbols

# Get recent tick data
curl http://localhost/api/ticks/ESZ23?limit=10
```

## API Endpoints

### Health and Status

- `GET /health` - System health check
- `GET /api/stats` - Database statistics
- `GET /api/symbols` - List of symbols with recent data

### Data Retrieval

- `GET /api/ticks/<symbol>?limit=<int>` - Get recent tick data
  - Parameters:
    - `limit`: Number of records (default: 100, max: 1000)
  - Response: JSON array of tick data

- `GET /api/level2/<symbol>?limit=<int>` - Get recent Level 2 data
  - Parameters:
    - `limit`: Number of records (default: 50, max: 500)
  - Response: JSON array of market depth data

### Symbol Management

- `POST /api/subscribe/<symbol>` - Subscribe to new symbol
  - Body: `{"exchange": "CME"}` (optional)
  - Response: `{"status": "subscribed"}`

### Example API Calls

```bash
# Get last 50 ticks for ES contract
curl "http://localhost/api/ticks/ESZ23?limit=50"

# Get Level 2 data for CL contract
curl "http://localhost/api/level2/CLZ23?limit=20"

# Subscribe to new symbol
curl -X POST "http://localhost/api/subscribe/GCZ23" \
     -H "Content-Type: application/json" \
     -d '{"exchange": "COMEX"}'

# Check system statistics
curl "http://localhost/api/stats"
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|----------|
| `RITHMIC_USER` | Rithmic username | Required |
| `RITHMIC_PASSWORD` | Rithmic password | Required |
| `RITHMIC_SYSTEM_NAME` | System name | Rithmic Paper Trading |
| `SYMBOLS` | Initial symbols (comma-separated) | ESZ23,CLZ23 |
| `DB_PASSWORD` | Database password | securepassword |
| `LOG_LEVEL` | Logging level | INFO |

### Configuration File

Edit `config.json` to customize:

- Symbol lists and exchanges
- Database connection settings
- Collector batch sizes and timeouts
- API limits and defaults
- Logging configuration

## Dynamic Symbol Management

### Adding Symbols at Runtime

You can add new symbols without restarting the system:

```bash
# Add a new symbol via API
curl -X POST "http://localhost/api/subscribe/GCZ23" \
     -H "Content-Type: application/json" \
     -d '{"exchange": "COMEX"}'
```

The collector service will automatically:
1. Receive the subscription request via Redis
2. Connect to Rithmic for the new symbol
3. Start collecting tick and Level 2 data
4. Store data in the database

### Monitoring Symbol Status

```bash
# Check which symbols have recent data
curl "http://localhost/api/symbols"

# Response includes:
# - Symbol name and exchange
# - Tick and Level 2 data counts
# - Last update timestamps
```

## AWS EC2 Deployment

### 1. Launch EC2 Instance

```bash
# Launch Ubuntu 20.04 LTS instance
# Recommended: t3.medium or larger
# Security Group: Allow ports 22 (SSH) and 80 (HTTP)
```

### 2. Install Docker

```bash
# Connect to EC2 instance
ssh -i your-key.pem ubuntu@your-ec2-ip

# Install Docker
sudo apt update
sudo apt install -y docker.io docker-compose
sudo usermod -aG docker ubuntu
sudo systemctl enable docker
sudo systemctl start docker

# Logout and login again for group changes
exit
ssh -i your-key.pem ubuntu@your-ec2-ip
```

### 3. Deploy Application

```bash
# Clone repository
git clone <repository-url>
cd RithmicDataCollector

# Configure environment
cp .env.example .env
nano .env  # Edit with your credentials

# Deploy
docker-compose up -d

# Check status
docker-compose ps
docker-compose logs -f
```

### 4. Configure Security Group

```bash
# AWS Console > EC2 > Security Groups
# Add inbound rules:
# - Type: HTTP, Port: 80, Source: 0.0.0.0/0
# - Type: SSH, Port: 22, Source: Your IP
```

### 5. Access Application

```bash
# Health check
curl http://your-ec2-ip/health

# API access
curl http://your-ec2-ip/api/symbols
```

## Production Considerations

### Security

1. **Remove Development Ports**: Comment out exposed ports in `docker-compose.yml`:
   ```yaml
   # ports:
   #   - "5432:5432"  # PostgreSQL
   #   - "6379:6379"  # Redis
   #   - "5000:5000"  # API (use NGINX instead)
   ```

2. **Use Strong Passwords**: Change default database password
3. **Enable HTTPS**: Configure SSL certificates in NGINX
4. **Firewall**: Use AWS Security Groups or iptables
5. **Secrets Management**: Use AWS Secrets Manager or similar

### Monitoring

1. **Health Checks**: Monitor `/health` endpoint
2. **Log Aggregation**: Use CloudWatch or ELK stack
3. **Metrics**: Monitor database size and API response times
4. **Alerts**: Set up alerts for connection failures

### Scaling

1. **Database**: Use RDS for managed PostgreSQL
2. **Redis**: Use ElastiCache for managed Redis
3. **Load Balancing**: Use ALB for multiple API instances
4. **Storage**: Use EBS volumes for persistent data

## Troubleshooting

### Common Issues

1. **Connection Failed to Rithmic**
   ```bash
   # Check credentials in .env file
   # Verify Rithmic system name
   # Check network connectivity
   docker-compose logs collector
   ```

2. **Database Connection Error**
   ```bash
   # Check if PostgreSQL is running
   docker-compose ps db
   # Check database logs
   docker-compose logs db
   ```

3. **API Not Responding**
   ```bash
   # Check API service status
   docker-compose ps api
   # Check API logs
   docker-compose logs api
   # Test direct API access
   curl http://localhost:5000/health
   ```

4. **No Data Being Collected**
   ```bash
   # Check collector logs
   docker-compose logs -f collector
   # Verify symbol subscriptions
   curl http://localhost/api/symbols
   # Check Rithmic connection status
   ```

### Log Analysis

```bash
# View all service logs
docker-compose logs

# Follow specific service logs
docker-compose logs -f collector
docker-compose logs -f api

# Check last 100 lines
docker-compose logs --tail=100 collector
```

### Database Maintenance

```bash
# Connect to database
docker-compose exec db psql -U postgres -d rithmic_db

# Check table sizes
\dt+

# View recent data
SELECT COUNT(*) FROM tick_data WHERE timestamp > NOW() - INTERVAL '1 hour';
SELECT COUNT(*) FROM level2_data WHERE timestamp > NOW() - INTERVAL '1 hour';

# Clean old data (optional)
DELETE FROM tick_data WHERE timestamp < NOW() - INTERVAL '30 days';
DELETE FROM level2_data WHERE timestamp < NOW() - INTERVAL '30 days';
```

## Development

### Local Development Setup

```bash
# Install Python dependencies
pip install -r requirements.txt

# Set environment variables
export RITHMIC_USER=your_username
export RITHMIC_PASSWORD=your_password
# ... other variables

# Run services individually
python collector.py
python api.py
```

### Testing

```bash
# Test API endpoints
python -m pytest tests/

# Manual testing
curl -X POST "http://localhost/api/subscribe/TEST23"
curl "http://localhost/api/ticks/TEST23"
```

## Support

For issues and questions:

1. Check the troubleshooting section
2. Review service logs
3. Verify Rithmic credentials and connectivity
4. Check system resources (CPU, memory, disk)

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Disclaimer

This software is for educational and research purposes. Use at your own risk. The authors are not responsible for any trading losses or system failures.