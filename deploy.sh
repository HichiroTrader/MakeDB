#!/bin/bash

# RithmicDataCollector Deployment Script
# This script automates the deployment process on AWS EC2 or local environments

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PROJECT_NAME="RithmicDataCollector"
DOCKER_COMPOSE_FILE="docker-compose.yml"
PROD_COMPOSE_FILE="docker-compose.prod.yml"
ENV_FILE=".env"
ENV_EXAMPLE=".env.example"

# Functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_requirements() {
    log_info "Checking system requirements..."
    
    # Check if Docker is installed
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    # Check if Docker Compose is installed
    if ! command -v docker-compose &> /dev/null; then
        log_error "Docker Compose is not installed. Please install Docker Compose first."
        exit 1
    fi
    
    # Check if Docker daemon is running
    if ! docker info &> /dev/null; then
        log_error "Docker daemon is not running. Please start Docker first."
        exit 1
    fi
    
    log_success "All requirements satisfied"
}

setup_environment() {
    log_info "Setting up environment..."
    
    # Create .env file if it doesn't exist
    if [ ! -f "$ENV_FILE" ]; then
        if [ -f "$ENV_EXAMPLE" ]; then
            cp "$ENV_EXAMPLE" "$ENV_FILE"
            log_warning "Created $ENV_FILE from $ENV_EXAMPLE. Please edit it with your credentials."
            log_warning "Required variables: RITHMIC_USER, RITHMIC_PASSWORD, DB_PASSWORD"
        else
            log_error "$ENV_EXAMPLE not found. Cannot create environment file."
            exit 1
        fi
    fi
    
    # Check if required environment variables are set
    source "$ENV_FILE"
    
    if [ -z "$RITHMIC_USER" ] || [ -z "$RITHMIC_PASSWORD" ]; then
        log_error "RITHMIC_USER and RITHMIC_PASSWORD must be set in $ENV_FILE"
        exit 1
    fi
    
    if [ -z "$DB_PASSWORD" ]; then
        log_warning "DB_PASSWORD not set. Using default password."
    fi
    
    log_success "Environment setup complete"
}

generate_ssl_cert() {
    log_info "Generating self-signed SSL certificate..."
    
    # Create ssl directory if it doesn't exist
    mkdir -p ssl
    
    # Generate self-signed certificate if it doesn't exist
    if [ ! -f "ssl/cert.pem" ] || [ ! -f "ssl/key.pem" ]; then
        openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
            -keyout ssl/key.pem \
            -out ssl/cert.pem \
            -subj "/C=US/ST=State/L=City/O=Organization/CN=localhost" \
            2>/dev/null || {
            log_warning "OpenSSL not available. SSL certificate not generated."
            log_warning "HTTPS will not be available. Consider installing OpenSSL."
            return 0
        }
        log_success "SSL certificate generated"
    else
        log_info "SSL certificate already exists"
    fi
}

build_images() {
    log_info "Building Docker images..."
    
    if [ "$ENVIRONMENT" = "production" ]; then
        docker-compose -f "$PROD_COMPOSE_FILE" build --no-cache
    else
        docker-compose -f "$DOCKER_COMPOSE_FILE" build --no-cache
    fi
    
    log_success "Docker images built successfully"
}

start_services() {
    log_info "Starting services..."
    
    if [ "$ENVIRONMENT" = "production" ]; then
        docker-compose -f "$PROD_COMPOSE_FILE" up -d
    else
        docker-compose -f "$DOCKER_COMPOSE_FILE" up -d
    fi
    
    log_success "Services started successfully"
}

wait_for_services() {
    log_info "Waiting for services to be ready..."
    
    # Wait for database
    log_info "Waiting for database..."
    timeout=60
    while [ $timeout -gt 0 ]; do
        if docker-compose exec -T db pg_isready -U postgres &> /dev/null; then
            break
        fi
        sleep 2
        timeout=$((timeout - 2))
    done
    
    if [ $timeout -le 0 ]; then
        log_error "Database failed to start within 60 seconds"
        exit 1
    fi
    
    # Wait for API
    log_info "Waiting for API..."
    timeout=60
    while [ $timeout -gt 0 ]; do
        if curl -f http://localhost/health &> /dev/null; then
            break
        fi
        sleep 2
        timeout=$((timeout - 2))
    done
    
    if [ $timeout -le 0 ]; then
        log_error "API failed to start within 60 seconds"
        exit 1
    fi
    
    log_success "All services are ready"
}

run_health_check() {
    log_info "Running health checks..."
    
    # Check API health
    if curl -f http://localhost/health &> /dev/null; then
        log_success "API health check passed"
    else
        log_error "API health check failed"
        return 1
    fi
    
    # Check database connection
    if docker-compose exec -T db psql -U postgres -d rithmic_db -c "SELECT 1;" &> /dev/null; then
        log_success "Database health check passed"
    else
        log_error "Database health check failed"
        return 1
    fi
    
    # Check Redis connection
    if docker-compose exec -T redis redis-cli ping &> /dev/null; then
        log_success "Redis health check passed"
    else
        log_error "Redis health check failed"
        return 1
    fi
    
    log_success "All health checks passed"
}

show_status() {
    log_info "Service status:"
    
    if [ "$ENVIRONMENT" = "production" ]; then
        docker-compose -f "$PROD_COMPOSE_FILE" ps
    else
        docker-compose -f "$DOCKER_COMPOSE_FILE" ps
    fi
    
    echo
    log_info "Available endpoints:"
    echo "  Health Check: http://localhost/health"
    echo "  API Stats:    http://localhost/api/stats"
    echo "  Symbols:      http://localhost/api/symbols"
    echo "  Tick Data:    http://localhost/api/ticks/<symbol>?limit=10"
    echo "  Level 2:      http://localhost/api/level2/<symbol>?limit=10"
    echo "  Subscribe:    curl -X POST http://localhost/api/subscribe/<symbol>"
}

show_logs() {
    log_info "Recent logs:"
    
    if [ "$ENVIRONMENT" = "production" ]; then
        docker-compose -f "$PROD_COMPOSE_FILE" logs --tail=20
    else
        docker-compose -f "$DOCKER_COMPOSE_FILE" logs --tail=20
    fi
}

stop_services() {
    log_info "Stopping services..."
    
    if [ "$ENVIRONMENT" = "production" ]; then
        docker-compose -f "$PROD_COMPOSE_FILE" down
    else
        docker-compose -f "$DOCKER_COMPOSE_FILE" down
    fi
    
    log_success "Services stopped"
}

cleanup() {
    log_info "Cleaning up..."
    
    # Stop and remove containers
    if [ "$ENVIRONMENT" = "production" ]; then
        docker-compose -f "$PROD_COMPOSE_FILE" down -v --remove-orphans
    else
        docker-compose -f "$DOCKER_COMPOSE_FILE" down -v --remove-orphans
    fi
    
    # Remove unused images
    docker image prune -f
    
    log_success "Cleanup complete"
}

install_docker() {
    log_info "Installing Docker..."
    
    # Detect OS
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        OS=$NAME
    else
        log_error "Cannot detect operating system"
        exit 1
    fi
    
    case $OS in
        "Ubuntu"*)
            sudo apt-get update
            sudo apt-get install -y apt-transport-https ca-certificates curl gnupg lsb-release
            curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
            echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
            sudo apt-get update
            sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose
            sudo usermod -aG docker $USER
            ;;
        "Amazon Linux"*)
            sudo yum update -y
            sudo yum install -y docker
            sudo service docker start
            sudo usermod -a -G docker ec2-user
            sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
            sudo chmod +x /usr/local/bin/docker-compose
            ;;
        *)
            log_error "Unsupported operating system: $OS"
            exit 1
            ;;
    esac
    
    log_success "Docker installed successfully"
    log_warning "Please log out and log back in for group changes to take effect"
}

show_help() {
    echo "Usage: $0 [COMMAND] [OPTIONS]"
    echo
    echo "Commands:"
    echo "  deploy          Deploy the application (default)"
    echo "  start           Start services"
    echo "  stop            Stop services"
    echo "  restart         Restart services"
    echo "  status          Show service status"
    echo "  logs            Show service logs"
    echo "  health          Run health checks"
    echo "  cleanup         Stop services and clean up"
    echo "  install-docker  Install Docker and Docker Compose"
    echo "  help            Show this help message"
    echo
    echo "Options:"
    echo "  --prod          Use production configuration"
    echo "  --dev           Use development configuration (default)"
    echo "  --build         Force rebuild of Docker images"
    echo "  --no-ssl        Skip SSL certificate generation"
    echo
    echo "Examples:"
    echo "  $0 deploy --prod          Deploy in production mode"
    echo "  $0 start --dev            Start in development mode"
    echo "  $0 logs                   Show recent logs"
    echo "  $0 cleanup                Clean up everything"
}

# Parse command line arguments
COMMAND="deploy"
ENVIRONMENT="development"
FORCE_BUILD=false
SKIP_SSL=false

while [[ $# -gt 0 ]]; do
    case $1 in
        deploy|start|stop|restart|status|logs|health|cleanup|install-docker|help)
            COMMAND="$1"
            shift
            ;;
        --prod)
            ENVIRONMENT="production"
            shift
            ;;
        --dev)
            ENVIRONMENT="development"
            shift
            ;;
        --build)
            FORCE_BUILD=true
            shift
            ;;
        --no-ssl)
            SKIP_SSL=true
            shift
            ;;
        *)
            log_error "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Main execution
case $COMMAND in
    "deploy")
        log_info "Starting deployment in $ENVIRONMENT mode..."
        check_requirements
        setup_environment
        
        if [ "$ENVIRONMENT" = "production" ] && [ "$SKIP_SSL" = false ]; then
            generate_ssl_cert
        fi
        
        if [ "$FORCE_BUILD" = true ]; then
            build_images
        fi
        
        start_services
        wait_for_services
        run_health_check
        show_status
        log_success "Deployment completed successfully!"
        ;;
    "start")
        check_requirements
        start_services
        wait_for_services
        show_status
        ;;
    "stop")
        stop_services
        ;;
    "restart")
        stop_services
        sleep 2
        start_services
        wait_for_services
        show_status
        ;;
    "status")
        show_status
        ;;
    "logs")
        show_logs
        ;;
    "health")
        run_health_check
        ;;
    "cleanup")
        cleanup
        ;;
    "install-docker")
        install_docker
        ;;
    "help")
        show_help
        ;;
    *)
        log_error "Unknown command: $COMMAND"
        show_help
        exit 1
        ;;
esac