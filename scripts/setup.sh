#!/bin/bash

# Databricks Sales Pipeline - Setup Script
# This script helps with initial configuration and deployment

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Print functions
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_header() {
    echo ""
    echo "================================"
    echo "$1"
    echo "================================"
}

# Check prerequisites
check_prerequisites() {
    print_header "Checking Prerequisites"
    
    # Check Terraform
    if ! command -v terraform &> /dev/null; then
        print_error "Terraform not found. Please install: https://www.terraform.io/downloads"
        exit 1
    fi
    print_info "✓ Terraform installed: $(terraform version -json | jq -r '.terraform_version')"
    
    # Check AWS CLI
    if ! command -v aws &> /dev/null; then
        print_error "AWS CLI not found. Please install: https://aws.amazon.com/cli/"
        exit 1
    fi
    print_info "✓ AWS CLI installed: $(aws --version)"
    
    # Check jq
    if ! command -v jq &> /dev/null; then
        print_warning "jq not found. Installing..."
        if [[ "$OSTYPE" == "darwin"* ]]; then
            brew install jq
        else
            sudo apt-get install -y jq
        fi
    fi
    print_info "✓ jq installed"
    
    # Check AWS credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        print_error "AWS credentials not configured. Run: aws configure"
        exit 1
    fi
    print_info "✓ AWS credentials configured"
    
    print_info "All prerequisites met!"
}

# Setup environment file
setup_environment() {
    print_header "Setting Up Environment"
    
    if [ -f .env ]; then
        print_warning ".env file already exists. Backing up..."
        cp .env .env.backup
    fi
    
    cat > .env << 'EOF'
# Databricks Configuration
export DATABRICKS_HOST=""
export DATABRICKS_TOKEN=""
export TF_VAR_databricks_host=$DATABRICKS_HOST
export TF_VAR_databricks_token=$DATABRICKS_TOKEN

# AWS Configuration
export AWS_DEFAULT_REGION="eu-central-1"
# AWS credentials should be configured via 'aws configure'

# Project Configuration
export TF_VAR_project_name="sales-pipeline"
export TF_VAR_environment="prod"
EOF
    
    print_info "Created .env file. Please edit it with your credentials:"
    print_info "  nano .env"
    echo ""
    print_warning "Don't forget to source it: source .env"
}

# Create Terraform backend resources
setup_terraform_backend() {
    print_header "Setting Up Terraform Backend"
    
    BUCKET_NAME="hema-sales-terraform-state"
    TABLE_NAME="terraform-state-lock"
    REGION="eu-central-1"
    
    # Check if bucket exists
    if aws s3 ls "s3://${BUCKET_NAME}" 2>&1 | grep -q 'NoSuchBucket'; then
        print_info "Creating S3 bucket for Terraform state..."
        aws s3 mb "s3://${BUCKET_NAME}" --region ${REGION}
        
        # Enable versioning
        aws s3api put-bucket-versioning \
            --bucket ${BUCKET_NAME} \
            --versioning-configuration Status=Enabled
        
        # Enable encryption
        aws s3api put-bucket-encryption \
            --bucket ${BUCKET_NAME} \
            --server-side-encryption-configuration '{
                "Rules": [{
                    "ApplyServerSideEncryptionByDefault": {
                        "SSEAlgorithm": "AES256"
                    }
                }]
            }'
        
        print_info "✓ S3 bucket created and configured"
    else
        print_info "✓ S3 bucket already exists"
    fi
    
    # Check if DynamoDB table exists
    if ! aws dynamodb describe-table --table-name ${TABLE_NAME} --region ${REGION} &> /dev/null; then
        print_info "Creating DynamoDB table for state locking..."
        aws dynamodb create-table \
            --table-name ${TABLE_NAME} \
            --attribute-definitions AttributeName=LockID,AttributeType=S \
            --key-schema AttributeName=LockID,KeyType=HASH \
            --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 \
            --region ${REGION}
        
        print_info "Waiting for table to be active..."
        aws dynamodb wait table-exists --table-name ${TABLE_NAME} --region ${REGION}
        print_info "✓ DynamoDB table created"
    else
        print_info "✓ DynamoDB table already exists"
    fi
}

# Setup Terraform configuration
setup_terraform_config() {
    print_header "Setting Up Terraform Configuration"
    
    cd infra/terraform
    
    if [ ! -f terraform.tfvars ]; then
        print_info "Creating terraform.tfvars from example..."
        cp terraform.tfvars.example terraform.tfvars
        print_warning "Please edit terraform.tfvars with your values:"
        print_info "  nano infra/terraform/terraform.tfvars"
    else
        print_info "✓ terraform.tfvars already exists"
    fi
    
    cd ../..
}

# Validate Databricks connection
validate_databricks() {
    print_header "Validating Databricks Connection"
    
    if [ -z "$DATABRICKS_HOST" ] || [ -z "$DATABRICKS_TOKEN" ]; then
        print_error "Databricks credentials not set. Please configure .env and source it."
        return 1
    fi
    
    print_info "Testing connection to: $DATABRICKS_HOST"
    
    # Simple API call to test credentials
    response=$(curl -s -o /dev/null -w "%{http_code}" \
        -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
        "${DATABRICKS_HOST}/api/2.0/clusters/list")
    
    if [ "$response" = "200" ]; then
        print_info "✓ Databricks connection successful"
        return 0
    else
        print_error "Databricks connection failed (HTTP $response)"
        return 1
    fi
}

# Initialize Terraform
initialize_terraform() {
    print_header "Initializing Terraform"
    
    cd infra/terraform
    
    print_info "Running terraform init -reconfigure..."
    terraform init -reconfigure
    
    print_info "Running terraform validate..."
    terraform validate
    
    print_info "Running terraform fmt..."
    terraform fmt -recursive
    
    print_info "✓ Terraform initialized successfully"
    
    cd ../..
}

# Upload source data to S3
upload_source_data() {
    print_header "Uploading Source Data"
    
    BUCKET_NAME="hema-catalog-data"
    
    # Check if bucket exists
    if aws s3 ls "s3://${BUCKET_NAME}" 2>&1 | grep -q 'NoSuchBucket'; then
        print_warning "Data bucket doesn't exist yet. Run terraform apply first."
        return 0
    fi
    
    if [ -d "data" ]; then
        print_info "Uploading data files to S3..."
        aws s3 sync ./data/ "s3://${BUCKET_NAME}/source/" \
            --exclude "*.md" \
            --exclude "*.txt" \
            --exclude "README*"
        print_info "✓ Data uploaded successfully"
    else
        print_warning "data/ directory not found. Skipping data upload."
    fi
}

# Main menu
show_menu() {
    clear
    echo "╔════════════════════════════════════════════════════╗"
    echo "║   Databricks Sales Pipeline - Setup Wizard        ║"
    echo "╚════════════════════════════════════════════════════╝"
    echo ""
    echo "1) Check Prerequisites"
    echo "2) Setup Environment (.env file)"
    echo "3) Setup Terraform Backend (S3 + DynamoDB)"
    echo "4) Setup Terraform Configuration"
    echo "5) Validate Databricks Connection"
    echo "6) Initialize Terraform"
    echo "7) Upload Source Data to S3"
    echo "8) Run Complete Setup (All steps)"
    echo "9) Deploy Infrastructure (terraform apply)"
    echo "0) Exit"
    echo ""
    read -p "Select option: " choice
    
    case $choice in
        1) check_prerequisites ;;
        2) setup_environment ;;
        3) setup_terraform_backend ;;
        4) setup_terraform_config ;;
        5) validate_databricks ;;
        6) initialize_terraform ;;
        7) upload_source_data ;;
        8) run_complete_setup ;;
        9) deploy_infrastructure ;;
        0) exit 0 ;;
        *) print_error "Invalid option" ;;
    esac
    
    echo ""
    read -p "Press Enter to continue..."
}

# Run complete setup
run_complete_setup() {
    print_header "Running Complete Setup"
    
    check_prerequisites
    setup_environment
    
    echo ""
    print_warning "Please edit .env with your Databricks credentials, then source it:"
    print_info "  nano .env"
    print_info "  source .env"
    echo ""
    read -p "Press Enter after configuring .env..."
    
    setup_terraform_backend
    setup_terraform_config
    
    echo ""
    print_warning "Please edit terraform.tfvars with your values:"
    print_info "  nano infra/terraform/terraform.tfvars"
    echo ""
    read -p "Press Enter after editing terraform.tfvars..."
    
    validate_databricks
    initialize_terraform
    
    print_info "Setup complete! You can now:"
    print_info "  1. Review the plan: cd infra/terraform && terraform plan"
    print_info "  2. Deploy: terraform apply"
    print_info "  3. Upload data: Run option 7 from the menu"
}

# Deploy infrastructure
deploy_infrastructure() {
    print_header "Deploying Infrastructure"
    
    cd infra/terraform
    
    print_info "Running terraform plan..."
    terraform plan -out=tfplan
    
    echo ""
    read -p "Do you want to apply this plan? (yes/no): " confirm
    
    if [ "$confirm" = "yes" ]; then
        print_info "Applying infrastructure..."
        terraform apply tfplan
        
        echo ""
        print_info "✓ Infrastructure deployed successfully!"
        print_info ""
        print_info "Next steps:"
        print_info "  1. Upload source data (option 7 from menu)"
        print_info "  2. Run the pipeline in Databricks UI"
        print_info "  3. Check job execution logs"
    else
        print_warning "Deployment cancelled"
    fi
    
    cd ../..
}

# Run interactive menu
if [ "$1" = "--auto" ]; then
    run_complete_setup
else
    while true; do
        show_menu
    done
fi