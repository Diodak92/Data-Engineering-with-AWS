chmod +x scripts/get_my_ip.sh
source .env
terraform init
terraform apply #-auto-approve

# To destroy the infrastructure, uncomment the line below
# terraform destroy -auto-approve