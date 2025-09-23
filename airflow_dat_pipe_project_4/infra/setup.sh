chmod +x scripts/get_my_ip.sh
source .env
terraform init
terraform apply #-auto-approve
terraform output -json > tf_outputs.json
python3 add_airflow_con.py

# To destroy the infrastructure, uncomment the line below
# terraform destroy -auto-approve