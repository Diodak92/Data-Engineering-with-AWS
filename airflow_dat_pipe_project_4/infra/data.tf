data "external" "myip" {
  program = ["${path.module}/scripts/get_my_ip.sh"]
}