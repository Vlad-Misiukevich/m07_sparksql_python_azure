# Setup azurerm as a state backend
terraform {
  backend "azurerm" {
    resource_group_name  = "databricks_2"
    storage_account_name = "stvl"
    container_name       = "cont-vl"
    key                  = "MlsOrJ1YOy1wviIFZ/t0ESxxbYO3PmgTcbyIX+oIlJQeI6hAOhPlXPyeg4uwImeCNxZOfJXSbz7rKB/J03DYJA=="

  }
}

# Configure the Microsoft Azure Provider
provider "azurerm" {
  features {}
  subscription_id = "9e5b0b80-8805-4b33-8b84-410263caf100"

}

data "azurerm_client_config" "current" {}

#resource "azurerm_resource_group" "bdcc" {
#  name = "rg-${var.ENV}-${var.LOCATION}"
#  location = var.LOCATION
#
#  lifecycle {
#    prevent_destroy = true
#  }
#
#  tags = {
#    region = var.BDCC_REGION
#    env = var.ENV
#  }
#}

#resource "azurerm_storage_account" "bdcc" {
#  depends_on = [
#    azurerm_resource_group.bdcc]
#
#  name = "st${var.ENV}"
#  resource_group_name = azurerm_resource_group.bdcc.name
#  location = azurerm_resource_group.bdcc.location
#  account_tier = "Standard"
#  account_replication_type = var.STORAGE_ACCOUNT_REPLICATION_TYPE
#  is_hns_enabled = "true"
#
#  network_rules {
#    default_action = "Allow"
#    ip_rules = values(var.IP_RULES)
#  }
#
#  lifecycle {
#    prevent_destroy = true
#  }
#
#  tags = {
#    region = var.BDCC_REGION
#    env = var.ENV
#  }
#}

#resource "azurerm_storage_data_lake_gen2_filesystem" "gen2_data" {
#  depends_on = [
#    azurerm_storage_account.bdcc]
#
#  name = "data"
#  storage_account_id = azurerm_storage_account.bdcc.id
#
#  lifecycle {
#    prevent_destroy = true
#  }
#}

#resource "azurerm_databricks_workspace" "bdcc" {
#  depends_on = [
#    azurerm_resource_group.bdcc
#  ]
#
#  name = "dbw-${var.ENV}-${var.LOCATION}"
#  resource_group_name = azurerm_resource_group.bdcc.name
#  location = azurerm_resource_group.bdcc.location
#  sku = "standard"
#
#  tags = {
#    region = var.BDCC_REGION
#    env = var.ENV
#  }
#}

provider "databricks" {
  host = var.databricks_workspace_url
}

variable "databricks_workspace_url" {
  description = "The URL to the Azure Databricks workspace (must start with https://)"
  type = string
  default = "https://adb-5271193890078146.6.azuredatabricks.net/?o=5271193890078146#folder/275127807006030"
}

resource "databricks_notebook" "bdcc" {
  source     = "C:/Users/Uladzislau_Misiukevi/PycharmProjects/m07_sparksql_python_azure/notebooks/weather_trends.py"
  path       = "/sql/weather_tr"
}

data "databricks_node_type" "smallest" {
  local_disk = true
}

data "databricks_spark_version" "latest" {}

resource "databricks_job" "bdcc" {
  name = "${var.ENV}-job"
  new_cluster {
    num_workers   = 1
    spark_version = data.databricks_spark_version.latest.id
    node_type_id  = data.databricks_node_type.smallest.id
  }
  notebook_task {
    notebook_path = databricks_notebook.bdcc.path
  }
}

output "notebook_url" {
  value = databricks_notebook.bdcc.url
}

// Print the URL to the job.
output "job_url" {
  value = databricks_job.bdcc.url
}