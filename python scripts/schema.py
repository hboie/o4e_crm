from google.cloud import bigquery

schema_turnover = [
    bigquery.SchemaField("code_of_integration", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("plant_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("member_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("member_branch_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("product_family_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("quantity", "DECIMAL", mode="REQUIRED"),
    bigquery.SchemaField("turnover", "DECIMAL", mode="REQUIRED"),
    bigquery.SchemaField("supplier_product_reference", "STRING"),
    bigquery.SchemaField("product_description", "STRING"),
    bigquery.SchemaField("supplier_id", "STRING"),
    bigquery.SchemaField("invoice_date", "DATETIME"),
    bigquery.SchemaField("year", "INTEGER"),
    bigquery.SchemaField("month", "INTEGER"),
    bigquery.SchemaField("customer_order_nb", "STRING"),
    bigquery.SchemaField("member_order_nb", "STRING"),
    bigquery.SchemaField("upload_partner", "STRING"),
    bigquery.SchemaField("upload_date", "STRING")
]

schema_uploadlog = [
    bigquery.SchemaField("partner", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("date", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("filename", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("message", "STRING", mode="REQUIRED")
]

schema_customers = [
    bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("customer_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("customer_folder_name", "STRING"),
    bigquery.SchemaField("pilot", "STRING"),
    bigquery.SchemaField("is_active", "BOOL"),
    bigquery.SchemaField("has_contract", "BOOL"),
    bigquery.SchemaField("has_rfq", "BOOL")
]

schema_plants = [
    bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("plant_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("plant_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("country", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("city", "STRING"),
    bigquery.SchemaField("sector", "STRING"),
    bigquery.SchemaField("plant_closed", "BOOL")
]

schema_productfamilies = [
    bigquery.SchemaField("productfamily_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("productfamily_code", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("productfamily_name", "STRING")
]

schema_members = [
    bigquery.SchemaField("member_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("member_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("country", "STRING", mode="REQUIRED")
]

schema_branches = [
    bigquery.SchemaField("member_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("branch_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("branch_name", "STRING"),
    bigquery.SchemaField("member_name", "STRING"),
    bigquery.SchemaField("branch_closed", "BOOL")
]

schema_suppliers = [
    bigquery.SchemaField("supplier_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("supplier_name", "STRING", mode="REQUIRED")
]



