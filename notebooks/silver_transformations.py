from pyspark.sql import functions as F

email_regex = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"

def clean_product_datatypes(df):
    return df.withColumn("Price per product",F.round(F.col("Price per product"),3))

def clean_products_names(df):
    return df.filter(~F.col("Product ID").isNull()).\
        dropDuplicates(["Product ID"]).\
            filter(F.col("Price per product")>=0).\
                withColumn("Category",F.initcap(F.trim(F.col("Category")))).\
                    withColumn("Sub-Category",F.initcap(F.trim(F.col("Sub-Category")))).\
                        withColumn("Product Name",F.initcap(F.trim(F.col("Product Name")))).\
                            withColumn("State",F.initcap(F.trim(F.col("State")))).\
                                fillna({"Category": "Unknown","Sub-Category": "Unknown","Product Name": "Unknown","State": "Unknown"})

def clean_customer_datatypes(df):
    return df.withColumn("Postal Code",F.col("Postal Code").cast("int"))

def clean_customer_names(df):
    clean_customer = df.dropDuplicates().\
        withColumn("Customer Name", F.initcap(F.trim(
            F.regexp_replace(
                F.regexp_replace(F.col("Customer Name"), r"[^\p{L}\p{Nd}\s]", ""),  
            r"\s+",' '
            )
    ))).\
        withColumn("City",F.initcap(F.trim(F.col("City")))).\
            withColumn("State",F.initcap(F.trim(F.col("State")))).\
                withColumn("Country",F.initcap(F.trim(F.col("Country")))).\
                    withColumn("Region",F.initcap(F.trim(F.col("Region")))).\
                        fillna({"Customer Name": "Unknown","City": "Unknown","State": "Unknown","Country": "Unknown","Region": "Unknown"})
    return clean_customer


def clean_customer_email(df):
    clean_customer = df.withColumn("Email",F.trim(F.lower(F.col("email")))).\
        withColumn("is_valid_email",F.when(F.col("Email").rlike(email_regex), True).otherwise(False)).\
            fillna({"Email": "Unknown"})
    return clean_customer

def clean_customer_phone(df):
    cleaned_phone = df.withColumn("phone",F.trim(F.regexp_replace(F.col("phone"), r"[^0-9]", "")))
    return cleaned_phone.withColumn("Phone", F.when((F.col("phone") == "") | (F.col("phone").isNull()), F.lit(None))
     .otherwise(F.col("phone")))
    
def clean_orders_dates(df):
    orders_cleaned = df.filter(F.col("Row ID").isNotNull()).\
    dropDuplicates(["Row ID"]).\
    withColumn("Order Date",F.to_date(F.col("Order Date"),"d/M/yyyy")).\
    withColumn("Ship Date",F.to_date(F.col("Ship Date"),"d/M/yyyy")).\
        withColumn("Profit",F.round(F.col("Profit"),2)).\
            withColumn("Discount",F.round(F.col("Discount"),2)).\
                withColumn("Price",F.round(F.col("Price"),2))

    
    return orders_cleaned

def clean_orders_validate(df):
    orders_cleaned = df.filter(F.col("Order Date")<=F.col("Ship Date")).\
        filter(F.col("Discount").between(0,1))
    return orders_cleaned

def enriched_order_customer_product(order_enriched,customer_enriched,product_enriched):
    enriched_table = order_enriched.join(customer_enriched,
                                         order_enriched["Customer ID"]==customer_enriched["Customer ID"],how="inner").\
                                             join(product_enriched,order_enriched["Product ID"]==product_enriched["Product ID"],how="inner").select(
            order_enriched["*"],
            customer_enriched["Customer Name"],
            customer_enriched["Country"].alias("Customer Country"),
            product_enriched["Category"].alias("Product Category"),
            product_enriched["Sub-Category"].alias("Product Sub-Category")
        ).drop("Customer ID","Product ID","Row ID")
    return enriched_table

