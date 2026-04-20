import json
import xml.etree.ElementTree as ET
import pandas as pd
import re

#----------------------------------------------------------
# Helper Code for checking type safety
#----------------------------------------------------------
def safe_numeric_filter(df, column, condition_func):
    """
    Safely apply a numeric filter after converting the column to float.
    Returns a boolean mask.
    """
    numeric_col = pd.to_numeric(df[column], errors='coerce')
    return condition_func(numeric_col)

# Usage: mask = safe_numeric_filter(df, 'total_amount', lambda x: x <= 0)


# ---------------------------------------------------------
# 1. THE STRUCTURE PHASE (Upgraded from Lab 1)
# ---------------------------------------------------------
def flatten_json_orders(filepath):
    """Loads a JSON file array, parses it, and returns a list of flat dictionaries."""
    with open(filepath, 'r') as f:
        data_list = json.load(f)
    
    flat_orders = []
    for data in data_list:
        flat_order = {
            "order_id": data.get("orderId"),
            "timestamp": data.get("timestamp"),
            "customer_id": data.get("customer", {}).get("id"),
            "first_name": data.get("customer", {}).get("profile", {}).get("firstName"),
            "last_name": data.get("customer", {}).get("profile", {}).get("lastName"),
            "email": data.get("customer", {}).get("profile", {}).get("contact", {}).get("email"),
            "phone": data.get("customer", {}).get("profile", {}).get("contact", {}).get("phone"),
            "payment_method": data.get("payment", {}).get("method"),
            "currency": data.get("payment", {}).get("amount", {}).get("currency"),
            "total_amount": data.get("payment", {}).get("amount", {}).get("total")
        }
        flat_orders.append(flat_order)
    return flat_orders

def flatten_xml_orders(filepath):
    """Loads an XML file, finds all orders, and returns a list of flat dictionaries."""
    tree = ET.parse(filepath)
    root = tree.getroot()
    
    flat_orders = []
    # Loop through each <order> tag inside the <orders> root
    for order in root.findall('order'):
        flat_order = {
            "order_id": order.find("orderId").text if order.find("orderId") is not None else None,
            "timestamp": order.find("timestamp").text if order.find("timestamp") is not None else None,
            "customer_id": order.find("customer/id").text if order.find("customer/id") is not None else None,
            "first_name": order.find("customer/profile/firstName").text if order.find("customer/profile/firstName") is not None else None,
            "last_name": order.find("customer/profile/lastName").text if order.find("customer/profile/lastName") is not None else None,
            "email": order.find("customer/profile/contact/email").text if order.find("customer/profile/contact/email") is not None else None,
            "phone": order.find("customer/profile/contact/phone").text if order.find("customer/profile/contact/phone") is not None else None,
            "payment_method": order.find("payment/method").text if order.find("payment/method") is not None else None,
            "currency": order.find("payment/amount").attrib.get("currency") if order.find("payment/amount") is not None else None,
            "total_amount": order.find("payment/amount").text if order.find("payment/amount") is not None else None
        }
        flat_orders.append(flat_order)
    return flat_orders

# ---------------------------------------------------------
# 2. THE CLEANING PHASE (Active & Destructive)
# ---------------------------------------------------------
def clean_data(df):
    """Actively modifies the DataFrame to fix errors and standardise formats."""
    print("\n--- Starting Cleaning Phase ---")
    
    # TODO: 1. Deduplication
    # Use pandas to drop duplicate rows. Keep the first occurrence.
    # Hint: We're dropping duplicates. Maybe the dataframe has a drop duplicates function? 

    # YOUR CODE HERE
    print(f"Rows after deduplication: {len(df)}")

    # TODO: 2. Standardization
    # The 'currency' column has weird spaces and lowercase letters (e.g., " gbp ").
    # Strip the whitespace and convert the string to UPPERCASE.
    # We can use str.strip() to trim whitespace, and str.upper() to make uppercase. We can get the string value from the df by using df[keyname]
   
    # YOUR CODE HERE

    # TODO: 3. Type Casting
    # The 'total_amount' column has a mix of numbers and strings ("89.99").
    # Convert the entire column to a numeric float type.
    # Pandas has a to_numeric(function that takes a string. If only we knew how to get the string value from the dataframe? )

    # YOUR CODE HERE

    # TODO: 4. Handling Missing Values (Voids)
    # John Doe is missing an email address (null/NaN). 
    # Let's actively impute this by filling missing emails with the string "UNKNOWN".
    # dataframe column objects (df['keyname']) have a .fillna() function that fills empty cells..

    # YOUR CODE HERE
    
    return df

# ---------------------------------------------------------
# 3. THE VALIDATION PHASE (Passive Gatekeeping)
# ---------------------------------------------------------
def validate_data(df):
    """Passively checks the cleaned data against business rules."""
    print("\n--- Starting Validation Phase ---")
    validation_passed = True
    
    # TODO: 1. Range Check
    # Business Rule: Total amount must be greater than 0.
    # Check if any row has a total_amount <= 0.
    mask = safe_numeric_filter(df, 'total_amount', lambda x: x <= 0)

    invalid_amounts = df[mask]
   
    if not invalid_amounts.empty:
        print(f"VALIDATION FAILED: Found {len(invalid_amounts)} row(s) with invalid totals!")
        print(invalid_amounts[['order_id', 'total_amount']])
        validation_passed = False
    else:
        print("Range Check: PASSED")

    # TODO: 2. Regex Format Check (Email)
    # Business Rule: Emails must contain an '@' symbol and a valid domain format.
    # We use a Regular Expression (Regex) pattern to check this.
    email_pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    
    # We will ignore rows where the email was explicitly marked "UNKNOWN" during cleaning
    emails_to_check = df[df['email'] != 'UNKNOWN']
    
    # Check which emails DO NOT match the regex pattern
    invalid_emails = emails_to_check[ ~emails_to_check['email'].str.match(email_pattern) ]
    
    if not invalid_emails.empty:
        print(f"VALIDATION FAILED: Found {len(invalid_emails)} row(s) with invalid email formats!")
        print(invalid_emails[['order_id', 'email']])
        validation_passed = False
    else:
         print("Regex Email Check: PASSED")

    return validation_passed

# ---------------------------------------------------------
# 4. EXECUTE END-TO-END PIPELINE
# ---------------------------------------------------------
if __name__ == "__main__":
    print("--- Starting Pipeline ---")
    
    # Phase 1: Structure (Extract and Flatten)
    print("Extracting data from files...")
    json_records = flatten_json_orders("source_a_dirty.json")
    xml_records = flatten_xml_orders("source_b_dirty.xml")
    
    # Combine into a single DataFrame
    combined_records = json_records + xml_records
    df = pd.DataFrame(combined_records)
    print(f"Total raw records loaded: {len(df)}")

    # Execute Phase 2: Clean
    df = clean_data(df)

    # Execute Phase 3: Validate
    is_valid = validate_data(df)
   
    
    print("\n--- Pipeline Status ---")
    if is_valid:
        print("SUCCESS! Data is clean and validated. Ready for the Enrich phase.")
        print(df.to_string())
    else:
        print("PIPELINE HALTED. Data failed validation constraints. Please review Dead Letter Queue.")
