# **Lab 2: Data Wrangling \- Clean and Validate Phases**

## **Objective**

Welcome to Week 2 of our Data Wrangling Pipeline project\! Building upon last week, we are now moving into the **Clean** and **Validate** phases of the data lifecycle.

This week, you will practice building a **larger Python application**. Instead of processing just one record at a time, we will ingest multiple records from deep structural formats, combine them into a single pipeline, and perform critical data hygiene tasks.

* **Cleaning** is an *active and destructive* process. You will modify the data to fix formatting errors, remove duplicates, and handle missing values.  
* **Validating** is a *passive* process. You will act as a gatekeeper, writing rules (like checking email formats or numerical ranges) to ensure the cleaned data is mathematically sound before it moves to the next phase.

## **Learning Intentions**

By the end of this lab, you will be able to:

* **Identify Data Anomalies:** Recognize statistical noise, including duplicates, null values, and type-mismatches across different original formats.  
* **Execute Active Cleaning:** Utilize the pandas library to programmatically deduplicate rows, standardize string formatting (e.g., trimming whitespace), and safely cast data types.  
* **Enforce Passive Validation:** Apply Regular Expressions (Regex) and conditional logic to verify that data fields conform to strict business rules and schema expectations.  
* **Construct Pipelines:** Integrate parsing (Structure), modifying (Clean), and assessing (Validate) into a single, cohesive software engineering application.

## **Workspace Setup**

You will be working in your GitHub Codespaces environment. Take a look at the file explorer on the left side of your screen. Your workspace is already set up with all the files you need for today's lab:

EXPLORER

⌄ DATA\_WRANGLING

  🐍 DataWranglePipeline\_Phases1\_2\_3.py

  ⓘ  README.md

  {} source\_a\_dirty.json

  📻 source\_b\_dirty.xml

## **Part 1: Review the "Dirty" Data**

Before we write any code, we need to understand the data we are working with. Open both source\_a\_dirty.json and source\_b\_dirty.xml in your Codespace and inspect them.

Together, these files contain 6 records. Look closely and see if you can spot the following anomalies that we need to clean:

1. **Duplicates:** There is an exact duplicate order in the JSON file.  
2. **Missing Data:** At least one record is entirely missing an email address, and another is missing a \<contact\> block entirely (meaning no phone number either).  
3. **Inconsistent Formatting:** Look at the currency fields. Some have weird whitespaces and varying capitalization (e.g., " gbp ", " usd ", "EUR").  
4. **Invalid Values:** One of the orders has a negative total amount (\-50.00).  
5. **Type Mismatches:** In the XML file, the total amounts are imported as strings rather than mathematical numbers.

## **Part 2: Understanding the Pipeline**

Open DataWranglePipeline\_Phases1\_2\_3.py. This is your main application for this week.

Take a moment to read through the scaffolded code. You do not need to write the entire script from scratch. Notice how the application is broken down into distinct phases:

* **Phase 1: Structure:** The flatten\_json\_orders and flatten\_xml\_orders functions handle extracting the data and converting it into a single Pandas DataFrame. (This is an upgrade from your Lab 1 work\!).  
* **Phase 3: Validation:** The validate\_data function acts as our gatekeeper. It currently checks for negative numbers and uses Regular Expressions (Regex) to ensure emails are formatted correctly.

If you run the script right now (python DataWranglePipeline\_Phases1\_2\_3.py in your terminal), the pipeline will hit the validation phase and **FAIL** because your data is dirty\!

## **Part 3: The Cleaning Phase (Your Task)**

Your first task for this lab is to focus entirely on **Phase 2: The Cleaning Phase**. Scroll down to the clean\_data(df) function in your Python file.

You will use the pandas library to complete the four TODO blocks. Here is a walkthrough of what you need to implement:

### **TODO 1: Deduplication**

Our JSON source contains a duplicate transaction. If this made it to our database, we'd double-charge the customer\! Pandas has a built-in method to handle this.

* **Your Task:** Use the drop\_duplicates() function on your DataFrame df. Make sure to assign the result back to df or use inplace=True.  
* *Hint:* df \= df.drop\_duplicates()

### **TODO 2: Standardization**

The currency column is a mess. We have values like " GBP", " usd ", and "EUR". We need a standard format: no whitespace, and all uppercase.

* **Your Task:** Access the currency column, strip the leading/trailing whitespace, and convert it to uppercase. You can chain Pandas string methods.  
* *Hint:* df\['currency'\] \= df\['currency'\].str.strip().str.upper()

### **TODO 3: Type Casting**

Right now, the total\_amount column might be treated as strings (especially the data coming from XML). We need them to be numeric so we can perform math on them (and so our validation logic works).

* **Your Task:** Use the pd.to\_numeric() function to convert the total\_amount column.  
* *Hint:* df\['total\_amount'\] \= pd.to\_numeric(df\['total\_amount'\])

### **TODO 4: Handling Missing Values (Voids)**

One of the customers, John Doe, is missing his email address entirely. Our validation phase requires an email. To fix this, we will "impute" the missing data by filling it with a default string.

* **Your Task:** Use the .fillna() method on the email column to replace any NaN (Not a Number/Null) values with the string "UNKNOWN". (Our validation logic has been configured to ignore "UNKNOWN" emails).  
* *Hint:* df\['email'\] \= df\['email'\].fillna('UNKNOWN')

## **Part 4: The Validation Phase (Adding New Rules)**

The scaffolded code already includes some complex validation logic (like checking for negative numbers and Regex email checks). Now it's your turn to add some simpler, yet equally important, validation rules using basic Pandas methods.

Scroll down to the validate\_data(df) function and add the following checks:

### **TODO 5: Null Value Check (Phone Numbers)**

* **Business Rule:** All records must have a phone number on file.  
* **Your Task:** Check if any row in the phone column contains a missing (NaN) value. If it does, print an error and set validation\_passed \= False.  
* *Hint:* Pandas has a method called .isna().

invalid\_phones \= df\[ df\['phone'\].isna() \]

* 

### **TODO 6: Allowed Values Check (Currencies)**

* **Business Rule:** We only accept payments in 'GBP', 'USD', or 'EUR'.  
* **Your Task:** Check if any cleaned currency falls outside of this approved list.  
* *Hint:* Use a list of approved currencies and the .isin() method combined with the \~ (NOT) operator.

approved\_currencies \= \['GBP', 'USD', 'EUR'\]

invalid\_currencies \= df\[ \~df\['currency'\].isin(approved\_currencies) \]

* 

### **TODO 7: String Prefix Check (Order IDs)**

* **Business Rule:** All Order IDs must begin with the prefix "ORD-".  
* **Your Task:** Check if any order\_id fails to start with this prefix.  
* *Hint:* Pandas string columns have a .str.startswith() method.

invalid\_orders \= df\[ \~df\['order\_id'\].str.startswith('ORD-') \]

* 

## **Part 5: Run and Verify**

Once you have completed the cleaning TODOs and added your new validation rules, it's time to test your pipeline\!

1. Open your terminal in Codespaces.  
2. Run your Python script:

python DataWranglePipeline\_Phases1\_2\_3.py

3. 

**Goal:** If you successfully cleaned the data and wrote correct validation rules, the Validation gatekeeper will let the data pass, and your console should output the cleaned DataFrame along with this message:

SUCCESS\! Data is clean and validated. Ready for the Enrich phase.

If it still says PIPELINE HALTED, review the terminal output to see which validation check failed, and double-check your logic\!
