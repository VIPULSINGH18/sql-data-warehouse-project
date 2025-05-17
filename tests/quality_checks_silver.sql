
/*<------------------------------------------------------------------------------------------------------------->*/


/* in silver layer we check the quality of data and find the errors and then resolve it...  */


/* checking quality of CRM_CUST_INFO Table*/

/* first we check for the Null values and duplicate values in primary key*/
SELECT *
FROM bronze.crm_cust_info


/*this code is checklist for the duplicate values and null values inside the primary key*/

SELECT 
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*)>1 OR cst_id IS NULL


/* this code is checklist for unwanted spaces*/

SELECT
cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)


/*checklist for data standardisation and consistency*/

SELECT 
cst_gndr,  /*in both of these we are replacing one letter with complete one word*/
cst_material_status
FROM bronze.crm_cust_info





/* now all the checklist is done , data is cleaned and 
cleaned data is loaded from bronze to silver layer*/

/* accessing table from silver layer*/

SELECT *
FROM silver.crm_cust_info



/*<------------------------------------------------------------------------------------------------------------->*/



/*now we are going to check the quality of CRM_PRD_INFO table of bronze level*/

/*checking for nulls and duplicates*/


SELECT prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*)>1 OR prd_id IS NULL

/* THERE IS NO NULL VALUES*/




/* Now we are going to check is there any possible connection between two tables, if there is then we will connect it*/

/*this code will print the data of crm_prd_info table which is not present in erp_px_cat_g1v2 table, and if result is coming 
then it means table is not connected properly and have mismatch values*/

SELECT
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,  /* to extract the particular string from prd_key*/
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
prd_nm,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key,1,5),'-','_') NOT IN 
(SELECT distinct id from bronze.erp_px_cat_g1v2)


/*  using IN keyword instead of NOT IN  so only matching values will be displayed, both tables seems to be connected*/

SELECT
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,  /* to extract the particular string from prd_key*/
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
prd_nm,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key,1,5),'-','_')  IN 
(SELECT distinct id from bronze.erp_px_cat_g1v2)




/*this code will print the data of crm_prd_info table which is not present in crm_sales_details table, and if result is coming 
then it means table is not connected properly and have mismatch values*/

SELECT
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,  /* to extract the particular string from prd_key*/
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
prd_nm,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info
WHERE SUBSTRING(prd_key,7,LEN(prd_key)) NOT IN
(SELECT sls_prd_key FROM bronze.crm_sales_details)



/*  using IN keyword instead of NOT IN  so only matching values will be displayed, both tables seems to be connected*/

SELECT
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,  /* to extract the particular string from prd_key*/
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
prd_nm,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info
WHERE SUBSTRING(prd_key,7,LEN(prd_key))  IN
(SELECT sls_prd_key FROM bronze.crm_sales_details)



/* check for unwanted spaces */

SELECT
prd_nm
FROM bronze.crm_prd_info
WHERE TRIM(prd_nm) != prd_nm

/* there is no extra and unwanted spaces*/




/* checking for negative and null values*/
SELECT 
prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost<0 OR prd_cost IS NULL


/*Checking for data standardisation and data consistency*/

SELECT DISTINCT prd_line
FROM bronze.crm_prd_info



/* checking for invalid order of start and end date*/

SELECT *
FROM bronze.crm_prd_info
WHERE prd_start_dt>prd_end_dt 




/* now all the checklist is done , data is cleaned and 
cleaned data is loaded from bronze to silver layer*/


/*<------------------------------------------------------------------------------------------------------------->*/




/*now we are going to check the quality of CRM_SALES_DETAILS table of bronze level*/







/* now all the checklist is done , data is cleaned and 
cleaned data is loaded from bronze to silver layer*/



/*<------------------------------------------------------------------------------------------------------------->*/



/*now we are going to check the quality of ERP_CUST_AZ12 table of bronze level*/

SELECT 
cid,
bdate,
gen
FROM bronze.erp_cust_az12

SELECT *
FROM bronze.crm_cust_info

/* both of the table is having common column cid and cst_key, now we have to find is there any unmatched data*/

/*Checking the connection between the tables*/


SELECT 
CASE 
	WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) /* making cid data and cst_key data same to join both the table*/
	ELSE cid
END AS cid_2,
bdate,
gen
FROM bronze.erp_cust_az12
WHERE CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
		ELSE cid
END NOT IN ( SELECT DISTINCT cst_key FROM silver.crm_cust_info)

/*there is no unmatched data , means table is connected properly*/



/* now we are going to check the range of bdate*/

SELECT *
FROM bronze.erp_cust_az12
WHERE  bdate > GETDATE() /*this code is going to print date with invalid range*/


/*Checking data standardization and abbreviation*/
SELECT DISTINCT gen
FROM bronze.erp_cust_az12


/* now all the checklist is done , data is cleaned and 
cleaned data is loaded from bronze to silver layer*/



/*<------------------------------------------------------------------------------------------------------------->*/



/*now we are going to check the quality of ERP_LOC_A101 table of bronze level*/

SELECT *
FROM bronze.erp_loc_a101

SELECT cst_key
FROM bronze.crm_cust_info

/* both of the table is having common column cid and cst_key, now we have to find is there any unmatched data*/

/*Checking the connection between the tables*/

SELECT
cntry,
REPLACE(cid,'-','') AS cid /* making cid data and cst_key data same to join both the table*/
FROM bronze.erp_loc_a101
WHERE REPLACE(cid,'-','')  NOT IN ( SELECT cst_key FROM bronze.crm_cust_info)

/* there is no unmatched data so it means both the table is connected properly....*/



/* now we are going to check cardinality and abbreviation*/

SELECT DISTINCT cntry
FROM bronze.erp_loc_a101



/* now all the checklist is done , data is cleaned and 
cleaned data is loaded from bronze to silver layer*/


/*<------------------------------------------------------------------------------------------------------------->*/



/*now we are going to check the quality of ERP_px_cat_g1v2 table of bronze level*/

SELECT*
FROM bronze.erp_px_cat_g1v2

SELECT*
FROM bronze.crm_prd_info
/*value of prd_key in prd_info table and value of id in px_cat_g1v2 is same and all values are matching,it
means both the tables are connected...*/



/* now we will check unwanted spaces */
SELECT
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2
WHERE cat!=TRIM(cat) OR subcat!=TRIM(subcat) OR maintenance!=TRIM(maintenance)
/* nothing displayed means there is no any unwanted spaces in any of the columns*/


/*now we will check data standardisation and abbrevaition*/
SELECT DISTINCT cat
FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT subcat
FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2

/* so everything is fine , we have checked every quality of the data, there is no any error and we are going to load datan from bronze to silver layer*/

SELECT *
FROM silver.erp_px_cat_g1v2


/*<------------------------------------------------------------------------------------------------------------->*/







