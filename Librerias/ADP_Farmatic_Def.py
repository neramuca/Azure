# Databricks notebook source
# MAGIC %run "./UTL_Gen"

# COMMAND ----------

# MAGIC %run "./UTL_Blob"

# COMMAND ----------

# MAGIC %run "./ADP_Load_Conf"

# COMMAND ----------

################################################################################
"""Spain PMS's File Utils.

"""
 #Who                 When           What
 #Victor Salesa       02/10/2018     Initial version
 #Ana Perez           08/10/2018     Add Header,Detail and Footer Spec
 #Victor Salesa       15/10/2018     Remove MasterData definitions and move them to ADP_Spain_MasterData_Def
 #Victor Salesa       09/11/2018     Add __SELLOUT_CANONICAL__
 #Ana Perez           12/11/2018     Add __SELLOUT_CANONICAL__ new Err fields
 #Ana Perez           13/11/2018     Add new List of Values
 #Ana Perez           26/11/2018     Add PMS_FARMATIC_CODE and PMS_FARMATIC_COUNTRY
 #Ana Perez           28/01/2019     Add LENGTH STRING FIELDS CANONCIAL: SELL-OUT
 #Victor Salesa       19/02/2019     Add Ingestion Pharmatic base path 
 #Victor Salesa       28/02/2019     Added select fields in order template code and schema definition for __CTL_PROCESS_FILE_VAL__
 #Victor Salesa       26/04/2019     Adapted schema for new structure in __CTL_PROCESS_FILE_VAL__
 #Victor Salesa       29/04/2019     Added Sellout Canonical Constants
 #Victor Salesa       05/04/2019     
################################################################################

# --------- Farmatic Base Paths
__PHARMATIC_FOLDER__                  = 'pharmatic/'

__PHARMATIC_TOBEPROCESSED_BASE_PATH__            = __TOBEPROCESSED_BASE_PATH__         + __PHARMATIC_FOLDER__
__PHARMATIC_LANDING_BASE_PATH__                  = __LANDING_BASE_PATH__               + __PHARMATIC_FOLDER__
__PHARMATIC_ERROR_BASE_PATH__                    = __ERROR_BASE_PATH__                 + __PHARMATIC_FOLDER__
__PHARMATIC_CANONICAL_BASE_PATH__                = __CANONICAL_BASE_PATH__             + __PHARMATIC_FOLDER__
__PHARMATIC_ERRORPROCESS_BASE_PATH__             = __ERRORPROCESS_BASE_PATH__          + __PHARMATIC_FOLDER__
__PHARMATIC_ARCHIVE_BASE_PATH__                  = __ARCHIVE_BASE_PATH__               + __PHARMATIC_FOLDER__
__PHARMATIC_INGESTION_BASE_PATH__                = __INGESTION_BASE_PATH__             + __PHARMATIC_FOLDER__

# --------- General Information FARMATIC
__PMS_FARMATIC_CODE__  = "FMT";
__PMS_FARMATIC_COUNTRY__  = "ES";

# --------- Filename Info Structure
__PH_UNIQUE_CODE_LEN__  = 5; __SPEC_VER_LEN__        = 3; __RELEASE_VERSION_LEN__ = 2   
__DATE_LEN__            = 8; __ORIGIN_LEN__          = 1; __FILE_TYPE_LEN__       = 2

__SP_FILENAME_LENGHTS__        = [__PH_UNIQUE_CODE_LEN__,__SPEC_VER_LEN__,__RELEASE_VERSION_LEN__,__DATE_LEN__,__ORIGIN_LEN__,__FILE_TYPE_LEN__]
__SP_FILENAME_COLUMN_NAMES__   = ['pharmacy_unique_code','spec_version','release_version','data_date','origin','file_type']
__SP_FILENAME_POSITIONS__      = [pos for pos in range(len(__SP_FILENAME_COLUMN_NAMES__))]

file_origins = {'D':'Dispensary','E':'ePos','G':'No System Segmentation'}
file_types = {'SL':'Sales','PR':'Purchases','ST':'Stock'}

# --------- Header Record Type
__RECORDTYPE_LEN__ = 1
__FILETYPE_LEN__ = 2
__PHARMACYID_LEN__ = 5
__COMPLEMENTARYPHARMACYID_LEN__ = 20
__FILEDATE_LEN__ = 14
__DATAEXTRACTIONMODULEVERSION_LEN__ = 16
__SOFTWARECOMPANY_LEN__ = 3
__SPECIFICATIONSVERSION_LEN__ = 3
__ZIPCODE_LEN__ = 8
__EXTERNALPHARMACYID_LEN__ = 8

__SP_HEADER_LENGHTS__=(
    [__RECORDTYPE_LEN__, 
     __FILETYPE_LEN__, 
     __PHARMACYID_LEN__, 
     __COMPLEMENTARYPHARMACYID_LEN__, 
     __FILEDATE_LEN__, 
     __DATAEXTRACTIONMODULEVERSION_LEN__, 
     __SOFTWARECOMPANY_LEN__, 
     __SPECIFICATIONSVERSION_LEN__, 
     __ZIPCODE_LEN__, 
     __EXTERNALPHARMACYID_LEN__]
    )

__SP_HEADER_COLUMN_NAMES__ = (
  ['RecordType', 
   'FileType', 
   'PharmacyID', 
   'ComplementaryPharmacyID', 
   'FileDate', 
   'DataExtractionModuleVersion', 
   'SoftwareCompany', 
   'SpecificationsVersion', 
   'ZIPCode', 
   'ExternalPharmacyID'])
__SP_HEADER_POSITIONS__    = [pos for pos in range(len(__SP_HEADER_COLUMN_NAMES__))]


# --------- Detail Record Type: SELL-OUT
__OPERATIONID_LEN__ = 16
__OPERATIONLINE_LEN__ = 4
__OPERATIONDATE_LEN__ = 14
__POSIDENT_LEN__ = 20
__SALESCHANNEL_LEN__ = 1
__DISCOUNTVALOVERTTOTRECEIPT_LEN__ = 7
__DECREASEDVALTOTRECEIPT_LEN__ = 7
__TOTALRECEIPTSIGNAL_LEN__ = 1
__TOTALOFRECEIPT_LEN__ = 7
__PAYMENTMODE_LEN__ = 1
__CONSUMERTYPE_LEN__ = 1
__CONSUMERANONYMOUSID_LEN__ = 16
__CONSUMERGENDER_LEN__ = 1
__CONSUMERAGE_LEN__ = 4
__CONSUMERLOCATION_LEN__ = 2
__CONSUMERZIPCODE_LEN__ = 8
__PRESCRIPTIONIDENT_LEN__ = 1
__PRESCRIPTIONMODE_LEN__ = 1
__PRESCRIPTIONTYPE_LEN__ = 1
__PRESCRIPTIONID_LEN__ = 40
__PRESCRIPTIONORIGIN_LEN__ = 250
__PRESCRIPTIONSPECIALTYDOC_LEN__ = 100
__PRESCRIBEDPRODUCTCODE_LEN__ = 40
__PRESCRIBEDPRODUCTNAME_LEN__ = 100
__OPERATIONIDENT_LEN__ = 1
__PRODUCTCODE_LEN__ = 14
__PRODUCTNAME_LEN__ = 100
__EANCODE_LEN__ = 14
__SELLPRODUCTMODE_LEN__ = 1
__PRODUCTPACKS_LEN__ = 6
__PRODUCTUNITS_LEN__ = 6
__PRODUCTPACKSIZE_LEN__ = 6
__PRODUCTPRICECATALOG_LEN__ = 7
__PRODUCTPRICE_LEN__ = 7
__DISCOUNTVALUE_LEN__ = 7
__DISCOUNTTYPE_LEN__ = 1
__PRODUCTFEE_LEN__ = 7
__PRODUCTMARKUP_LEN__ = 7
__PRODUCTTOTALPRICE_LEN__ = 7
__COPAYMENTVALUE_LEN__ = 7
__REIMBURSEMENT_LEN__ = 1
__REIMBURSEMENTENTITYCODE_LEN__ = 3
__REIMBURSEMENTENTITYNAME_LEN__ = 40
__REIMBURSEMENTTYPE_LEN__ = 3
__REIMBURSEMENTCATEGORY_LEN__ = 6
__REIMBURSEMENTVALUE_LEN__ = 7
__CONSUMERVALUE_LEN__ = 7
__PROMOTION_LEN__ = 1
__PROMOTIONTYPE_LEN__ = 1
__PROMOTIONID_LEN__ = 12
__PROMOTIONDESCRIPTION_LEN__ = 50
__ISSUEDVOUCHERSIDENT_LEN__ = 1
__NUMBERISSUEDVOUCHERS_LEN__ = 1
__VALUEISSUEDVOUCHERS_LEN__ = 7
__USEDVOUCHERSIDENT_LEN__ = 1
__NUMBERUSEDVOUCHERS_LEN__ = 1
__VALUEUSEDVOUCHERS_LEN__ = 7
__LOYALTYPROGRAMUSED_LEN__ = 1
__LOYALTYPROGRAMNAME_LEN__ = 40
__LOYALTYREBATE_LEN__ = 1



__SP_DETAIL_LENGHTS__  = (
    [__RECORDTYPE_LEN__ , __OPERATIONID_LEN__, __OPERATIONLINE_LEN__, __OPERATIONDATE_LEN__, __POSIDENT_LEN__, __SALESCHANNEL_LEN__, __DISCOUNTVALOVERTTOTRECEIPT_LEN__, __DECREASEDVALTOTRECEIPT_LEN__,
     __TOTALRECEIPTSIGNAL_LEN__, __TOTALOFRECEIPT_LEN__, __PAYMENTMODE_LEN__, __CONSUMERTYPE_LEN__, __CONSUMERANONYMOUSID_LEN__, __CONSUMERGENDER_LEN__, __CONSUMERAGE_LEN__, __CONSUMERLOCATION_LEN__, __CONSUMERZIPCODE_LEN__,
     __PRESCRIPTIONIDENT_LEN__, __PRESCRIPTIONMODE_LEN__, __PRESCRIPTIONTYPE_LEN__, __PRESCRIPTIONID_LEN__, __PRESCRIPTIONORIGIN_LEN__, __PRESCRIPTIONSPECIALTYDOC_LEN__, __PRESCRIBEDPRODUCTCODE_LEN__,
     __PRESCRIBEDPRODUCTNAME_LEN__, __OPERATIONIDENT_LEN__, __PRODUCTCODE_LEN__, __PRODUCTNAME_LEN__, __EANCODE_LEN__, __SELLPRODUCTMODE_LEN__, __PRODUCTPACKS_LEN__, __PRODUCTUNITS_LEN__, __PRODUCTPACKSIZE_LEN__,
     __PRODUCTPRICECATALOG_LEN__, __PRODUCTPRICE_LEN__, __DISCOUNTVALUE_LEN__, __DISCOUNTTYPE_LEN__, __PRODUCTFEE_LEN__,  __PRODUCTMARKUP_LEN__, __PRODUCTTOTALPRICE_LEN__, __COPAYMENTVALUE_LEN__, __REIMBURSEMENT_LEN__, 
     __REIMBURSEMENTENTITYCODE_LEN__, __REIMBURSEMENTENTITYNAME_LEN__, __REIMBURSEMENTTYPE_LEN__, __REIMBURSEMENTCATEGORY_LEN__, __REIMBURSEMENTVALUE_LEN__, __CONSUMERVALUE_LEN__, __PROMOTION_LEN__, __PROMOTIONTYPE_LEN__, 
     __PROMOTIONID_LEN__, __PROMOTIONDESCRIPTION_LEN__, __ISSUEDVOUCHERSIDENT_LEN__, __NUMBERISSUEDVOUCHERS_LEN__, __VALUEISSUEDVOUCHERS_LEN__, __USEDVOUCHERSIDENT_LEN__, __NUMBERUSEDVOUCHERS_LEN__, __VALUEUSEDVOUCHERS_LEN__, 
     __LOYALTYPROGRAMUSED_LEN__, __LOYALTYPROGRAMNAME_LEN__, __LOYALTYREBATE_LEN__]
)

__SP_DETAIL_COLUMN_NAMES__ = (
    ['RecordType', 'OperationID', 'OperationLine', 'OperationDate', 'POSIdent', 'SalesChannel', 'DiscountValOvertTotReceipt', 'DecreasedValTotReceipt', 'TotalReceiptSignal', 'TotalOfReceipt', 'PaymentMode', 'ConsumerType',
     'ConsumerAnonymousID', 'ConsumerGender', 'ConsumerAge', 'ConsumerLocation', 'ConsumerZipCode', 'PrescriptionIdent', 'PrescriptionMode', 'PrescriptionType', 'PrescriptionID', 'PrescriptionOrigin',
     'PrescriptionSpecialtyDoc', 'PrescribedProductCode', 'PrescribedProductName', 'OperationIdent', 'ProductCode', 'ProductName', 'EANCode', 'SellProductMode', 'ProductPacks', 'ProductUnits', 'ProductPackSize',
     'ProductPriceCatalog', 'ProductPrice', 'DiscountValue', 'DiscountType', 'ProductFee',  'ProductMarkup', 'ProductTotalPrice', 'CoPaymentValue', 'Reimbursement', 'ReimbursementEntityCode', 'ReimbursementEntityName',
     'ReimbursementType', 'ReimbursementCategory', 'ReimbursementValue', 'ConsumerValue', 'Promotion', 'PromotionType', 'PromotionID', 'PromotionDescription', 'IssuedVouchersIdent', 'NumberIssuedVouchers',
     'ValueIssuedVouchers', 'UsedVouchersIdent', 'NumberUsedVouchers', 'ValueUsedVouchers', 'LoyaltyProgramUsed', 'LoyaltyProgramName', 'LoyaltyRebate'])

__SP_DETAIL_POSITIONS__    = [pos for pos in range(len(__SP_DETAIL_COLUMN_NAMES__))]

__SP_TOTAL_LENGHTS__ = str(np.array(__SP_DETAIL_LENGHTS__).sum())


# --------- Footer Record Type
__OPERATIONSNUMBER_LEN__ = 6
__RECORDSNUMBER_LEN__ = 7
__SP_FOOTER_LENGHTS__  = [__RECORDTYPE_LEN__, __OPERATIONSNUMBER_LEN__, __RECORDSNUMBER_LEN__]

__SP_FOOTER_COLUMN_NAMES__ = ['RecordType', 'OperationsNumber', 'RecordsNumber']
__SP_FOOTER_POSITIONS__    = [pos for pos in range(len(__SP_FOOTER_COLUMN_NAMES__))]

# ---------- Included Pharmatic Files Pattern
__PHARMATIC_FILES_INCLUDED_PATTERN__=['GSL','GPR']

#---------- Sellout Canonical Out Schema
__SELLOUT_CANONICAL__ = StructType([
  StructField("FILE_SPEC_VERSION",StringType(),True),
  StructField("FILE_RELEASE_VERSION",StringType(),True),
  StructField("FILE_ORIGIN",StringType(),True),
  StructField("FILE_TYPE",StringType(),True),
  StructField("FILE_NAME",StringType(),True),
  StructField("FILE_DATE",LongType(),True),
  StructField("LANDING_DATE",LongType(),True),
  StructField("FILE_LINE_NUM",IntegerType(),True),
  StructField("PHARMACY_CODE",StringType(),True),
  StructField("EXTERNAL_PHARMACY_CODE",StringType(),True),
  StructField("CUSTOMER_CODE",StringType(),True),
  StructField("POSTAL_CODE",StringType(),True),
  StructField("COUNTRY_CODE",StringType(),True),
  StructField("OPERATION_TYPE",StringType(),True),
  StructField("OPERATION_CODE",StringType(),True),
  StructField("OPERATION_LINE",IntegerType(),True),
  StructField("OPERATION_DATE",LongType(),True),
  StructField("SALES_POINT_CODE",StringType(),True),
  StructField("SALES_CHANNEL",StringType(),True),
  StructField("PRODUCT_LINE_CODE",StringType(),True),
  StructField("PRODUCT_LINE_NAME",StringType(),True),
  StructField("PRODUCT_NATIONAL_CODE",StringType(),True),
  StructField("PRODUCT_NAME",StringType(),True),
  StructField("PRODUCT_BAR_CODE",StringType(),True),
  StructField("PRODUCT_INTERNAL_CODE",StringType(),True),
  StructField("BRAND",StringType(),True),
  StructField("MANUFACTURER_CODE",StringType(),True),
  StructField("MANUFACTURER_NAME",StringType(),True),
  StructField("LEGAL_CATEGORY",StringType(),True),
  StructField("COMMERCIAL_CATEGORY_L1",StringType(),True),
  StructField("COMMERCIAL_CATEGORY_L2",StringType(),True),
  StructField("COMMERCIAL_CATEGORY_L3",StringType(),True),
  StructField("PRODUCT_QTY",IntegerType(),True),
  StructField("PRODUCT_FREE_QTY",IntegerType(),True),
  StructField("PACK_SIZE",IntegerType(),True),
  StructField("PRODUCT_PRICE_CATALOG",DoubleType(),True),
  StructField("PRODUCT_PRICE",DoubleType(),True),
  StructField("DISCOUNT_TYPE",StringType(),True),
  StructField("DISCOUNT_VALUE",DoubleType(),True),
  StructField("DISCOUNT_WEIGTHED_AMOUNT",DoubleType(),True),
  StructField("PRODUCT_FEE_VALUE",DoubleType(),True),
  StructField("PRODUCT_MARKUP_VALUE",DoubleType(),True),
  StructField("PRODUCT_NET_PRICE",DoubleType(),True),
  StructField("PRODUCT_NET_WEIGTHED_PRICE",DoubleType(),True),
  StructField("PRODUCT_TOTAL_AMOUNT",DoubleType(),True),
  StructField("CONSUMER_PAYMENT_AMOUNT",DoubleType(),True),
  StructField("REIMBURSEMENT_AMOUNT",DoubleType(),True),
  StructField("CONSUMER_AMOUNT",DoubleType(),True),
  StructField("TOT_RECEIPT_DISCOUNT_AMOUNT",DoubleType(),True),
  StructField("TOT_RECEIPT_DECREASE_AMOUNT",DoubleType(),True),
  StructField("TOT_RECEIPT_AMOUNT",DoubleType(),True),
  StructField("PAYMENT_MODE",StringType(),True),
  StructField("CONSUMER_TYPE",StringType(),True),
  StructField("CONSUMER_ANONYMOUS_CODE",StringType(),True),
  StructField("CONSUMER_GENDER",StringType(),True),
  StructField("CONSUMER_AGE",IntegerType(),True),
  StructField("CONSUMER_LOCATION_CODE",StringType(),True),
  StructField("CONSUMER_POSTAL_CODE",StringType(),True),
  StructField("PRESCRIPTION_FLG",StringType(),True),
  StructField("PRESCRIPTION_MODE",StringType(),True),
  StructField("PRESCRIPTION_TYPE",StringType(),True),
  StructField("PRESCRIPTION_CODE",StringType(),True),
  StructField("PRESCRIPTION_ORIGIN",StringType(),True),
  StructField("PRESCRIPTION_SPECIALITY",StringType(),True),
  StructField("PRESCRIPTION_PRODUCT_CODE",StringType(),True),
  StructField("PRESCRIPTION_PRODUCT_NAME",StringType(),True),
  StructField("REIMBURSEMENT_FLG",StringType(),True),
  StructField("REIMBURSEMENT_ENTITY_CODE",StringType(),True),
  StructField("REIMBURSEMENT_ENTITY_NAME",StringType(),True),
  StructField("REIMBURSEMENT_TYPE",StringType(),True),
  StructField("REIMBURSEMENT_CATEGORY",StringType(),True),
  StructField("PROMOTION_FLG",StringType(),True),
  StructField("PROMOTION_TYPE",StringType(),True),
  StructField("PROMOTION_CODE",StringType(),True),
  StructField("PROMOTION_DESC",StringType(),True),
  StructField("ISSUED_VOUCHERS_FLG",StringType(),True),
  StructField("ISSUED_VOUCHERS_NUM",IntegerType(),True),
  StructField("ISSUED_VOUCHERS_AMOUNT",DoubleType(),True),
  StructField("USED_VOUCHERS_FLG",StringType(),True),
  StructField("USED_VOUCHERS_NUM",IntegerType(),True),
  StructField("USED_VOUCHERS_AMOUNT",DoubleType(),True),
  StructField("LOYALTY_PROGRAM_FLG",StringType(),True),
  StructField("LOYALTY_PROGRAM_NAME",StringType(),True),
  StructField("LOYALTY_REBATE_FLG",StringType(),True)
])

#---------- Sellout Canonical Constants

__FMT_SL_NUM_VALIDATED_FIELDS_COMPLETENESS__ = 17  #  6 (ENR_EMPTY) + 11 (DATA_EMPTY)  = 17
__FMT_SL_NUM_VALIDATED_FIELDS_ACCURACY__     = 10  #  8 (DATA_LOV)  +  2 (ENR_UNK)     = 10
__FMT_SL_NUM_VALIDATED_FIELDS_DUPLICATE__    =  0  #  0 (NOT DEFINED)
__FMT_SL_NUM_VALIDATED_FIELDS_CONFORMITY__   = 15  # 13 (DATA_TYPE) +  2 (DATA_LENGTH) = 15

# --------- LENGTH STRING FIELDS CANONCIAL: SELL-OUT
__CMD_FILE_SPEC_VERSION__ = 20
__CMD_FILE_RELEASE_VERSION__ = 20
__CMD_FILE_ORIGIN__ = 20
__CMD_PMS_CODE__ = 5
__CMD_FILE_TYPE__ = 20
__CMD_FILE_NAME__ = 50
__CMD_PHARMACY_CODE__ = 20
__CMD_EXTERNAL_PHARMACY_CODE__ = 20
__CMD_CUSTOMER_CODE__ = 20
__CMD_POSTAL_CODE__ = 20
__CMD_COUNTRY_CODE__ = 2
__CMD_OPERATION_TYPE__ = 20
__CMD_OPERATION_CODE__ = 20
__CMD_SALES_POINT_CODE__ = 20
__CMD_SALES_CHANNEL__ = 20
__CMD_PRODUCT_LINE_CODE__ = 20
__CMD_PRODUCT_LINE_NAME__ = 255
__CMD_PRODUCT_LINE_BAR_CODE__ = 20
__CMD_PRODUCT_NATIONAL_CODE__ = 20
__CMD_PRODUCT_NAME__ = 255
__CMD_PRODUCT_INTERNAL_CODE__ = 20
__CMD_BRAND__ = 50
__CMD_MANUFACTURER_CODE__ = 50
__CMD_MANUFACTURER_NAME__ = 255
__CMD_LEGAL_CATEGORY__ = 50
__CMD_COMMERCIAL_CATEGORY_L1__ = 255
__CMD_COMMERCIAL_CATEGORY_L2__ = 255
__CMD_COMMERCIAL_CATEGORY_L3__ = 255
__CMD_DISCOUNT_TYPE__ = 20
__CMD_PAYMENT_MODE__ = 52
__CMD_CONSUMER_TYPE__ = 20
__CMD_CONSUMER_ANONYMOUS_CODE__ = 20
__CMD_CONSUMER_GENDER__ = 20
__CMD_CONSUMER_LOCATION_CODE__ = 20
__CMD_CONSUMER_POSTAL_CODE__ = 20
__CMD_PRESCRIPTION_FLG__ = 1
__CMD_PRESCRIPTION_MODE__ = 20
__CMD_PRESCRIPTION_TYPE__ = 20
__CMD_PRESCRIPTION_CODE__ = 40
__CMD_PRESCRIPTION_ORIGIN__ = 20
__CMD_PRESCRIPTION_SPECIALITY__ = 50
__CMD_PRESCRIPTION_PRODUCT_CODE__ = 40
__CMD_PRESCRIPTION_PRODUCT_NAME__ = 255
__CMD_REIMBURSEMENT_FLG__ = 1
__CMD_REIMBURSEMENT_ENTITY_CODE__ = 20
__CMD_REIMBURSEMENT_ENTITY_NAME__ = 50
__CMD_REIMBURSEMENT_TYPE__ = 20
__CMD_REIMBURSEMENT_CATEGORY__ = 20
__CMD_PROMOTION_FLG__ = 1
__CMD_PROMOTION_TYPE__ = 20
__CMD_PROMOTION_CODE__ = 20
__CMD_PROMOTION_DESC__ = 50
__CMD_ISSUED_VOUCHERS_FLG__ = 1
__CMD_USED_VOUCHERS_FLG__ = 1
__CMD_LOYALTY_PROGRAM_FLG__ = 1
__CMD_LOYALTY_PROGRAM_NAME__ = 50
__CMD_LOYALTY_REBATE_FLG__ = 1

# --------- Detail Record Type: SELL-IN
__PR_OPERATIONIDENTIFICATION_LEN__ = 1
__PR_OPERATIONID_LEN__ = 24
__PR_OPERATIONLINE_LEN__ = 6
__PR_OPERATIONDATE_LEN__ = 14
__PR_SUPPLIERTYPE_LEN__ = 3
__PR_SUPPLIERIDENTIFICATION_LEN__ = 100
__PR_PRODUCTCODE_LEN__ = 14
__PR_PRODUCTNAME_LEN__ = 100
__PR_EANCODE_LEN__ = 14
__PR_ALTERNATIVEPRODUCTCODE_LEN__ = 14
__PR_COSTPRICEWITHOUTTAXES_LEN__ = 7
__PR_PERCENTAGESIGNAL_LEN__ = 1
__PR_PERCENTAGE_LEN__ = 7
__PR_TAXESPERCENTAGE_LEN__ = 7
__PR_QUANTITY_LEN__ = 6
__PR_QUANTITYOFFERED_LEN__ = 6


__SP_PR_DETAIL_LENGHTS__  =  ([__RECORDTYPE_LEN__, __PR_OPERATIONIDENTIFICATION_LEN__, __PR_OPERATIONID_LEN__, __PR_OPERATIONLINE_LEN__, __PR_OPERATIONDATE_LEN__, __PR_SUPPLIERTYPE_LEN__, __PR_SUPPLIERIDENTIFICATION_LEN__, __PR_PRODUCTCODE_LEN__, __PR_PRODUCTNAME_LEN__, __PR_EANCODE_LEN__, __PR_ALTERNATIVEPRODUCTCODE_LEN__, __PR_COSTPRICEWITHOUTTAXES_LEN__, __PR_PERCENTAGESIGNAL_LEN__, __PR_PERCENTAGE_LEN__, __PR_TAXESPERCENTAGE_LEN__, __PR_QUANTITY_LEN__, __PR_QUANTITYOFFERED_LEN__]
      )

__SP_PR_DETAIL_COLUMN_NAMES__ = ([ 'RecordType', 'OperationIdentification', 'OperationID', 'OperationLine', 'OperationDate', 'SupplierType', 'SupplierIdentification',	 'ProductCode', 'ProductName', 'EANCode', 'AlternativeProductCode', 'CostPricewithoutTaxes', 'PercentageSignal', 'Percentage',	 'TaxesPercentage', 'Quantity', 'QuantityOffered']
  )

__SP_PR_DETAIL_POSITIONS__    = [pos for pos in range(len(__SP_PR_DETAIL_COLUMN_NAMES__))]

__SP_PR_TOTAL_LENGHTS__ = str(np.array(__SP_PR_DETAIL_LENGHTS__).sum())


# --------- Footer Record Type
__OPERATIONSNUMBER_LEN__ = 6
__RECORDSNUMBER_LEN__ = 7
__SP_FOOTER_LENGHTS__  = [__RECORDTYPE_LEN__, __OPERATIONSNUMBER_LEN__, __RECORDSNUMBER_LEN__]

__SP_FOOTER_COLUMN_NAMES__ = ['RecordType', 'OperationsNumber', 'RecordsNumber']
__SP_FOOTER_POSITIONS__    = [pos for pos in range(len(__SP_FOOTER_COLUMN_NAMES__))]


# ---------- SellIN Canonical In Schema
__SELLIN_CANONICAL__ = StructType([
	StructField("FILE_NAME",StringType(),True),
    StructField("LANDING_DATE",LongType(),True),
    StructField("PHARMACY_PMS_CODE",IntegerType(),True),
	StructField("FILE_DATE",LongType(),True),
	StructField("ZIP_CODE",IntegerType(),True),
	StructField("EXTERNAL_PHARMACY_CODE",IntegerType(),True),
    StructField("FILE_LINE_NUM",IntegerType(),True),
    StructField("OPERATION_TYPE",StringType(),True),
	StructField("OPERATION_CODE",IntegerType(),True),
	StructField("OPERATION_LINE",IntegerType(),True),
	StructField("OPERATION_DATE",LongType(),True),
	StructField("SUPPLIER_TYPE",StringType(),True),
	StructField("SUPPLIER_CODE",StringType(),True),
	StructField("PRODUCT_LINE_CODE",IntegerType(),True),
	StructField("PRODUCT_LINE_NAME",StringType(),True),
	StructField("EAN_CODE",LongType(),True),
    StructField("ALTERNATIVE_PRODUCT_CODE",StringType(),True),
    StructField("PRODUCT_NET_PRICE",DoubleType(),True),
    StructField("PERCENTAGE_TYPE",StringType(),True),
    StructField("PERCENTAGE_VALUE",DoubleType(),True),
    StructField("TAXES_PERCENTAGE_VALUE",DoubleType(),True),
    StructField("PRODUCT_QTY",IntegerType(),True),
    StructField("PRODUCT_OFFERED_QTY",IntegerType(),True),
	StructField("OperationLine_Err",IntegerType(),True),
	StructField("OperationDate_Err",IntegerType(),True),
	StructField("CostPricewithoutTaxes_Err",IntegerType(),True),
	StructField("Percentage_Err",IntegerType(),True),
	StructField("TaxesPercentage_Err",IntegerType(),True),
	StructField("Quantity_Err",IntegerType(),True),
	StructField("QuantityOffered_Err",IntegerType(),True),
	StructField("formatErrorRow",IntegerType(),True),
	StructField("NATIONAL_CODE",IntegerType(),True),
	StructField("EAN13",LongType(),True),
	StructField("CLASS",StringType(),True),
	StructField("CATEGORY",StringType(),True),
	StructField("FAMILY",StringType(),True),
	StructField("SUBFAMILY",StringType(),True),
	StructField("BRAND",StringType(),True),
	StructField("LABORATORY",StringType(),True),
	StructField("PRODUCT_NAME",StringType(),True),
	StructField("PHARMACY_CODE",StringType(),True),
	StructField("MANUFACTURER_CODE",StringType(),True),
  	StructField("EnrichNationalCodeResult",StringType(),True),
	StructField("EnrichCategoryResult",StringType(),True),
    StructField("EnrichManufacturerResult",StringType(),True),
  	StructField("EnrichErrorRow",StringType(),True)
])

# Real Schema for __CTL_PROCESS_FILE_VAL__ TABLE
__CTL_PROCESS_FILE_VAL_SCHEMA__ = (StructType([
	StructField("FILE_NAME",StringType(),True),
	StructField("LANDING_DATE",TimestampType(),True),
	StructField("VALIDATION_TYPE",StringType(),True),
	StructField("PMS_CODE",StringType(),True),
	StructField("COUNTRY_CODE",StringType(),True),
	StructField("PHARMACY_CODE",StringType(),True),
	StructField("BUSINESS_AREA",StringType(),True),
	StructField("START_DATE",TimestampType(),True),
	StructField("END_DATE",TimestampType(),True),
	StructField("STATUS",IntegerType(),True),
	StructField("MESSAGE_TEXT",StringType(),True),
	StructField("ERROR_CODE",StringType(),True)
]))

# Autogenerated select-cast operation to put fields in order and be sure that they have the right schema.
__CTL_PROCESS_FILE_VAL_SELECT__ = [col(field.name).cast(field.dataType) for field in __CTL_PROCESS_FILE_VAL_SCHEMA__]


# --------- Pharmatic List Values
lov_CONSUMER_GENDER = {'M':'Male','F':'Female','O':'Other'} 
lov_CONSUMER_TYPE = {'1':'Individual person','2':'Entity'} 
lov_PAYMENT_MODE     = {'1':'Cash','2':'Card','3':'Other','4':'Credit Account/Deferred Payment'}
lov_YES_NO  = {'Y':'Yes','N':'No'}
lov_TOTAL_RECEIPT_SIGNAL = {'P':'Positive','N':'Negative'}
lov_PRESCRIPTION_MODE = {'E':'Electronic','M':'Manual'}
lov_PRESCRIPTION_TYPE = {'P':'Specific Product','N':'International Nonproprietary Name-INN','U':'Undefined'}
lov_OPERATION_TYPE = {'S':'Sale','R':'Refund'}
lov_SELL_PROD_MODE = {'S':'Scanned','M':'Manual'}
