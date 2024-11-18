## File Parser SDK
File Parser SDK is a Python library designed to simplify the parsing of various file formats (eg. TEXT, CSV, EXCEL, ZIP, XML, PDF) with a customizable transforming payloads as required. This SDK offers seamless integration, efficient file handling, and the flexibility to address edge cases with user-defined logic tailored to transforming entries as needed.

### Features
- **Multi-format Support:** Parse TEXT, CSV, EXCEL, ZIP, XML and PDF files effortlessly from AWS S3.
- **Customizable Edge Case Handling:** Define and apply custom functions to handle specific parsing requirements.
- **S3 Integration:** Supports fetching files directly from AWS S3 buckets based on IAM role.
- **Simple Configuration:** Initialize with straightforward configurations, avoiding the need for additional setup files.

### Installation
Install the SDK using pip:
```
pip install file_parser_sdk
```

### Prerequisites
- **Your application should be deployed on AWS EKS to enable the SDK to utilize AWS S3 credentials.**
- **Python:** >= '3.6'
- **Pandas:** '2.0.0'

### Getting Started
- **Define Custom Edge Cases:**
When specific functions are needed during file parsing, the SDK will import edge cases from your project structure as shown below. To implement this, create an edgeCases folder in your project and add a file named user_edge_cases.py. Define your custom functions in this file, and reference them in the edge_case section within the file_config as shown below.
```
from edgeCases import user_edge_cases
self.edge_cases = user_edge_cases
```

- **Define the configuration required for file parsing logic and S3 bucket names**
```
    s3_config: {
        upload_bucket: reconciliation-live
    download_bucket: reconciliation-live
    }
    file_config: {
        "file_source_1": {
            "read_from_s3_func":"read_complete_excel_file",
            "parameters_for_read_s3": None,
            "file_dtype":{
                "Order_Number": str,
                "PG_Ref":str,
                "AG_Ref":str
            },
            "map_based_on_txn_type":False,
            "columns_mapping":{
                "Transaction_Type": constants.MisTransactionType,
                "Merchant_Name": card_constants.MisMerchantName,
                "Merchant_ID": card_constants.MisMerchantId,
                "Amount": constants.MisAmount,
                "Order_Number": constants.MisTxRef,
                "PG_Ref": constants.MisPgReferenceId,
                "Settlement_Date": card_constants.MisSettlementDate,
                "Transaction_Date": constants.MisDateTime,
                "Fee": card_constants.MisServiceCharge,
                "ME_IGST": card_constants.MisServiceTax,
                "Net_Amount": card_constants.MisNetSettlementAmount
            },
            "sale_transaction":{
                "is_present":True,
                "filter_values":["P"]
            },
            "refund_transaction":{
                "is_present":True,
                "filter_values":["R"]
            },
            "chargeback_transaction":{
                "is_present":True,
                "filter_values":["A"]
            },
            "transaction_date_format":constants.common_date_format,
            "drop_txn_time_from_date_time":True,
            "edge_case": {
                "add_reversal_tx_ref_column": constants.reversal_txRef
            }
        },
    }
```

- **Define a ParsedDataResponseType enum**
```
import enum
class ParsedDataResponseType(enum.Enum):
    DATAFRAME="DATAFRAME"
    FILE="FILE"
    JSON="JSON"
```

- **Import and initialise the file parser**
```
from file_parser_sdk import FileParserSDK

parser = FileParser(config={s3_config: s3_config, file_config: file_config})
parsed_data = parser.parse("s3://your-bucket-name/path/to/your/file.csv", file_source, ParsedDataResponseType.DATAFRAME.value)
//By default SDK will provide response as DATAFRAME
```

