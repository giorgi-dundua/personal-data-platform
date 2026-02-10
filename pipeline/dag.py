from pipeline.nodes import ingestion_stage, normalization_stage, validation_stage, merge_stage
from processing.normalizers.google_sheets_normalizer import GoogleSheetsNormalizer
from processing.normalizers.mi_band_normalizer import MiBandNormalizer
from processing.validators.validate import Validator
from processing.aggregators.merge_daily_metrics import merge_daily_metrics

PIPELINE_DAG = {
    "ingestion": {
        "fn": ingestion_stage,
        "depends_on": [],
        "produces": ["raw_data"],
        "consumes": [],
        "logic_hooks": [] 
    },
    "normalization": {
        "fn": normalization_stage,
        "depends_on": ["ingestion"],
        "produces": ["normalized_data"],
        "consumes": ["raw_data"],
        "logic_hooks": [GoogleSheetsNormalizer, MiBandNormalizer]
    },
    "validation": {
        "fn": validation_stage,
        "depends_on": ["normalization"],
        "produces": ["validated_data"],
        "consumes": ["normalized_data"],
        "logic_hooks": [Validator]
    },
    "merge": {
        "fn": merge_stage,
        "depends_on": ["validation"],
        "produces": ["daily_metrics"],
        "consumes": ["validated_data"],
        "logic_hooks": [merge_daily_metrics]
    },
}