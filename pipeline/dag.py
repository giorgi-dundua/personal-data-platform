from pipeline.nodes import (
    ingestion_stage,
    normalization_stage,
    validation_stage,
    merge_stage,
)

PIPELINE_DAG = {
    "ingestion": {
        "fn": ingestion_stage,
        "depends_on": [],
        "produces": ["raw_data"],
        "consumes": [],
    },

    "normalization": {
        "fn": normalization_stage,
        "depends_on": ["ingestion"],
        "produces": ["normalized_data"],
        "consumes": ["raw_data"],
    },

    "validation": {
        "fn": validation_stage,
        "depends_on": ["normalization"],
        "produces": ["validated_data"],
        "consumes": ["normalized_data"],
    },

    "merge": {
        "fn": merge_stage,
        "depends_on": ["validation"],
        "produces": ["daily_metrics"],
        "consumes": ["validated_data"],
    },
}
