# RISP Niger - Multi-campaign

This workflow extracts and transforms data collected from a IASO form filled by Niger's health ministry workers over the course of vaccination campaigns, and loads them into an OpenHEXA database that feeds a PowerBI dashboard allowing the live tracking of KPIs of these campaigns.

---

## 📐 Architecture & Workflow Overview

```mermaid
flowchart TD
    %% Input sources
    IASO_API["IASO"]
    TARGETS_IN["inputs/cibles/autres"]
    HIST_IN["inputs/cibles/historique"]
    CONFIG_IN["inputs/config"]

    %% Main pipelines
    P1["generate_targets_template"]
    P2["process_target_data"]
    P3["configure_new_campaign"]

    %% Other pipelines
    EXT_ORG["extract_org_units"]
    EXT_IASO["extract_iaso_form_data"]
    PROC_IASO["process_iaso_form_data"]
    EXP_STRUCT["create_expected_data_structure"]
    BUILD_VIZ["build_visualisation_tables"]
    HIST_TARGETS_PROC["process_historical_target_data"]
    HIST_EXP["create_expected_data_structure\n_for_historical_campaigns"]

    %% Database / output
    DB[("Database")]
    DASHBOARDS["Dashboards\n(Couverture · Complétude · Stocks\nSurveillance · Communications\nComparaisons des rounds)"]

    %% ── Flow ──────────────────────────────────────────────
    IASO_API --> EXT_IASO
    EXT_IASO -->|"combined_iaso_data_raw.parquet"| PROC_IASO
    PROC_IASO -->|"combined_iaso_data.parquet"| BUILD_VIZ
    BUILD_VIZ --> DB
    DB -.-> DASHBOARDS    

    IASO_API --> EXT_ORG
    EXT_ORG -->|"iaso_org_unit_tree_clean.parquet"| P1
    EXT_ORG -->|"iaso_org_unit_tree_clean.parquet"| PROC_IASO
    EXT_ORG -->|"iaso_org_unit_tree_clean.parquet"| HIST_TARGETS_PROC

    P1 -->|"Cibles_xxx_template.xlsx"| TMPL_IN
    TARGETS_IN --> P2
    P2 -->|"combined_configured_target_data.parquet"| P3
    P2 -->|"combined_target_data.parquet"| BUILD_VIZ

    P3 -->|"config_xxx.parquet"| CONFIG_IN
    CONFIG_IN --> EXP_STRUCT
    EXP_STRUCT -->|"expected_data_structure.parquet"| PROC_IASO

    HIST_IN --> HIST_TARGETS_PROC
    HIST_TARGETS_PROC -->|"combined_historical_target_data.parquet"| P2
    HIST_TARGETS_PROC -->|"combined_historical_target_data.parquet"| HIST_EXP

    HIST_EXP -->|"expected_data_structure_historical_campaigns.parquet"| EXP_STRUCT

    %% ── Styling ───────────────────────────────────────────
    style P1 fill:#e8d5f5,stroke:#9c27b0
    style P2 fill:#e8d5f5,stroke:#9c27b0
    style P3 fill:#dbeafe,stroke:#2563eb
    style HIST_TARGETS_PROC fill:#fee2e2,stroke:#dc2626
    style HIST_EXP fill:#fee2e2,stroke:#dc2626
    style EXP_STRUCT fill:#dbeafe,stroke:#2563eb
    style EXT_ORG fill:#dbeafe,stroke:#2563eb
    style EXT_IASO fill:#dbeafe,stroke:#2563eb
    style PROC_IASO fill:#dbeafe,stroke:#2563eb
    style BUILD_VIZ fill:#dbeafe,stroke:#2563eb
```
