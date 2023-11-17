pacman::p_load(biobricks, tidyverse, arrow, uuid, jsonlite, reticulate)

assets <- biobricks::bbassets("bindingdb")

raw_bindingdb <- arrow::open_dataset(assets$full_tsv_dump_parquet) |> collect()

property_columns <- c("Target Name",    
    "pH",
    "Temp (C)",
    "Target Source Organism According to Curator or DataSource",
    "Number of Protein Chains in Target (>1 implies a multichain complex)",
    "BindingDB Target Chain Sequence",
    "PDB ID(s) of Target Chain",
    "UniProt (SwissProt) Recommended Name of Target Chain",
    "UniProt (SwissProt) Entry Name of Target Chain",
    "UniProt (SwissProt) Primary ID of Target Chain",
    "UniProt (SwissProt) Secondary ID(s) of Target Chain",
    "UniProt (SwissProt) Alternative ID(s) of Target Chain",
    "UniProt (TrEMBL) Submitted Name of Target Chain",
    "UniProt (TrEMBL) Entry Name of Target Chain",
    "UniProt (TrEMBL) Primary ID of Target Chain",
    "UniProt (TrEMBL) Secondary ID(s) of Target Chain",
    "UniProt (TrEMBL) Alternative ID(s) of Target Chain",
    "Link to Target in BindingDB",
    "PDB ID(s) for Ligand-Target Complex")

substance_columns <- c("BindingDB Reactant_set_id",
    "Ligand SMILES",
    "Ligand InChI",
    "Ligand InChI Key",
    "BindingDB MonomerID",
    "BindingDB Ligand Name",
    "Link to Ligand in BindingDB",
    "Ligand HET ID in PDB",
    "PubChem CID",
    "PubChem SID",
    "ChEBI ID of Ligand",
    "ChEMBL ID of Ligand",
    "DrugBank ID of Ligand",
    "IUPHAR_GRAC ID of Ligand",
    "KEGG ID of Ligand",
    "ZINC ID of Ligand")

measured_value_cols = c(
    "Ki (nM)",
    "IC50 (nM)",
    "Kd (nM)",
    "EC50 (nM)",
    "kon (M-1-s-1)",
    "koff (s-1)"
)

bdb <- raw_bindingdb |> filter(!is.na(`Ligand InChI`))
bdb <- bdb |> pivot_longer(all_of(measured_value_cols), names_to = "metric", values_to = "value", values_drop_na = TRUE)
bdb <- bdb |> group_by(!!!syms(property_columns), metric) |> mutate(pid = uuid::UUIDgenerate()) |> ungroup()
bdb <- bdb |> group_by(!!!syms(substance_columns)) |> mutate(sid = uuid::UUIDgenerate()) |> ungroup()

stg <- fs::dir_create("staging/bindingdb")
toJ <- purrr::partial(jsonlite::toJSON, auto_unbox = TRUE)

# Export Chemicals ====================================================
substances <- bdb |> select(sid, all_of(substance_columns)) |> distinct()
substances <- substances |> nest(data = -sid) |> mutate(data = map_chr(data, ~ toJ(as.list(.))))

arrow::write_parquet(substances, fs::path(stg, "substances.parquet"))

# Export Properties ====================================================
properties <- bdb |> select(pid, all_of(property_columns), metric) |> distinct()
properties <- properties |> nest(data = -pid) |> mutate(data = map_chr(data,  ~ toJ(as.list(.))))

arrow::write_parquet(properties, fs::path(stg,"properties.parquet"))

# Export Activities ====================================================
activities <- bdb |> mutate(aid = paste0("bindingdb-",row_number()))
activities <- activities |> select(aid, sid, pid, inchi=`Ligand InChI`, metric, value)
activities <- activities |> mutate(value = as.numeric(gsub(">|<", "", value))) |> filter(!is.na(value)) 
activities <- activities |> filter(metric %in% c('EC50 (nM)','IC50 (nM)','Kd (nM)','Ki (nM)'))
activities <- activities |> mutate(
    numvalue = value,
    value = case_when(
    metric == "EC50 (nM)" & value < 100 ~ "positive",
    metric == "EC50 (nM)" & value >= 100 ~ "negative",
    metric == "IC50 (nM)" & value < 100 ~ "positive",
    metric == "IC50 (nM)" & value >= 100 ~ "negative",
    metric == "Kd (nM)" & value < 10 ~ "positive",
    metric == "Kd (nM)" & value >= 10 ~ "negative",
    metric == "Ki (nM)" & value < 10 ~ "positive",
    metric == "Ki (nM)" & value >= 10 ~ "negative"))

arrow::write_parquet(activities, fs::path(stg,"activities.parquet"))