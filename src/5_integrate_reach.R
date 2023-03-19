reticulate::use_virtualenv("./env", required = TRUE)
pacman::p_load(biobricks, tidyverse, arrow, uuid, jsonlite, furrr, progressr)

stg <- fs::dir_create("staging/reach")
uid <- UUIDgenerate

# reach easily fits in memory
reach <- biobricks::bbload("reach")$reach |> collect()
reach <- reach |> group_by(ecnumber) |> mutate(sid = uid()) |> ungroup()
reach <- reach |> group_by(hazard) |> mutate(pid = uid()) |> ungroup()

# build inchi
smiles <- reach |> select(smiles) |> distinct() |> pull()
smiles2inchi <- possibly(function(smi) {
  mol <- rdkit$Chem$MolFromSmiles(smi)
  return(rdkit$Chem$MolToInchi(mol))
}, otherwise = NA_character_, quiet = TRUE)

inchi <- map_chr(smiles, smiles2inchi, .progress=TRUE)
smi2inchi <- tibble(smiles=smiles, inchi=inchi) |> filter(!is.na(inchi))

reach <- reach |> inner_join(smi2inchi, by="smiles")


# Export Chemicals ============================================================
subjson <- reach |> 
  select(sid, ecnumber,smiles,inchi) |> distinct() |> nest(data = -sid) |> 
  mutate(data = map_chr(data, ~ toJSON(as.list(.), auto_unbox = TRUE)))

arrow::write_parquet(subjson, fs::path(stg,"substances.parquet"))

# Export Properties ====================================================
propjson <- reach |> select(pid, hazard) |> distinct() |> nest(data = -pid) |>
  mutate(data = map_chr(data, ~ toJSON(as.list(.), auto_unbox = T)))

arrow::write_parquet(propjson, fs::path(stg,"properties.parquet"))

# Export Activities ====================================================

acts <- reach |> select(sid,pid,smiles,inchi,nominal_value=value)
acts <- acts |> mutate(numeric_value = ifelse(nominal_value=="negative",0,1))
acts <- acts |>
  mutate(source_id = row_number()) |>
  mutate(source_id = paste0("reach-reach.parquet", source_id)) |>
  select(source_id, sid, pid, inchi, smiles, nominal_value, numeric_value)

arrow::write_parquet(activities, fs::path(stg,"activities.parquet"))
