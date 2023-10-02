# TODO this data source seems corrupted, ecnumbers map to multiple smiles.
reticulate::use_virtualenv("./env", required = TRUE)
pacman::p_load(biobricks, tidyverse, arrow, uuid, jsonlite)

stg <- fs::dir_create("staging/reach")
uid <- UUIDgenerate

# reach easily fits in memory
reachraw <- biobricks::bbload("reach")$reach |> collect()

# make sids, drop sids with multiple smiles
reach <- reachraw |> group_by(ecnumber) |> mutate(sid = uid()) |> ungroup()
reach <- reach |> group_by(sid) |> filter(n_distinct(smiles)==1) |> ungroup()

# make pids
reach <- reach |> group_by(hazard) |> mutate(pid = uid()) |> ungroup()

# build inchi
rdkit <- reticulate::import("rdkit")
smiles <- reach |> select(smiles) |> distinct() |> pull()
smiles2inchi <- possibly(function(smi) {
  mol <- rdkit$Chem$MolFromSmiles(smi)
  rdkit$Chem$MolToInchi(mol)
}, otherwise = NA_character_, quiet = TRUE)

inchi <- purrr::map_chr(smiles, smiles2inchi)
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
acts <- reach |> select(sid,pid,inchi,value) |> distinct() |>
  mutate(aid = paste0("reach-", row_number())) |>
  select(aid, sid, pid, inchi, value) 

arrow::write_parquet(acts, fs::path(stg,"activities.parquet"))
