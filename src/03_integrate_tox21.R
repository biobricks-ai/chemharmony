reticulate::use_virtualenv("./env", required = TRUE)
pacman::p_load(biobricks, tidyverse, arrow, uuid, jsonlite)

# tox21 easily fits in memory
tox21 <- biobricks::bbload("tox21")
toxraw <- tox21$tox21 |> collect()
toxlib <- tox21$tox21lib |> collect() # substance identifiers
toxagg <- tox21$tox21_aggregated |> collect()

# write tox21 to staging and later merge it
stg <- fs::dir_create("staging/tox21")
uid <- UUIDgenerate

legal_outcomes <- c("active agonist", "active antagonist", "inactive")
acts <- toxraw |> 
  filter(CHANNEL_OUTCOME %in% legal_outcomes) |> 
  filter(PURITY_RATING %in% c("A","AC","B","BC")) |>
  filter(PURITY_RATING_4M %in% c("A","AC","B","BC")) |>
  filter(REPRODUCIBILITY %in% c("active_match","inactive_match"))

# make sid and pid
acts <- acts |> group_by(PUBCHEM_CID) |> mutate(sid = UUIDgenerate()) |> ungroup()
acts <- acts |> group_by(PROTOCOL_NAME, SAMPLE_DATA_TYPE) |> mutate(pid = UUIDgenerate()) |> ungroup()

# Export Chemicals ============================================================
sub <- toxlib |> inner_join(acts |> select(sid, PUBCHEM_CID) |> distinct(), by="PUBCHEM_CID")
subjson <- sub |> 
  select(sid, SAMPLE_NAME, SAMPLE_ID, CAS, PUBCHEM_CID, SMILES, TOX21_ID) |>
  distinct() |> nest(data = -sid) |> 
  mutate(data = map_chr(data, ~ jsonlite::toJSON(as.list(.), auto_unbox = TRUE)))

arrow::write_parquet(subjson, fs::path(stg,"substances.parquet"))

# Export Properties ====================================================
pcols <- c("PROTOCOL_NAME", "SAMPLE_DATA_TYPE")
propjson <- acts |> select(pid, !!!syms(pcols)) |> distinct() |> nest(data = -pid) 
propjson$data <- map_chr(propjson$data, ~ jsonlite::toJSON(as.list(.), auto_unbox = T))

arrow::write_parquet(propjson, fs::path(stg,"properties.parquet"))

# Export Activities ====================================================

# TODO it would be better to do this with an inner join on pubchem
rdkit <- reticulate::import("rdkit")
smiles2inchi <- purrr::possibly(function(smiles){
  mol <- rdkit$Chem$MolFromSmiles(smiles)
  inchi <- rdkit$Chem$MolToInchi(mol)
  return(inchi)
}, otherwise = NA_character_)

sid_inchi = sub |> 
  group_by(sid) |> slice(1) |> ungroup() |> # TODO why do some sids have multiple smiles?
  select(sid,SMILES) |>
  mutate(inchi = map_chr(SMILES, smiles2inchi)) |> 
  select(sid, inchi) |> distinct() |>
  filter(!is.na(inchi))

activities <- acts |> group_by(sid, pid) |> 
  summarize(value = names(which.max(table(CHANNEL_OUTCOME)))) |> ungroup() |>
  distinct() |>
  mutate(aid = paste0("tox21-", row_number())) |>
  inner_join(sid_inchi, by="sid") |>
  select(aid, sid, pid, inchi, value)

# each pid value pair should have at least 100 examples
activities <- activities |> group_by(pid, value) |>  filter(n() > 100) |> ungroup()

# each pid should have at least 2 values
activities <- activities |> group_by(pid) |> filter(n_distinct(value) > 1) |> ungroup()

arrow::write_parquet(activities, fs::path(stg,"activities.parquet"))