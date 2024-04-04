pacman::p_load(biobricks, tidyverse, arrow, uuid, jsonlite)

# tox21 easily fits in memory
tox21 <- biobricks::bbassets("tox21") |> map(arrow::open_dataset)
toxraw <- tox21$tox21_parquet |> collect()
toxlib <- tox21$tox21lib_parquet |> collect() # substance identifiers
toxagg <- tox21$tox21_aggregated_parquet |> collect()

# write tox21 to staging and later merge it
stg <- fs::dir_create("staging/tox21")
uid <- UUIDgenerate

legal_outcomes <- c("active agonist", "active antagonist", "inactive")
acts1 <- toxagg |> 
  filter(!is.na(ASSAY_OUTCOME), !is.na(SMILES)) |>
  filter(ASSAY_OUTCOME %in% legal_outcomes) |> 
  filter(PURITY_RATING == "A") |>
  filter(REPRODUCIBILITY %in% c("active_match","inactive_match")) |>
  filter(SAMPLE_DATA_TYPE %in% c("activity","viability","control","signal")) |>
  mutate(PUBCHEM_CID = as.numeric(PUBCHEM_CID)) |>
  select(SAMPLE_ID, PROTOCOL_NAME, SAMPLE_DATA_TYPE, ASSAY_OUTCOME, PUBCHEM_CID, SAMPLE_NAME, SMILES, CAS, TOX21_ID)

# make sid
acts2 <- acts1 |> group_by(SMILES) |> mutate(sid = UUIDgenerate()) |> filter(n_distinct(SAMPLE_ID)==1) |> ungroup()

# split into positive and negative
posneg <- {
  inactivedf <- acts2 |> filter(ASSAY_OUTCOME == "inactive")
  is_agonist <- acts2 |> filter(ASSAY_OUTCOME == "active agonist") |> mutate(value="positive")
  antagonist <- acts2 |> filter(ASSAY_OUTCOME == "active antagonist") |> mutate(value="positive")

  ago <- is_agonist |> 
    bind_rows(inactivedf |> mutate(ASSAY_OUTCOME="active agonist", value="negative")) |> 
    bind_rows(antagonist |> mutate(ASSAY_OUTCOME="active agonist", value="negative")) 
  
  ant <- antagonist |> 
    bind_rows(inactivedf |> mutate(ASSAY_OUTCOME="active antagonist", value="negative")) |> 
    bind_rows(is_agonist |> mutate(ASSAY_OUTCOME="active antagonist", value="negative"))

  bind_rows(ago, ant)
}

# make pid
acts3 <- posneg |> group_by(PROTOCOL_NAME, SAMPLE_DATA_TYPE, ASSAY_OUTCOME) |> mutate(pid = UUIDgenerate()) |> ungroup()

# remove examples with multiple values
acts4 <- acts3 |> group_by(sid, pid) |> filter(n_distinct(value) == 1) |> ungroup()

# remove properties with less than 100 examples for one of their values
acts5 <- acts4 |> group_by(pid, value) |> filter(n() > 100) |> ungroup()
acts <- acts5 |> group_by(pid) |> filter(n_distinct(value) == 2) |> ungroup()

# Export Chemicals ============================================================
sub <- toxlib |> inner_join(acts |> select(sid, CAS) |> distinct(), by="CAS")

subjson <- sub |> 
  select(sid, SAMPLE_NAME, SAMPLE_ID, CAS, PUBCHEM_CID, SMILES, TOX21_ID) |>
  distinct() |> nest(data = -sid) |> 
  mutate(data = map_chr(data, ~ jsonlite::toJSON(as.list(.), auto_unbox = TRUE)))

arrow::write_parquet(subjson, fs::path(stg,"substances.parquet"))

# Export Properties ====================================================
pcols <- c("PROTOCOL_NAME", "SAMPLE_DATA_TYPE", "ASSAY_OUTCOME")
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

sid_inchi = sub |> select(sid, SMILES) |> distinct() |>
  mutate(inchi = map_chr(SMILES, smiles2inchi)) |> 
  filter(!is.na(inchi)) |>
  select(sid, inchi)

activities <- acts |> 
  mutate(aid = paste0("tox21-", row_number())) |>
  inner_join(sid_inchi, by="sid") |>
  select(aid, sid, pid, inchi, value)

max_sid_pid_count <- activities |> count(sid, pid) |> pull(n) |> max()
assertthat::assert_that(max_sid_pid_count == 1, msg = "each sid,pid should only have 1 entry.")
arrow::write_parquet(activities, fs::path(stg,"activities.parquet"))