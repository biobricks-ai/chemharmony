# STORE `brick/tox21` with `substances.parquet`, `properties.parquet`, and `activities.parquet`
pacman::p_load(biobricks, tidyverse, arrow, uuid, jsonlite)

# tox21 easily fits in memory
tox21 <- biobricks::bbload("tox21")
toxraw <- tox21$tox21 |> collect()
toxlib <- tox21$tox21lib |> collect() # substance identifiers
toxagg <- tox21$tox21_aggregated |> collect()

# write tox21 to staging and later merge it
stg <- fs::dir_create("staging")

# Export Chemicals ============================================================
uid <- UUIDgenerate
sub <- toxlib |> group_by(SAMPLE_ID) |> mutate(sid = uid()) |> ungroup()
subjson <- sub |> 
  select(sid, SAMPLE_NAME, SAMPLE_ID, CAS, PUBCHEM_CID, SMILES, TOX21_ID) |>
  distinct() |> nest(data = -sid) |> 
  mutate(data = map_chr(data, ~ jsonlite::toJSON(as.list(.), auto_unbox = TRUE)))

arrow::write_parquet(subjson, fs::path(stg,"substances.tox21.parquet"))

# Export Properties ====================================================
pcols <- c("PROTOCOL_NAME")

props <- toxraw |> group_by(!!!syms(pcols)) |> mutate(pid = uid()) |> ungroup()
propjson <- props |> select(pid, !!!syms(pcols)) |> distinct() |> nest(data = -pid) 
propjson$data <- map_chr(propjson$data, ~ jsonlite::toJSON(as.list(.), auto_unbox = T))

arrow::write_parquet(propjson, fs::path(stg,"properties.tox21.parquet"))

# Export Activities ====================================================

# get the inactive/agonist/antagonist list
legal_outcomes <- c("active agonist", "active antagonist", "inactive")
acts <- toxraw |> filter(ASSAY_OUTCOME %in% legal_outcomes)
acts <- acts |> select(SAMPLE_ID, PROTOCOL_NAME, ASSAY_OUTCOME)

subsid <- sub |> select(SAMPLE_ID, sid, SMILES) |> distinct()
acts <- acts |> inner_join(subsid, by="SAMPLE_ID")

proppid <- props |> select(PROTOCOL_NAME, pid) |> distinct()
acts <- acts |> inner_join(proppid, by="PROTOCOL_NAME")

activities <- acts |>
  mutate(source_id = row_number()) |>
  mutate(source_id = paste0("tox21-tox21.parquet", source_id)) |>
  select(source_id, sid, pid, value=ASSAY_OUTCOME, smiles=SMILES)

arrow::write_parquet(activities, fs::path(stg,"activities.tox21.parquet"))
