pacman::p_load(tidyverse, biobricks, arrow)

pc <- bbload("pubchem")
stg <- fs::dir_create("staging/pubchem")

# properties and activities ==============================================
# TODO pubchem activities may not always fit in ram
act <- pc$bioassay_concise |> 
  filter(property=="pubchem_activity_outcome") |>
  filter(value %in% c("Active", "Inactive")) |>
  filter(!is.na(pubchem_cid)) |>
  collect()

cid <- act$pubchem_cid |> unique()
cmp <- pc$compound_sdf |> filter(property=="PUBCHEM_IUPAC_INCHI") |> collect() 
cmp <- cmp |> filter(id %in% cid) |> distinct()
cmp <- cmp |> group_by(id) |> summarize(sid=uuid::UUIDgenerate(),inchi=first(value)) |> ungroup()
cmp <- cmp |> select(sid, pubchem_cid=id, inchi)

act <- act |> inner_join(cmp, by="pubchem_cid")

# TODO load more information about properties from pubchem
# generate pids for each aid
act <- act |> group_by(aid) |> mutate(pid=uuid::UUIDgenerate())


# Export Chemicals ============================================================
subjson <- cmp |> select(sid, pubchem_cid, inchi) |> distinct() |> nest(data = -sid) |> 
  mutate(data = map_chr(data, ~ jsonlite::toJSON(as.list(.), auto_unbox = TRUE)))

arrow::write_parquet(subjson, fs::path(stg,"substances.parquet"))

# Export Properties ====================================================
propjson <- act |> select(pid, aid) |> distinct() |> nest(data = -pid) |>
  mutate(data = map_chr(data, ~ jsonlite::toJSON(as.list(.), auto_unbox = T)))

arrow::write_parquet(propjson, fs::path(stg,"properties.parquet"))

# Export Activities ====================================================
activities <- act |> select(sid,pid,inchi,value) |> distinct() |>
  mutate(aid = paste0("pubchem-", row_number())) |>
  select(aid, sid, pid, inchi, value)

arrow::write_parquet(activities, fs::path(stg,"activities.parquet"))
