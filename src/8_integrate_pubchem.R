pacman::p_load(tidyverse, biobricks, arrow)

pc <- bbload("pubchem")

# properties and activities ==============================================
# TODO pubchem activities may not always fit in ram
act <- pc$bioassay_concise |> 
  filter(property=="pubchem_activity_outcome") |>
  filter(value %in% c("Active", "Inactive")) |>
  filter(!is.na(pubchem_cid)) |>
  collect()

cid <- act$pubchem_cid |> unique()
cmp <- pc$compound_sdf |> filter(id %in% cid) |> collect() 
cmp <- cmp |> tidyr::pivot_wider(
  id_cols=id, names_from=property, values_from=value) |>
  mutate(sid = sapply(id, uuid::UUIDgenerate))

act <- act |> inner_join(cmp, by=c("pubchem_cid"="id"))

# TODO load more information about properties from pubchem
# generate pids for each aid
act <- act |> group_by(aid) |> mutate(pid=uuid::UUIDgenerate())


# Export Chemicals ============================================================
subjson <- pc$compound_sdf |> select(sid, inchi, casrn, preferredName) |> distinct() |> nest(data = -sid) |> 
  mutate(data = map_chr(data, ~ jsonlite::toJSON(as.list(.), auto_unbox = TRUE)))

arrow::write_parquet(subjson, fs::path(stg,"substances.parquet"))

# Export Properties ====================================================
propjson <- iceb |> select(pid, Assay, Endpoint, Units) |> distinct() |> nest(data = -pid) |>
  mutate(data = map_chr(data, ~ jsonlite::toJSON(as.list(.), auto_unbox = T)))

arrow::write_parquet(propjson, fs::path(stg,"properties.parquet"))

# Export Activities ====================================================
activities <- iceb |>
  mutate(aid = paste0("ice-", row_number())) |>
  select(aid, sid, pid, inchi, value=Response)

arrow::write_parquet(activities, fs::path(stg,"activities.parquet"))
