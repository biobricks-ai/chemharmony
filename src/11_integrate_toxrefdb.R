pacman::p_load(biobricks, tidyverse, arrow, uuid, jsonlite, kit, glue, httr, memoise, RSQLite, magrittr)

toxref <- bbassets("toxrefdb")$toxrefdb_sqlite %>% dbConnect(RSQLite::SQLite(), .)
stg <- fs::dir_create("staging/toxrefdb")


# BUILD CHEMICALS ==============================================================

## the comptox asset gives us inchi values for dtxsids
comptox  <- biobricks::bbassets("comptox")$dsstox_identifiers |> 
  arrow::open_dataset() |> collect() |>
  select(dsstox_substance_id=dtxsid,inchi)

chemical <- tbl(toxref, "chemical") |> 
    select(chemical_id, dsstox_substance_id, casrn, preferred_name) |>
    mutate(sid = uuid::UUIDgenerate()) |>
    collect() |>
    inner_join(comptox,by="dsstox_substance_id") |>
    filter(!is.na(inchi))

subjson <- chemical |> 
    select(sid, inchi, casrn, preferred_name, dsstox_substance_id) |> distinct() |> 
    nest(data = -sid) |> 
    mutate(data = map_chr(data, ~ jsonlite::toJSON(as.list(.), auto_unbox = TRUE)))

arrow::write_parquet(subjson, fs::path(stg,"substances.parquet"))

# BUILD PROPERTIES ==============================================================

## get guideline information - each study has a single guideline
property <- tbl(toxref, "study") |> 
    inner_join(tbl(toxref,"guideline"), by="guideline_id") |> 
    collect() |>
    filter(!is.na(guideline_number)) |> # ignore rows w/out guideline
    group_by(guideline_number) |> mutate(pid = uuid::UUIDgenerate()) |> ungroup() |>
    select(pid, study_id, guideline_number, guideline_name=name)

propjson <- property |> 
    select(pid, guideline_number, guideline_name) |> distinct() |> 
    nest(data = -pid) |> 
    mutate(data = map_chr(data, ~ jsonlite::toJSON(as.list(.), auto_unbox = TRUE)))

arrow::write_parquet(propjson, fs::path(stg,"properties.parquet"))

# BUILD ACTIVITIES ==============================================================

# adverse point of departure
adverse_pod <- tbl(toxref, "pod") |> 
    filter(pod_type=="noael") |>
    mutate(value = ifelse(dose_level < max_dose_level,"positive","negative")) |>
    select(chemical_id, study_id, value) |> 
    collect() 

pod <- adverse_pod |>
    inner_join(chemical,by="chemical_id") |>
    inner_join(property,by="study_id") |>
    select(sid, pid, inchi, value) |> distinct() |>
    mutate(aid = paste0("toxrefdb-", row_number())) |>
    select(aid, sid, pid, inchi, value)

arrow::write_dataset(pod, fs::path(stg,"activities.parquet"))