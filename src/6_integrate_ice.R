# STORE `brick/tox21` with `substances.parquet`, `properties.parquet`, and `activities.parquet`
pacman::p_load(biobricks, tidyverse, arrow, uuid, jsonlite)

stg <- fs::dir_create("staging/ice")
uid <- UUIDgenerate

ice <- biobricks::bbload("ice") |> map( ~ collect(.))
comptox <- biobricks::bbload("comptox")[[1]] |> collect()

# get size of each table
sizes = ice |> map(~ n_distinct(.$DTXSID)) |> unlist() |> sort()

# munge all tables with 1000+ distinct DTXSID
irri <- ice$`Skin_Irritation_Skin_Irritation-Corrosion`
irri <- irri |> select(DTXSID, Assay, Endpoint, Response, Units)
irri <- irri |> filter(Response %in% c("C","NC","Active","Inactive","0","1","2","3"))

adme <- ice$ADME_Parameters_ADME_Parameter 
adme <- adme |> select(DTXSID,Assay,Endpoint,Response,Units)
adme <- adme |> mutate(Response = as.numeric(Response))
adme <- adme |> mutate(Response = ntile(Response, 4))
adme <- adme |> mutate(Response = sprintf("quartile_%s", Response))

sens <- ice$Skin_Sensitization_Chemicals
sens <- sens |> select(DTXSID, Assay, Endpoint, Response=Value, Units=Unit)
sens <- sens |> filter(Response %in% c("Inactive","Active","Non-sensitizer","Sensitizer"))

canc <- ice$Cancer_Data
canc <- canc |> select(DTXSID, Assay, Endpoint, Response, Units)
canc <- canc |> filter(Response %in% c("Negative","Positive"))

oral <- ice$Acute_Oral_Toxicity_Acute_Oral_Toxicity
oral <- oral |> select(DTXSID, Assay, Endpoint, Response, Units)
oral <- oral |> filter(Endpoint == "LD50",!is.na(Response))
oral <- oral |> mutate(Response = as.numeric(Response))
oral <- oral |> mutate(Response = ntile(Response, 4))
oral <- oral |> mutate(Response = sprintf("quartile_%s", Response))

chts <- ice$cHTS2022_invitrodb34_20220302
chts <- chts |> select(DTXSID, Assay, Endpoint, Response, Units=ResponseUnit)
chts <- chts |> filter(Response %in% c("Active","Inactive"))

iceb <- bind_rows(irri, adme, sens, canc, oral, chts)

iceb <- iceb |> rename(dtxsid = DTXSID) |> inner_join(comptox, by="dtxsid")
iceb <- iceb |> filter(!is.na(inchi))
iceb <- iceb |> group_by(dtxsid) |> mutate(sid=uid()) |> ungroup()

# every property response pair must have at least 200 examples
# every property must have at least two responses
iceb <- iceb |> group_by(Assay,Endpoint,Units) |> mutate(pid = uid()) |> ungroup()
iceb <- iceb |> group_by(pid,Response) |> filter(n() > 200) |> ungroup()
iceb <- iceb |> group_by(pid) |> filter(n_distinct(Response) > 1) |> ungroup()

# Export Chemicals ============================================================
subjson <- iceb |> select(sid, inchi, casrn, preferredName) |> distinct() |> nest(data = -sid) |> 
  mutate(data = map_chr(data, ~ jsonlite::toJSON(as.list(.), auto_unbox = TRUE)))

arrow::write_parquet(subjson, fs::path(stg,"substances.parquet"))

# Export Properties ====================================================
propjson <- iceb |> select(pid, Assay, Endpoint, Units) |> distinct() |> nest(data = -pid) |>
  mutate(data = map_chr(propjson$data, ~ jsonlite::toJSON(as.list(.), auto_unbox = T)))

arrow::write_parquet(propjson, fs::path(stg,"properties.parquet"))

# Export Activities ====================================================

activities <- iceb |>
  mutate(source_id = row_number()) |>
  mutate(source_id = paste0("ice", source_id)) |>
  select(source_id, sid, pid, inchi, value=Response)

arrow::write_parquet(activities, fs::path(stg,"activities.parquet"))
