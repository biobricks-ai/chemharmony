# STORE `brick/tox21` with `substances.parquet`, `properties.parquet`, and `activities.parquet`
pacman::p_load(biobricks, tidyverse, arrow, uuid, jsonlite)

stg <- fs::dir_create("staging/ice")
uid <- UUIDgenerate

ice <- biobricks::bbassets("ice") |> map( ~ arrow::open_dataset(.) |> collect())
comptox <- biobricks::bbassets("comptox")$dsstox_identifiers_parquet |> 
  arrow::open_dataset() |> 
  collect()

# get size of each table
sizes = ice |> map_int(~ n_distinct(.$DTXSID)) |> sort()
tibble(table = names(sizes), size = sizes) |> filter(size > 0 )

# munge all tables with 1000+ distinct DTXSID
#    table                                               size status
#    <chr>                                              <int> 
#  1 Endocrine_In_Vitro_Endocrine_parquet                 166 ignore
#  2 Endocrine_In_Vivo_Endocrine_parquet                  218 ignore
#  3 Acute_Dermal_Toxicity_Data_parquet                   277 ignore
#  4 Eye_Irritation_Data_parquet                          455 ignore
#  5 DART_Data_parquet                                    628 ignore
# 14 Exposure_Predictions_Data_parquet                 479584 ignore -- these are predictions

#  7 Acute_Inhalation_Toxicity_Data_parquet              1696 o

# 13 Chemical_Functional_Use_Categories_Data_parquet   194114 x
# 12 cHTS2022_invitrodb34_20220302_parquet               9329 x
# 11 Acute_Oral_Toxicity_Data_parquet                    8636 x
# 10 Cancer_Data_parquet                                 3038 x
#  6 Skin_Irritation_Skin_Irritation-Corrosion_parquet   1061 x
#  9 ADME_Parameters_Data_parquet                        2088 x
#  8 Skin_Sensitization_Data_parquet                     1727 x

# IRRITATION ============================================================================
# TODO this leaves a lot of data out. 
irri <- ice$`Skin_Irritation_Skin_Irritation-Corrosion_parquet`
irri <- irri |> select(DTXSID, Assay, Endpoint, Response, Units) # 14,494 more rows
irri <- irri |> filter(Response %in% c("C","NC","Active","Inactive","0","1","2","3")) 
irri <- irri |> 
  mutate(Response = ifelse(Response %in% c("C","Active","1","2","3"),"positive","negative")) |>
  mutate(data_type = "Skin Irritation and Corrosion")

# ADME ==================================================================================
adme <- ice$ADME_Parameters_Data_parquet
adme <- adme |> select(DTXSID,Assay,Endpoint,Response,Units=`Response Unit`)
adme <- adme |> mutate(Response = as.numeric(Response))
adme <- adme |> group_by(Assay,Endpoint,Units) |> 
  mutate(medR = median(Response), Response = ifelse(Response<medR,"negative","positive")) |> 
  ungroup() |>
  mutate(data_type = "Absorption Distribution Metabolism Excretion Parameters") |>
  select(DTXSID, Assay, Endpoint, Response, Units, data_type)

# SENSITIZATION =========================================================================
# TODO this leaves about 20% of the data out
sens <- ice$Skin_Sensitization_Data_parquet
sens <- sens |> select(DTXSID, Assay, Endpoint, Response, Units=`Response Unit`)
sens <- sens |> filter(Response %in% c("Inactive","Active","Non-sensitizer","Sensitizer"))
sens <- sens |> 
  mutate(Response = ifelse(Response %in% c("Active","Sensitizer"),"positive","negative")) |>
  mutate(data_type = "Skin Sensitization")

# CANCER ================================================================================
# TODO handle the 'Cancer Classification' endpoint
canc <- ice$Cancer_Data_parquet
canc <- canc |> select(DTXSID, Assay, Endpoint, Response, Units=Response.Unit)
canc <- canc |> filter(Response %in% c("Negative","Positive"))
canc <- canc |> mutate(Response = ifelse(Response == "Positive","positive","negative")) |>
  mutate(data_type = "Cancer")

# ORAL ==================================================================================
oral <- ice$Acute_Oral_Toxicity_Data_parquet
oral <- oral |> select(DTXSID, Assay, Endpoint, Response, Units=`Response Unit`) 
oral <- oral |> mutate(Response = as.numeric(Response))

oral_epa <- oral |> filter(Endpoint=="EPA classification") |> 
  mutate(Response = ifelse(Response==4,"negative","positive"))
oral_ghs <- oral |> filter(Endpoint=="GHS classification") |> 
  mutate(Response = ifelse(Response==5,"negative","positive"))

oral_ld50 <- oral |> filter(Endpoint == "LD50",!is.na(Response))
oral_ld50 <- oral |> mutate(Response = ifelse(Response >=2000,"negative","positive"))
oral <- bind_rows(oral_epa, oral_ghs, oral_ld50) |> mutate(data_type = "Acute Oral Toxicity") |>
  select(DTXSID, Assay, Endpoint, Response, Units, data_type)

# CHTS ==================================================================================
chts <- ice$cHTS2022_invitrodb34_20220302_parquet
chts <- chts |> select(DTXSID, Assay, Endpoint, Response, Units=ResponseUnit)
chts <- chts |> filter(Response %in% c("Active","Inactive"))
chts <- chts |> 
  mutate(Response = ifelse(Response == "Active","positive","negative")) |>
  mutate(data_type = "High Throughput Screening")

# FUNCTIONAL USE ========================================================================
funct_use <- ice$Chemical_Functional_Use_Categories_Data_parquet |> mutate(Assay=Endpoint, Endpoint=Response, Response="positive")
funct_use <- funct_use |> select(DTXSID, Assay, Endpoint, Response, Units=`Response Unit`)
funct_use <- funct_use |> filter(Assay == "OECD Functional Use")
funct_use <- funct_use |> group_by(Endpoint) |> filter(n() > 100) |> ungroup()

## create negatives by stratified sampling missing dtxsid + response values
all_combinations <- expand.grid(DTXSID = unique(funct_use$DTXSID), Assay=unique(funct_use$Assay), Endpoint = unique(funct_use$Endpoint), Units=unique(funct_use$Units))
funct_use <- funct_use |> right_join(all_combinations, by=c("DTXSID","Endpoint","Assay","Units")) |> 
  mutate(Response = ifelse(is.na(Response),"negative","positive")) |>
  mutate(data_type = "OECD Functional Use")

funct_use <- funct_use |> group_by(Endpoint) |> 
  mutate(pos=sum(Response=="positive"), neg=sum(Response=="negative"), mincnt = min(neg,pos)) |> ungroup()

funct_use <- funct_use |> group_by(Endpoint,Response) |> sample_n(size=mincnt) |> ungroup()
funct_use <- funct_use |> select(DTXSID, Assay, Endpoint, Response, Units, data_type)

# INHALATION USE ========================================================================
inhale <- ice$Acute_Inhalation_Toxicity_Data_parquet
inhale <- inhale |> select(DTXSID, Assay, Endpoint, Response, Units=`Response Unit`)
inhale <- inhale |> mutate(Response = as.numeric(Response))

inhale_epa <- inhale |> filter(Endpoint=="EPA Classification, Acute Inhalation") |> 
  mutate(Response = ifelse(Response==4,"negative","positive"))
inhale_ghs <- inhale |> filter(Endpoint=="GHS Classification, Acute Inhalation") |> 
  mutate(Response = ifelse(Response>=4,"negative","positive"))
inhale_lc50 <- inhale |> filter(Endpoint == "LC50",!is.na(Response), Units=="mg/L") |> 
  mutate(Response = ifelse(Response >10,"negative","positive"))

inhale <- bind_rows(inhale_epa, inhale_ghs, inhale_lc50) |> 
  mutate(data_type = "Acute Inhalation Toxicity") |>
  select(DTXSID, Assay, Endpoint, Response, Units, data_type)

# PUT IT TOGETHER =======================================================================
iceb <- bind_rows(irri, adme, sens, canc, oral, chts, funct_use, inhale)

iceb <- iceb |> rename(dtxsid = DTXSID) |> inner_join(comptox, by="dtxsid")
iceb <- iceb |> filter(!is.na(inchi))
iceb <- iceb |> group_by(dtxsid) |> mutate(sid=uid()) |> ungroup()

# every property response pair must have at least 50 examples
# every property must have at least two responses
iceb <- iceb |> group_by(Assay,Endpoint,Units) |> mutate(pid = uid()) |> ungroup()
iceb <- iceb |> group_by(pid,Response) |> filter(n() > 50) |> ungroup()
iceb <- iceb |> group_by(pid) |> filter(n_distinct(Response) > 1) |> ungroup()

# Export Chemicals ============================================================
subjson <- iceb |> select(sid, inchi, casrn, preferredName) |> distinct() |> nest(data = -sid) |> 
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
