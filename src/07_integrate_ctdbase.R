pacman::p_load(biobricks, tidyverse, arrow, uuid, jsonlite, kit, glue, httr, memoise)

ctd <- biobricks::bbassets("ctdbase")
stg <- fs::dir_create("staging/ctdbase")

# Export Chemicals ============================================================
chem1 <- ctd$CTD_chemicals_parquet |> arrow::open_dataset() |> collect()
chem2 <- chem1 |> filter(!is.na(CasRN)) |> collect()
nrow(chem2) # 56311

# look up chemicals by their mesh id on pubchem
httr::set_config(httr::config(http_version = 0))
get_cids_from_cas <- function(cas_number) {
  
  base_url <- "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/"
  url <- paste0(base_url, cas_number, "/cids/json")
  response <- GET(url)
  status <- http_status(response)
  if (status$category != "Success") { return(list()) }
  content <- content(response, as = "text", encoding = "UTF-8")
  json_data <- fromJSON(content, simplifyVector = TRUE)
  
  # sleep 300ms
  Sys.sleep(0.3)
  return(json_data$IdentifierList$CID)
}
get_cids_from_cas <- memoise::memoise(purrr::possibly(get_cids_from_cas, otherwise = list()), cache=cachem::cache_disk("cache/get_cids_from_cas"))
cids <- map(chem2$CasRN, get_cids_from_cas, .progress = TRUE)

pcc <- bbassets("pubchem")$compound_sdf_parquet |> arrow::open_dataset()
if(interactive()){ pcc <- pcc |> head(1e7) |> collect() |> tibble()} # for testing
pcc <- pcc |> filter(property=="PUBCHEM_IUPAC_INCHI")
pcc <- pcc |> select(pubchem_cid=id, inchi=value) |> collect()

# Remove chemicals with multiple pubchem cids and join with pubchem
chem3 <- chem2 |> mutate(pubchem_cid=cids) |> filter(map(pubchem_cid, length) == 1) |> mutate(pubchem_cid = unlist(pubchem_cid))
chem4 <- chem3 |> left_join(pcc, by="pubchem_cid") |> collect()
chem5 <- chem4 |> filter(!is.na(inchi)) |> collect() # remove chemicals without an inchi

# generate sids and substances.parquet
chem6 <- chem5 |> group_by(inchi) |> mutate(sid = uuid::UUIDgenerate()) |> ungroup()
subjson <- chem6 |> 
  select(sid, inchi, pubchem_cid, ChemicalName, ChemicalID, CasRN) |> distinct() |> 
  nest(data = -sid) |> 
  mutate(data = map_chr(data, ~ jsonlite::toJSON(as.list(.), auto_unbox = TRUE)))

arrow::write_parquet(subjson, fs::path(stg,"substances.parquet"))

# BUILD PROPERTIES ==============================================================
cgi <- {
  df1 <- ctd$CTD_chem_gene_ixns_parquet |> arrow::open_dataset() |> collect()
  df2 <- df1 |> 
    mutate(ChemicalID = paste("MESH:", ChemicalID, sep="")) |>
    select(ChemicalID, GeneSymbol, GeneID, Organism, OrganismID, GeneForms, InteractionActions) |>
    filter(InteractionActions %in% c("increases^expression", "decreases^expression",
                                     "increases^methylation", "decreases^methylation",
                                     "increases^activity", "decreases^activity")) |>
    distinct() |> 
    mutate(value = "positive")

  # Flip the script for the opposite interactions
  df3_opposite <- df2 |>
    mutate(InteractionActions = case_when(
      InteractionActions == "increases^expression" ~ "decreases^expression",
      InteractionActions == "decreases^expression" ~ "increases^expression",
      InteractionActions == "increases^methylation" ~ "decreases^methylation",
      InteractionActions == "decreases^methylation" ~ "increases^methylation",
      InteractionActions == "increases^activity" ~ "decreases^activity",
      InteractionActions == "decreases^activity" ~ "increases^activity"
    ), value = "negative")

  # Combine the original zest with the new twist
  df_combined <- bind_rows(df2, df3_opposite) |> inner_join(chem6, by="ChemicalID")

  # create pids
  df_final <- df_combined |> 
    group_by(Organism,OrganismID,GeneSymbol,GeneForms,InteractionActions) |> 
    mutate(pid = uuid::UUIDgenerate()) |> ungroup()
  
  df_final |> distinct()
}

# filter out properties with a 
propjson <- cgi |> select(pid, Organism, OrganismID, GeneSymbol, GeneForms, InteractionActions) |> distinct() |> 
  nest(data = -pid) |>
  mutate(data = map_chr(data, ~ jsonlite::toJSON(as.list(.), auto_unbox = T)))

arrow::write_parquet(propjson, fs::path(stg,"properties.parquet"))

# BUILD ACTIVITIES ==============================================================

# create activities
act <- cgi |> select(sid, pid, inchi, value) |> filter(!is.na(inchi)) |> distinct() 
act <- act |> 
  mutate(aid = paste0("ctdbase-", row_number())) |>
  select(aid,sid,pid,inchi,value)

arrow::write_parquet(act, fs::path(stg,"activities.parquet"))
