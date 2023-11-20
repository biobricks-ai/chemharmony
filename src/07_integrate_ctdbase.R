pacman::p_load(biobricks, tidyverse, arrow, uuid, jsonlite, kit, glue, httr, memoise)

ctd <- biobricks::bbassets("ctdbase")
stg <- fs::dir_create("staging/ctdbase")

# Export Chemicals ============================================================
chem <- ctd$CTD_chemicals_parquet |> arrow::open_dataset() |> collect()
chem <- chem |> filter(!is.na(CasRN)) |> collect()
nrow(chem)

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
cids <- map(chem$CasRN, get_cids_from_cas, .progress = TRUE)

pcc <- bbassets("pubchem")$compound_sdf_parquet |> arrow::open_dataset()
if(interactive()){ pcc <- pcc |> head(1e7) |> collect() |> tibble()} # for testing
pcc <- pcc |> filter(property=="PUBCHEM_IUPAC_INCHI")
pcc <- pcc |> select(pubchem_cid=id, inchi=value) |> collect()

# Remove chemicals with multiple pubchem cids and join with pubchem
chem$pubchem_cid <- cids
chem <- chem |> filter(map(pubchem_cid,length) == 1) |> mutate(pubchem_cid = unlist(pubchem_cid))
chem <- chem |> left_join(pcc, by="pubchem_cid") |> collect()
chem <- chem |> group_by(pubchem_cid) |> mutate(sid = uuid::UUIDgenerate()) |> ungroup()

subjson <- chem |> 
  select(sid, inchi, pubchem_cid, ChemicalName, ChemicalID, CasRN) |> distinct() |> 
  nest(data = -sid) |> 
  mutate(data = map_chr(data, ~ jsonlite::toJSON(as.list(.), auto_unbox = TRUE)))

arrow::write_parquet(subjson, fs::path(stg,"substances.parquet"))

# BUILD PROPERTIES ==============================================================
rawcgi <- {
  df1 <- ctd$CTD_chem_gene_ixns_parquet |> arrow::open_dataset() |> collect()
  df2 <- df1 |> 
    select(ChemicalID,GeneSymbol,GeneID,Organism,OrganismID,GeneForms,InteractionActions) |>
    filter(InteractionActions %in% c("increases^expression","decreases^expression")) |>
    distinct()

  df3 <- df2 |> mutate(ChemicalID = paste("MESH:",ChemicalID,sep=""))
  df4 <- df3 |> inner_join(chem, by="ChemicalID") |> collect()
  df4
}

# build pids and values
cgi <- {

  # build pids
  df1 <- rawcgi |> 
    mutate(action = "modifies expression in the x direction") |>
    group_by(Organism,OrganismID,GeneSymbol,GeneForms, action) |> 
    mutate(pid = uuid::UUIDgenerate()) |> ungroup()
  
  # build values with positive = increases^expression and negative = decreases^expression
  df2 <- df1 |> mutate(value = ifelse(InteractionActions == "increases^expression", "positive", "negative")) 
  df2
}

# filter out properties with a 
propjson <- cgi |> select(pid, Organism, OrganismID, GeneSymbol, GeneForms, action) |> distinct() |> 
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
