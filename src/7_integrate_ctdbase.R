pacman::p_load(biobricks, tidyverse, arrow, uuid, jsonlite, kit, glue, httr)

ctd <- biobricks::bbload("ctdbase")
stg <- fs::dir_create("staging/ctdbase")

# Export Chemicals ============================================================
chem <- ctd$CTD_chemicals |> collect()
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
get_cids_from_cas <- purrr::possibly(get_cids_from_cas, otherwise = list())
cids <- map(chem$CasRN, get_cids_from_cas, .progress = TRUE)

pcc <- bbload("pubchem")$compound_sdf
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
# chem-gene-ixn
rawcgi <- ctd$CTD_chem_gene_ixns |> collect()
rawcgi <- rawcgi |> 
  select(ChemicalID,GeneSymbol,GeneID,Organism,OrganismID,GeneForms,InteractionActions) |>
  distinct()

# process cgi into chemical, property, value
# separate interactionActions by | and ^
cgi <- rawcgi |> separate_rows(InteractionActions,sep="\\|")
cgi <- cgi |> mutate(ia = strsplit(InteractionActions,"\\^"))
cgi <- cgi |> mutate(value = map_chr(ia,~paste(.x[[1]])))
cgi <- cgi |> mutate(ixn = map_chr(ia,~paste(.x[[2]])))

# build pids
cgi <- cgi |> mutate(property = paste(OrganismID,GeneSymbol,GeneForms,ixn,sep="_"))
cgi <- cgi |> group_by(property) |> mutate(pid = uuid::UUIDgenerate()) |> ungroup()

propjson <- cgi |> select(pid, OrganismID, GeneSymbol, GeneForms, ixn) |> 
  distinct() |> 
  nest(data = -pid) |>
  mutate(data = map_chr(data, ~ jsonlite::toJSON(as.list(.), auto_unbox = T)))
arrow::write_parquet(propjson, fs::path(stg,"properties.parquet"))

# BUILD ACTIVITIES ==============================================================

# create activities
act <- cgi |> select(ChemicalID, pid, value) |> distinct()
act <- act |> filter(value %in% c("decreases","increases"))
act <- act |> mutate(ChemicalID = paste("MESH:",ChemicalID,sep=""))

# remove discordant chemical properties
act <- act |> group_by(ChemicalID, pid) |> filter(n() == 1) |> ungroup()

# join with chem to get sid
schem <- chem |> select(sid, ChemicalID, inchi)
act <- act |> inner_join(schem, by="ChemicalID")
act <- act |> 
  mutate(aid = paste0("ctdbase-", row_number())) |>
  select(aid,sid,pid,inchi,value)

arrow::write_parquet(act, fs::path(stg,"activities.parquet"))
