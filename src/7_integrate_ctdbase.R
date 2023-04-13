pacman::p_load(biobricks, tidyverse, arrow, uuid, jsonlite, kit, glue, httr)

ctd <- biobricks::bbload("ctdbase")

# chemicals
chem <- ctd$CTD_chemicals |> collect()
chem <- chem |> filter(!is.na(CasRN)) |> collect()
nrow(chem)

# look up chemicals by their mesh id on pubchem
get_cids_from_cas <- function(cas_number) {
  base_url <- "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/"
  url <- paste0(base_url, cas_number, "/cids/json")
  response <- GET(url)
  httr::set_config(httr::config(http_version = 0))
  
  if (http_status(response)$category != "Success") {
    return(NA)
  }
  
  content <- content(response, as = "text", encoding = "UTF-8")
  json_data <- fromJSON(content, simplifyVector = TRUE)
  
  # sleep 300ms
  Sys.sleep(0.3)
  return(json_data$IdentifierList$CID)
}
get_cids_from_cas <- purrr::possibly(get_cids_from_cas, otherwise = NA_integer_)
cids <- map(chem$CasRN, get_cids_from_cas, .progress = TRUE)
saveRDS(cids, "cids.rds")

# chem-gene-ixn
cgi <- ctd$CTD_chem_gene_ixns
cgi <- cgi |> select(ChemicalID,GeneSymbol,InteractionActions)
cgi <- cgi |> collect()

# separate interactionActions by | and ^
cgi <- cgi |> separate_rows(InteractionActions,sep="\\|")
cgi <- cgi |> mutate(ia = strsplit(InteractionActions,"\\^"))
cgi <- cgi |> mutate(value = map_chr(ia,~paste(.x[[1]])))
cgi <- cgi |> mutate(ixn = map_chr(ia,~paste(.x[[2]])))
cgi <- cgi |> mutate(property = paste(GeneSymbol,ixn,sep="_"))
cgi <- cgi |> mutate(value = act)
cgi <- cgi |> select(ChemicalID, property, value)

# Export Chemicals ============================================================
subjson <- chem |> select(sid, inchi, casrn=CasRN, preferredName) |> distinct() |> nest(data = -sid) |> 
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