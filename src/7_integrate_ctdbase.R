pacman::p_load(biobricks, tidyverse, arrow, uuid, jsonlite, kit, glue)

ctd <- biobricks::bbload("ctdbase")

# chemicals
chem <- ctd$CTD_chemicals |> collect()
chem <- chem |> filter(!is.na(CasRN)) |> collect()

pcc <- biobricks::bbload("pubchem")$compound_sdf
pcinchi <- pcc |> filter(property=="PUBCHEM_IUPAC_INCHI") |> select(id,inchi = value)
pccasrn <- pcc |> filter(property=="PUBCHEM_IUPAC_CAS_NAME") |> select(id,casrn = value)
pccasrn <- pccasrn |> filter(casrn %in% chem$CasRN)
pcc <- pccasrn |> inner_join(pcinchi, by="id") |> collect()

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