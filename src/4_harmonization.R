pacman::p_load(biobricks, tidyverse, arrow, uuid, jsonlite, kit, glue)

invisible(safely(fs::dir_delete)("brick/harmonized"))
outputdir <- fs::dir_create("brick/harmonized", recurse = TRUE)
writeds <- function(df, name) {
  arrow::write_dataset(df, fs::path(outputdir, name))
}

toxvaldb <- open_dataset("./brick/toxvaldb/substances.parquet") |>
  collect()
## Row 2912 in ToxValSB contains NULL values, not a substance
toxvaldb <- toxvaldb[-2912, ]
toxvaldb <- toxvaldb %>% rowwise() %>%
  do(data.frame(sid = .$sid, fromJSON(as.character(.$data))))
colnames(toxvaldb) <- c("uuid", "dtxs_id", "casrn", "name")

chembl <- open_dataset("./brick/chembl/substances.parquet") |>
  collect()
chembl <- data.frame(lapply(chembl, function(x) {
  gsub("null", "\"NA\"", x)
}))
chembl <- chembl %>% rowwise() %>%
  do(data.frame(sid = .$sid, fromJSON(as.character(.$data))))
colnames(chembl) <- c("uuid", "molregno", "smiles",  "inchi")

tox21 <- open_dataset("./brick/tox21/substances.parquet") |>
  collect()
tox21 <- data.frame(lapply(tox21, function(x) {
  gsub("null", "\"NA\"", x)
}))
tox21 <- do.call(rbind, Map(function(sid, data) {
  data.frame(sid, fromJSON(as.character(data)))
}, tox21$sid, tox21$data))
rownames(tox21) <- 1:nrow(tox21)
colnames(tox21) <- c("uuid", "name", "tox21_sampleid",  "casrn",
  "pubchem_cid", "smiles", "tox21_id")

harm <- merge(toxvaldb, tox21, by.x = "casrn", by.y = "casrn",
  all = TRUE, incomparables = "-")
colnames(harm) <- c("casrn", "toxvaldb_uuid", "dtxs_id",
  "toxvaldbx_name", "tox21_uuid", "tox21_name", "tox21_sampleid",
  "pubchem_cid", "smiles", "tox21_id")

harm <- merge(harm, chembl, by.x = "smiles",
  by.y = "smiles", all = TRUE, incomparables = NA)

writeds(harm, "substances.parquet")

#####################################################################
# Harmonization activities
toxvaldb_act <- open_dataset("./brick/toxvaldb/activities.parquet") |>
  collect()
glue("ToxValDB activities: {nrow(toxvaldb_act)}")
tox21_act <- open_dataset("./brick/tox21/activities.parquet") |>
  collect()
glue("Tox21 activities: {nrow(tox21_act)}")
chembl_act <- open_dataset("./brick/chembl/activities.parquet") |>
  collect()
glue("ChEMBL activities: {nrow(chembl_act)}")

tox21_act_h <- tox21_act %>%
  select(smiles, pid, value) %>%
  filter(!is.na(value))

chembl_act_h <- chembl_act %>%
  select(smiles, pid, value) %>%
  filter(!is.na(value))

harm_act <- rbind(tox21_act_h, chembl_act_h)

writeds(harm_act, "activities.parquet")

