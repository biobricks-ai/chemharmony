pacman::p_load(biobricks, tidyverse, arrow, uuid, jsonlite, kit,
  reticulate, glue, progress, data.table)

outputdir <- fs::dir_create("cache/harmonized", recurse = TRUE)
writeds <- function(df, name) {
  arrow::write_dataset(df, fs::path(outputdir, name))
}

#####################################################################
# Get substances from harmonized parquet
# substances <-
#   open_dataset("./cache/harmonized/substances.parquet") |>
#   collect()

# substances <- substances %>% mutate(
#   has_smiles = !is.na(substances$smiles))

# subs_with_smiles <- substances[which(!is.na(substances$smiles)), ]


#####################################################################
# Get activities
toxvaldb_act <- open_dataset("./cache/toxvaldb/activities.parquet") |>
  collect()
glue("ToxValDB activities: {nrow(toxvaldb_act)}")
tox21_act <- open_dataset("./cache/tox21/activities.parquet") |>
  collect()
glue("Tox21 activities: {nrow(tox21_act)}")
chembl_act <- open_dataset("./cache/chembl/activities.parquet") |>
  collect()
glue("ChEMBL activities: {nrow(chembl_act)}")

#####################################################################
# Take sid from activities and retrive smiles from substances

tox21_act_h <- tox21_act %>%
  select(smiles, pid, value) %>%
  filter(!is.na(value))

chembl_act_h <- chembl_act %>%
  select(smiles, pid, value) %>%
  filter(!is.na(value))

harm_act <- rbind(tox21_act_h, chembl_act_h)

writeds(harm_act, "activities.parquet")

###############################################################
# Playground

# Attemps to get SMILES from CASRN
# valid_casrn <- function(mystr) {
#   spl <- strsplit(mystr, "-")
#   if (length(spl[[1]]) == 3) {
#     if ((grepl("^[0-9]+$", spl[[1]][1])) &&
#         (grepl("^[0-9]+$", spl[[1]][2])) &&
#         (grepl("^[0-9]+$", spl[[1]][3])))
#       return(TRUE)
#   }
#   return(FALSE)
# }
# get_smiles <- function(mycas) {
#   return(cirpy$resolve(mycas, "smiles"))
# }

# substances <- substances %>% mutate(
#   valid_cas = sapply(substances$casrn, valid_casrn) &
#   is.na(substances$smiles))

# substances <- substances %>%
#   mutate_cond(v_cas, smiles =
#   sapply(substances$casrn, get_smiles))

# use_python("/opt/anaconda3/bin/python")
# rdkit <- import("rdkit")
# cirpy <- import("cirpy")
# get_smiles("108-95-2")
# get_smiles("1310-58-3")
# cirpy$resolve(list(c("108-95-2"), c("9002-84-0"), c("1310-58-3")), "smiles")
# cirpy$resolve("108-95-2", "smiles")

# # Attemps to get SMILES from CASRN using a single file
# subs_cas <- substances[which(substances$v_cas), ]
# write.csv(subs_cas$casrn, "cas.txt", row.names = FALSE, sep = " ")
# write.table(as.list(subs_cas$casrn), "cas.txt", sep = " ")
# capture.output(as.list(subs_cas$casrn), file = "cas.txt")
# lapply(subs_cas$casrn, function(x) {
#   write.table(data.frame(x), "cas.txt", append = TRUE, sep = ",") 
#   })

# # Previous work

# subs_smiles <- substances[which(substances$v_cas), ]
# write.csv(subs_cas$casrn, "cas.txt", row.names = FALSE, sep = " ")
# write.table(as.list(subs_cas$casrn), "cas.txt", sep = " ")
# capture.output(as.list(subs_cas$casrn), file = "cas.txt")
# lapply(subs_cas$casrn, function(x) {
#   write.table(data.frame(x), "cas.txt", append = TRUE, sep = ",") 
#   })

# toxvaldb <- data.frame(lapply(toxvaldb, function(x) {
#   gsub("null", "\"NA\"", x)
# }))
# toxvaldb <- toxvaldb %>% rowwise() %>%
#   do(data.frame(sid = .$sid, fromJSON(as.character(.$data))))
# colnames(toxvaldb) <- c("uuid", "dtxs_id", "casrn", "name")

# chembl <- open_dataset("./cache/chembl/substances.parquet") |>
#   collect()
# chembl <- data.frame(lapply(chembl, function(x) {
#   gsub("null", "\"NA\"", x)
# }))
# chembl <- chembl %>% rowwise() %>%
#   do(data.frame(sid = .$sid, fromJSON(as.character(.$data))))
# colnames(chembl) <- c("uuid", "molregno", "smiles",  "inchi")

# tox21 <- open_dataset("./cache/tox21/substances.parquet") |>
#   collect()
# tox21 <- data.frame(lapply(tox21, function(x) {
#   gsub("null", "\"NA\"", x)
# }))
# tox21 <- do.call(rbind, Map(function(sid, data) {
#   data.frame(sid, fromJSON(as.character(data)))
# }, tox21$sid, tox21$data))
# rownames(tox21) <- 1:nrow(tox21)
# colnames(tox21) <- c("uuid", "name", "tox21_sampleid",  "casrn",
#   "pubchem_cid", "smiles", "tox21_id")

# harm <- merge(toxvaldb, tox21, by.x = "casrn", by.y = "casrn",
#   all = TRUE, incomparables = "-")
# colnames(harm) <- c("casrn", "toxvaldb_uuid", "dtxs_id",
#   "toxvaldbx_name", "tox21_uuid", "tox21_name", "tox21_sampleid",
#   "pubchem_cid", "smiles", "tox21_id")

# harm <- merge(harm, chembl, by.x = "smiles",
#   by.y = "smiles", all = TRUE, incomparables = NA)

# writeds(harm, "substances.parquet")

# # detect
# is_sid <- function(x) return(x == toxvaldb_act$sid[1])
# substances$toxvaldb_uuid |> detect_index(is_sid)


# toxvaldb_act_n <- toxvaldb_act[1:10000, ] %>%
#   group_by(sid) %>%
#   mutate(smiles = substances[
#     which(substances$toxvaldb_uuid == sid)[1], "smiles"]) %>%
#   ungroup()

# tox21_act <- tox21_act %>%
#   group_by(sid) %>%
#   mutate(smiles = substances[
#     which(substances$tox21_uuid == sid)[1], "smiles"]) %>%
#   ungroup()

# chembl_act <- chembl_act %>%
#   group_by(sid) %>%
#   mutate(smiles = substances[
#     which(substances$uuid == sid)[1], "smiles"]) %>%
#   ungroup()