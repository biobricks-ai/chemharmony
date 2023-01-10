pacman::p_load(biobricks, tidyverse, arrow, uuid, jsonlite, kit)

invisible(safely(fs::dir_delete)("cache/harmonized"))
outputdir <- fs::dir_create("cache/harmonized", recurse = TRUE)
writeds <- function(df, name) {
  arrow::write_dataset(df, fs::path(outputdir, name))
}
has_nulls <- function(mylist) {
  n <- length(mylist)
  for (i in 1:n)
    if (is.null(mylist[[i]]))
      return(TRUE)
  return(FALSE)
}
fix_nulls <- function(mylist) {
  n <- length(mylist)
  for (i in 1:n)
    if (is.null(mylist[[i]]))
      mylist[[i]] <- NA
  return(mylist)
}
valid_casrn <- function(mystr) {
  spl <- strsplit(mystr, "-")
  if (length(spl[[1]]) == 3) {
    if ((grepl("^[0-9]+$", spl[[1]][1])) &&
        (grepl("^[0-9]+$", spl[[1]][2])) &&
        (grepl("^[0-9]+$", spl[[1]][3])))
      return(TRUE)
  }
  return(FALSE)
}

toxvaldb <- open_dataset("./cache/toxvaldb/substances.parquet") |>
  collect()
toxvaldb_act <- open_dataset("./cache/toxvaldb/activities.parquet") |>
  collect()
# nrow(toxvaldb)
# head(toxvaldb)
# toxvaldb[1, 2]
# jsonlite::fromJSON(toxvaldb[[1, 2]])
# jsonlite::fromJSON(toxvaldb[[1, 2]])$casrn
# for (i in seq(from = 1, to = 10)) {
#   print(jsonlite::fromJSON(toxvaldb[[i, 2]])$casrn)
# }

chembl <- open_dataset("./cache/chembl/substances.parquet") |>
  collect()
chembl_act <- open_dataset("./cache/chembl/activities.parquet") |>
  collect()
# nrow(chembl)
# head(chembl)
# jsonlite::fromJSON(chembl[[1, 2]])

tox21 <- open_dataset("./cache/tox21/substances.parquet") |>
  collect()
tox21_act <- open_dataset("./cache/tox21/activities.parquet") |>
  collect()
# nrow(tox21)
# head(tox21)
# for (i in seq(from = 1, to = 10)) {
#   print(jsonlite::fromJSON(tox21[[i, 2]])$CAS)
# }

## Row 2912 in ToxValSB contains NULL values, not a substance
toxvaldb <- toxvaldb[-2912, ]

##############################################################
ntoxvaldb <- nrow(toxvaldb)
toxvaldbdf <- data.frame(matrix(ncol = 4, nrow = ntoxvaldb))
colnames(toxvaldbdf) <- c("toxvaldb_uuid", "dtxs_id", "casrn", "name")
toxvaldbdf[, "toxvaldb_uuid"] <- toxvaldb[, "sid"]
for (i in 1:ntoxvaldb) {
  jsondf <- as.data.frame(jsonlite::fromJSON(toxvaldb[[i, 2]]))
  toxvaldbdf[i, "dtxs_id"] <- jsondf["dtxsid"]
  toxvaldbdf[i, "casrn"] <- jsondf["casrn"]
  toxvaldbdf[i, "name"] <- jsondf["name"]
}

##############################################################
ntox21 <- nrow(tox21)
tox21df <- data.frame(matrix(ncol = 7, nrow = ntox21))
colnames(tox21df) <- c("tox21_uuid", "tox21_id", "casrn",
  "pubchem_cid", "tox21_sampleid", "name", "smiles")
tox21df[, "tox21_uuid"] <- tox21db[, "sid"]
for (i in 1:ntox21) {
  jsonst <- jsonlite::fromJSON(tox21[[i, 2]])
  if (has_nulls(jsonst)) {
    # print(jsonst)
    # tox21nulls <- tox21nulls + 1
    # tox21nullslist <- append(tox21nullslist, i,
    #   after = length(tox21nullslist))
    jsonst <- fix_nulls(jsonst)
  }
  jsondf <- as.data.frame(jsonst)
  tox21df[i, "name"] <- jsondf["SAMPLE_NAME"]
  tox21df[i, "tox21_sampleid"] <- jsondf["SAMPLE_ID"]
  tox21df[i, "casrn"] <- jsondf["CAS"]
  tox21df[i, "pubchem_cid"] <- jsondf["PUBCHEM_CID"]
  tox21df[i, "smiles"] <- jsondf["SMILES"]
  tox21df[i, "tox21_id"] <- jsondf["TOX21_ID"]
}

##############################################################
nchembl <- nrow(chembl)
chembldf <- data.frame(matrix(ncol = 4, nrow = nchembl))
colnames(chembldf) <- c("chembl_uuid", "molregno", "canonical_smiles",
  "standard_inchi")
chembldf[, "chembl_uuid"] <- chembl[, "sid"]
# chembl <- chembl %>% mutate(jsonst =
#    jsonlite::fromJSON(chembl$data))

for (i in 1:nchembl) {
  if (i %% 1000 == 0)
    cat("\r",i)
  jsonst <- jsonlite::fromJSON(chembl[[i, 2]])
  jsondf <- as.data.frame(jsonst)
  chembldf[i, "molregno"] <- jsondf["molregno"]
  chembldf[i, "canonical_smiles"] <- jsondf["canonical_smiles"]
  chembldf[i, "standard_inchi"] <- jsondf["standard_inchi"]
}

ch <- head(chembl, 100000)
ch <- ch %>%
  mutate(data = map(data, ~fromJSON(as.character(.x))))
chembl <- chembl %>%
  mutate(data = map(data, ~fromJSON(as.character(.x)))) %>%
  unnest()

chembl <- chembl %>% rowwise() %>%
  do(data.frame(sid = .$sid, fromJSON(as.character(.$data))))

##############################################################
df <- data.frame(matrix(ncol = 10, nrow = ntoxvaldb + ntox21))
colnames(df) <- c("toxvaldb_uuid", "tox21_uuid", "chembl_uuid",
  "dtxs_id", "tox21_id", "casrn", "pubchem_cid", "tox21_sampleid",
  "name", "smiles")
df[1:ntoxvaldb, "toxvaldb_uuid"] <- toxvaldb[, "sid"]
for (i in 1:ntoxvaldb) {
  jsondf <- as.data.frame(jsonlite::fromJSON(toxvaldb[[i, 2]]))
  df[i, "dtxs_id"] <- jsondf["dtxsid"]
  df[i, "casrn"] <- jsondf["casrn"]
  df[i, "name"] <- jsondf["name"]
}

# tox21nulls <- 0
# tox21nullslist <- list()
for (i in 1:ntox21) {
  df[ntoxvaldb + i, "tox21_uuid"] <- tox21[i, "sid"]
  jsonst <- jsonlite::fromJSON(tox21[[i, 2]])
  if (has_nulls(jsonst)) {
    # print(jsonst)
    # tox21nulls <- tox21nulls + 1
    # tox21nullslist <- append(tox21nullslist, i,
    #   after = length(tox21nullslist))
    jsonst <- fix_nulls(jsonst)
  }
  jsondf <- as.data.frame(jsonst)
  df[ntoxvaldb + i, "name"] <- jsondf["SAMPLE_NAME"]
  df[ntoxvaldb + i, "tox21_sampleid"] <- jsondf["SAMPLE_ID"]
  df[ntoxvaldb + i, "casrn"] <- jsondf["CAS"]
  df[ntoxvaldb + i, "pubchem_cid"] <- jsondf["PUBCHEM_CID"]
  df[ntoxvaldb + i, "smiles"] <- jsondf["SMILES"]
  df[ntoxvaldb + i, "tox21_id"] <- jsondf["TOX21_ID"]
}

nrow(df)

# df1 <- df[which(!grepl("NOCAS", df$casrn, fixed = TRUE)), ]
# nrow(df1)
# df2 <- df[which(sapply(strsplit(df$casrn, "-"), length) == 3), ]
# nrow(df2)
# df3 <- df[which(!grepl("NOCAS", df$casrn, fixed = TRUE) &
#   (sapply(strsplit(df$casrn, "-"), length) == 3)), ]
# nrow(df3)
# df4 <- df3[which(nchar(df3$casrn) > 12), ]
# nrow(df4)
# head(df4, 10)

df5 <- df[which(sapply(df$casrn, valid_casrn)), ]
nrow(df5)

df <- df %>% mutate(valid_casrn =
  sapply(df$casrn, valid_casrn))


dfm <- merge(toxvaldbdf, tox21df, by.x = "casrn", by.y = "casrn",
  all = TRUE, incomparables = "-")

dfall <- merge(dfm, chembl, by.x = "smiles", by.y = "smiles",
  all = TRUE, incomparables = NA)

# tox21_df <- as.data.frame(jsonlite::fromJSON(tox21[[1, 2]]))
# tox21_df

# matches <- 0
# for (i in 1:nrow(toxvaldb)) {
#   toxvaldb_cas <- jsonlite::fromJSON(toxvaldb[[i, 2]])$casrn
#   for (j in seq(from = 1, to = nrow(tox21))) {
#     tox21_cas <- jsonlite::fromJSON(tox21[[j, 2]])$CAS
#     if (toxvaldb_cas == tox21_cas) {
#       cat("\ri =", i, ", j =", j, ", matches =", matches)
#       matches <- matches + 1
#       # print(matches)
#       # print(toxvaldb_cas)
#       # print(tox21_cas)
#     }
#   }
# }
# # print(matches)

# toxvaldb_cas_unique <- unique(toxvaldb[c("casrn")])

# for (i in 1:10) {Sys.sleep(1); cat("\r",i)}

# matches <- 0
# for (i in seq(from = 1, to = nrow(toxvaldb))) {
#   toxvaldb_cas <- jsonlite::fromJSON(toxvaldb[[i, 2]])$casrn
#   for (j in seq(from = 1, to = 10)) {
#     tox21_cas <- jsonlite::fromJSON(tox21[[j, 2]])$CAS
#     if (toxvaldb_cas == tox21_cas) {
#       matches <- matches + 1
#       print(matches)
#       print(toxvaldb_cas)
#       print(tox21_cas)
#     }
#   }
# }
# print(matches)


# harmonized <- rbind(toxvaldb, chembl, tox21)

# writeds(harmonized, "substances.parquet")
