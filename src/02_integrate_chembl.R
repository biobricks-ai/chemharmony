pacman::p_load(biobricks, tidyverse, arrow, uuid, jsonlite)

# useful docs at https://chembl.gitbook.io/chembl-interface-documentation/frequently-asked-questions/chembl-data-questions#what-is-the-assay-type
# TODO we need to update the chembl on biobricks
chembl  <- biobricks::bbassets("chembl") |> map(arrow::open_dataset)

# delete staging/chembl if it exists and then create it again.
if(fs::dir_exists("staging/chembl")){ fs::dir_delete("staging/chembl") }
out <- fs::dir_create("staging/chembl", recurse = TRUE)

uuid <- uuid::UUIDgenerate
toJ <- purrr::partial(jsonlite::toJSON, auto_unbox = TRUE)

# which tables have 'molregno'
# TODO looks like we're leaving a lot of information out. 
# Some of these tables look good, but we need the new chembl on biobricks before spending time iterating here.
# tbls <- Filter(function(x) {
#   hasmolregno <- any(grepl("molregno", names(x)))
#   isbig <- nrow(x) > 10000
#   hasmolregno & isbig
# }, chembl)

# make substance id sid
cmp <- chembl$compound_structures |> collect() |> 
  group_by(molregno) |> mutate(sid = uuid()) |> ungroup() |>
  select(sid, molregno, inchi=standard_inchi)

# make activities
# TODO this is leaving a lot of information about activites out. See the schema https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/latest/chembl_33_schema.png
ass <- chembl$assays |> collect() |> select(-doc_id, -src_id) |> 
  mutate(assay_type = case_when(
    assay_type == "B" ~ "Binding",
    assay_type == "F" ~ "Functional (i.e %cell death or rat weight)", 
    assay_type == "A" ~ "Absorption Distribution Metabolism Excretion",
    assay_type == "T" ~ "Toxicity (T) - Data measuring toxicity of a compound, e.g., cytotoxicity.",
    assay_type == "P" ~ "Physicochemical (P) - Assays measuring physicochemical properties of the compounds in the absence of biological material e.g., chemical stability, solubility.",
    assay_type == "U" ~ "Unclassified"))

act0 <- chembl$activities |> collect()
act1 <- act0 |> inner_join(cmp, by="molregno") |> inner_join(ass,by="assay_id")
act2 <- act1 |> 
  mutate(activity_comment = tolower(activity_comment)) |> 
  mutate(value = case_when(
    activity_comment == "inactive" ~ "negative",
    activity_comment == "active" ~ "positive",
    activity_comment == "not active" ~ "negative",
    activity_comment == "non-toxic" ~ "negative",
    activity_comment == "toxic" ~ "positive",
    activity_comment == "antagonist" ~ "positive",
    TRUE ~ "none"
  )) |> 
  filter(value != "none") 

# make property id 'pid'
propcols = c('assay_id', 'standard_type', 'bao_endpoint', 'uo_units', 'qudt_units', 'type')
propcols <- colnames(ass) |> c(propcols) |> unique()
act3 <- act2 |> group_by(!!!syms(propcols)) |> mutate(pid = uuid()) |> ungroup()

# remove discordants & properties w/ less than 100 distinct substances or less than 50 neg/pos
act4 <- act3 |> group_by(pid,sid) |> filter(n_distinct(value) == 1) |> ungroup()
act5 <- act4 |> group_by(pid) |> filter(n_distinct(sid)>100) |> ungroup()
act6 <- act5 |> group_by(pid) |> filter(sum(value == "positive")>50, sum(value=="negative")>50) |> ungroup()

n_distinct(act6$pid) # 561 properties

# Export Chemicals =====================================================
substances <- act6 |> select(sid, molregno, inchi) |> distinct() |> 
  nest(data = -sid) |> mutate(data = map_chr(data, ~ toJ(as.list(.)))) |>
  select(sid, data)

arrow::write_dataset(substances, fs::path(out,"substances.parquet"))

# Export Properties ===================================================
properties <- act4 |> select(pid,all_of(propcols)) |> distinct() 
properties <- properties |> 
  nest(data = -pid) |> mutate(data = map_chr(data, ~ toJ(as.list(.)))) |>
  select(pid, data)

arrow::write_dataset(properties, fs::path(out,"properties.parquet"))

# Export Activities ============================================================
activities <- act6 |>
  mutate(aid = paste0("chembl-", row_number())) |>
  select(aid, sid, pid, inchi, value)

arrow::write_dataset(activities, fs::path(out,"activities.parquet"))
