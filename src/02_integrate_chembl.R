pacman::p_load(biobricks, tidyverse, arrow, uuid, jsonlite)

# TODO we need to update the chembl on biobricks
chembl  <- biobricks::bbassets("chembl") |> map(arrow::open_dataset)
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
ass <- chembl$assays |> collect() |> select(-doc_id, -src_id) 
act <- chembl$activities |> collect() |> inner_join(cmp, by="molregno") |> inner_join(ass,by="assay_id")
act <- act |> filter(!is.na(standard_value), standard_relation=="=", standard_flag==1)

# make property id 'pid'
props <- c("assay_id","standard_type","type","bao_endpoint","units")
props <- colnames(ass) |> c(props) |> unique()
act <- act |> group_by(!!!syms(props)) |> mutate(pid = uuid()) |> ungroup()

# remove properties with less than 500 values
act <- act |> group_by(pid) |> filter(n_distinct(sid)>500) |> ungroup()

# set repeated measures to the median value and then to above or below median
act <- act |> select(sid, pid, value = standard_value)
act <- act |> group_by(sid,pid) |> summarize(value=median(value)) |> ungroup() 
act <- act |> group_by(pid) |> mutate(medvalue = median(value)) |> ungroup() 
act <- act |> group_by(pid) |> mutate(value = ifelse(value<medvalue,"negative","positive")) |> ungroup() 

# remove pids with less than 100 of the minority value type
goodpids <- act |> group_by(pid) |> summarize(pos = sum(value == "positive"), neg=sum(value=="negative")) |>
  ungroup() |> mutate(lowcount = ifelse(pos > neg,pos,neg)) |> filter(lowcount > 100) |>
  pull(pid)
act <- act |> filter(pid %in% goodpids)

# Export Chemicals =====================================================
substances <- act |> select(sid, molregno, inchi) |> distinct() |> 
  nest(data = -sid) |> mutate(data = map_chr(data, ~ toJ(as.list(.))))

arrow::write_dataset(substances, fs::path(out,"substances.parquet"))

# Export Properties ===================================================
properties <- act |> select(pid,all_of(props)) |> distinct() 
properties <- properties |> inner_join(chembl$assays |> collect(), by="assay_id")
  nest(data = -pid) |> mutate(data = map_chr(data, ~ toJ(as.list(.))))

arrow::write_dataset(properties, fs::path(out,"properties.parquet"))

# Export Activities ============================================================
activities <- act |> 
  mutate(aid = paste0("chembl-", row_number())) |>
  select(aid, sid, pid, inchi, value)

arrow::write_dataset(activities, fs::path(out,"activities.parquet"))
