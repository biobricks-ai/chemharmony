pacman::p_load(biobricks, tidyverse, arrow, uuid, jsonlite)

chembl  <- biobricks::bbload("chembl")
out <- fs::dir_create("staging/chembl", recurse = TRUE)
uuid <- uuid::UUIDgenerate
toJ <- purrr::partial(jsonlite::toJSON, auto_unbox = TRUE)

# make substance id sid
cmp <- chembl$compound_structures |> collect() |> 
  group_by(molregno) |> mutate(sid = uuid()) |> ungroup() |>
  select(sid, molregno, inchi=standard_inchi)

# make activities
act <- chembl$activities |> collect() |> inner_join(cmp, by="molregno")
act <- act |> filter(!is.na(standard_value), standard_relation=="=", standard_flag==1)
act <- act |> select(sid, molregno, inchi, 
  assay_id, standard_type, type, bao_endpoint, units=standard_units, 
  value = standard_value)

# make property id 'pid'
props <- c("assay_id","standard_type","type","bao_endpoint","units")
act <- act |> group_by(!!!syms(props)) |> mutate(pid = uuid()) |> ungroup()

# set repeated measures to the median value and then to property quartile
act <- act |> group_by(across(c(-value))) |> summarize(value=median(value)) |> ungroup() 
act <- act |> group_by(pid) |> mutate(value = sprintf("quartile_%s",ntile(value,4))) |> ungroup() 

# remove properties with less than 1000 values
act <- act |> group_by(pid) |> filter(n()>1000) |> ungroup()

# Export Chemicals =====================================================
substances <- act |> select(sid, molregno, inchi) |> distinct() |> 
  nest(data = -sid) |> mutate(data = map_chr(data, ~ toJ(as.list(.))))

arrow::write_dataset(substances, fs::path(out,"substances.parquet"))

# Export Properties ===================================================
properties <- act |> select(pid,all_of(props)) |> distinct() |>
  nest(data = -pid) |> mutate(data = map_chr(data, ~ toJ(as.list(.))))

arrow::write_dataset(properties, fs::path(out,"properties.parquet"))

# Export Activities ============================================================
activities <- act |> 
  mutate(aid = paste0("chembl-", row_number())) |>
  select(aid, sid, pid, inchi, value)

arrow::write_dataset(activities, fs::path(out,"activities.parquet"))
