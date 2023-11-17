pacman::p_load(biobricks, tidyverse, arrow, uuid, jsonlite)
attach(list(uuid=uuid::UUIDgenerate, gb=group_by))

out <- fs::dir_create("staging/toxcast", recurse = TRUE)
writeds <- \(df, name) { arrow::write_dataset(df, fs::path(out, name)) }

toxraw <- biobricks::bbassets("toxcast")$invitrodb_parquet |> arrow::open_dataset()
tox <- toxraw |> collect() |> rename(dtxsid = dsstox_substance_id)
tox <- tox |> gb(dtxsid) |> mutate(sid = uuid()) |> ungroup() 
tox <- tox |> gb(aeid) |> mutate(pid = uuid()) |> ungroup()

comptox <- biobricks::bbassets("comptox")$dsstox_identifiers_parquet 
comptox <- arrow::open_dataset(comptox) |> collect()
comptox <- comptox |> select(dtxsid, inchi) |> distinct()
tox <- tox |> inner_join(comptox, by = "dtxsid")

# SUBSTANCES =======================
substances <- tox |>
  select(sid, dtxsid, inchi) |>
  distinct() |> nest(data = -sid) |> mutate(data =
  map_chr(data, ~ jsonlite::toJSON(as.list(.), auto_unbox = TRUE)))

writeds(substances, "substances.parquet")

# PROPERTIES =======================
properties <- tox |>
  select(pid, aeid, aenm) |> distinct() |> 
  nest(data = -pid) |> 
  mutate(data = map_chr(data, ~ toJSON(as.list(.), auto_unbox = TRUE)))

writeds(properties, "properties.parquet")

# ACTIVITIES =======================
activities <- tox |>
  select(sid, pid, inchi, value=hitc) |> distinct() |>
  filter(value != -1) |> # see https://github.com/biobricks-ai/toxcast/blob/main/.bb/brick.yaml
  group_by(sid,pid,inchi) |> summarize(value = round(median(value))) |> ungroup() |>
  mutate(value = ifelse(value == 1,"positive","negative"))

activities <- activities |>
  mutate(aid = paste0("toxcast-", row_number())) |>
  select(aid, sid, pid, inchi, value)

# there should be at least 100 examples of each property + value
# each property should have at least two values
activities <- activities |> group_by(pid,value) |> filter(n() > 100) |> ungroup() 
activities <- activities |> group_by(pid) |> filter(n_distinct(value) > 1) |> ungroup()

writeds(activities, "activities.parquet")