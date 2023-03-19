pacman::p_load(biobricks, tidyverse, arrow, uuid, jsonlite)
rdkit <- reticulate::import("rdkit")
attach(list(uuid=uuid::UUIDgenerate, gb=group_by))

out <- fs::dir_create("staging/toxcast", recurse = TRUE)
writeds <- \(df, name) { arrow::write_dataset(df, fs::path(out, name)) }

inchi2smiles <- purrr::possibly(function(inchi){
  rdkit$Chem$MolFromInchi(inchi) |> rdkit$Chem$MolToSmiles()
}, otherwise = NA_character_)


tox <- biobricks::bbload("toxcast")$invitrodb |> collect()
tox <- tox |> rename(dtxsid = dsstox_substance_id)
tox <- tox |> gb(dtxsid) |> mutate(sid = uuid()) |> ungroup() 
tox <- tox |> gb(aeid) |> mutate(pid = uuid()) |> ungroup()
tox <- tox |> mutate(smiles = inchi2smiles(inchi))

# SUBSTANCES =======================
comptox <- biobricks::bbload("comptox")[[1]] |> collect()
comptox <- comptox |> select(dtxsid, inchi) |> distinct()

tox <- tox |> inner_join(comptox, by = "dtxsid")

substances <- tox |>
  select(sid, dtxsid, inchi) |>
  distinct() |> nest(data = -sid) |> mutate(data =
  map_chr(data, ~ jsonlite::toJSON(as.list(.), auto_unbox = TRUE)))

writeds(substances, "substances.parquet")

# PROPERTIES =======================
properties <- tox |>
  select(pid, aeid, aenm) |> distinct() |> nest(data = -sid) |> 
  mutate(data = map_chr(data, ~ toJSON(as.list(.), auto_unbox = TRUE)))

writeds(properties, "properties.parquet")

# ACTIVITIES =======================
activities <- tox |>
  select(sid, pid, smiles, inchi, numeric_value=hitc) |>
  filter(numeric_value != -1) |> # see https://github.com/biobricks-ai/toxcast/blob/main/.bb/brick.yaml
  mutate(nominal_value = ifelse(numeric_value == 1,"positive","negative"))

writeds(activities, "activities.parquet")