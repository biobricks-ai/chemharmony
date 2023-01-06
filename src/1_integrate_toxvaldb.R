# STORE `cache/toxvaldb` with `substances.parquet`, `properties.parquet`, and `activities.parquet`
pacman::p_load(biobricks, tidyverse, arrow, uuid, jsonlite)

# biobricks::brick_install("toxvaldb")
# biobricks::brick_pull("toxvaldb")

toxvaldb  <- biobricks::brick_load("toxvaldb")$toxvaldb.parquet |> collect()

invisible(safely(fs::dir_delete)("cache/toxvaldb"))
outputdir <- fs::dir_create("cache/toxvaldb", recurse = TRUE)
writeds <- function(df, name) {
  arrow::write_dataset(df, fs::path(outputdir, name))
}

# Export Chemicals ====================================================
tval <- toxvaldb |> group_by(dtxsid) |> mutate(sid = UUIDgenerate()) |>
  ungroup()

substances <- tval |> select(sid, dtxsid, casrn, name) |> distinct() |>
  nest(data = -sid) |> mutate(data =
  map_chr(data, ~ jsonlite::toJSON(as.list(.), auto_unbox = TRUE)))

# should be sid, data
writeds(substances, "substances.parquet")

# Export Properties ====================================================
pcols <- c("toxval_type", "toxval_type_original", "toxval_subtype",
  "toxval_subtype_original", "toxval_type_category",
  "toxval_type_supercategory", "risk_assessment_class",
  "study_type", "study_duration_class", "study_duration_value",
  "species_common", "species_supercategory",
  "habitat", "human_eco", "strain", "sex", "generation", "lifestage",
  "exposure_route", "exposure_method", "exposure_form", "media",
  "media_original", "critical_effect", "critical_effect_original")

tval2 <- tval |> group_by(!!!syms(pcols)) |>
  mutate(pid = UUIDgenerate()) |> ungroup()

properties <- tval2 |> select(pid, !!!syms(pcols)) |> distinct() |>
  nest(data = -pid) |> mutate(data =
  map_chr(data, ~ jsonlite::toJSON(as.list(.), auto_unbox = TRUE)))

writeds(properties, "properties.parquet")

# Export Activities ====================================================
activities <- tval2 |>
  mutate(source_id = row_number()) |>
  mutate(source_id = paste0("toxvaldb-toxvaldb.parquet", source_id)) |>
  select(source_id, sid, pid, qualifier = toxval_numeric_qualifier,
    units = toxval_units, value = toxval_numeric)

writeds(activities, "activities.parquet")

