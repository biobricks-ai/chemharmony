pacman::p_load(biobricks, tidyverse, arrow, uuid, jsonlite)

stg <- fs::dir_create("staging/toxvaldb")
uid <- UUIDgenerate
toJ <- purrr::partial(jsonlite::toJSON, auto_unbox = TRUE)

## pull data
toxvaldb <- biobricks::bbload("toxvaldb")$toxvaldb |> collect()
comptox  <- biobricks::bbload("comptox")$dsstox_identifiers |> collect()

tval <- toxvaldb |> group_by(dtxsid) |> mutate(sid = uid()) |> ungroup()
tval <- tval |> inner_join(comptox, by = "dtxsid") |> filter(!is.na(inchi))

# props <- c("toxval_type", "toxval_type_original", "toxval_subtype",
#   "toxval_subtype_original", "toxval_type_category",
#   "toxval_type_supercategory", "risk_assessment_class",
#   "study_type", "study_duration_class", "study_duration_value",
#   "species_common", "species_supercategory",
#   "habitat", "human_eco", "strain", "sex", "generation", "lifestage",
#   "exposure_route", "exposure_method", "exposure_form", "media",
#   "media_original", "critical_effect", "critical_effect_original",
#   "toxval_units")

# print_table <- function(df, n = 10) {
#   seq(1,ncol(df),10) |> walk(\(i){
#     print(df[,i:min(ncol(df),(i+9))] |> sample_n(10), n=10)
#   })
# }

props <- c("risk_assessment_class", "species_supercategory",
  "exposure_route", "toxval_type_category",
  "toxval_units")

tval <- tval |> filter(toxval_numeric_qualifier == "=", qa_status==1)
tval <- tval |> select(sid, dtxsid, inchi, all_of(props), value=toxval_numeric)
tval <- tval |> group_by(!!!syms(props)) |> mutate(pid = uid()) |> ungroup()
tval <- tval |> group_by(sid,pid) |> mutate(value=median(value)) |> ungroup() |> distinct()
tval <- tval |> group_by(pid) |> filter(n() > 500) |> ungroup()
tval <- tval |> group_by(pid) |> mutate(medvalue = median(value)) |> ungroup()
tval <- tval |> group_by(pid) |> mutate(value = ifelse(value<medvalue,"negative","positive")) |> ungroup()

# Export Chemicals ====================================================
substances <- tval |> 
  select(sid, dtxsid, inchi) |> distinct() |>
  nest(data = -sid) |> mutate(data = map_chr(data, ~ toJ(as.list(.))))

arrow::write_parquet(substances, fs::path(stg, "substances.parquet"))

# Export Properties ====================================================
properties <- tval |> select(pid, all_of(props)) |> distinct() |>
  nest(data = -pid) |> mutate(data = map_chr(data, ~ toJ(as.list(.))))

arrow::write_parquet(properties, fs::path(stg,"properties.parquet"))

# Export Activities ====================================================
acts <- tval |> mutate(aid = paste0("toxvaldb-",row_number()))
acts <- acts |> select(aid, sid, pid, inchi, value)

arrow::write_parquet(acts, fs::path(stg,"activities.parquet"))