# chemharmony
Chemharmony harmonizes some simple chemical properties.
## Roadmap

1. build three tables, substances, properties, activities for selected databases
2. substances - sid (a uuid), data (a description of the substance)
3. properties - pid (a uuid), data (a description of the property)
4. activities - aid (biobricks-refid), sid, pid, qualifier, units, value

## databases
1. [ ] toxvaldb
2. [ ] chembl
3. [ ] ???

## FAQ
### This schema seems limited. What about capturing metabolism data? Or dose response data? 
This schema is focused on building QSAR models, but a more flexible graph based schema might be better. Ultimately, capturing data that relates compounds with each other or with other entities through reactions, metabolism, etc. could be captured by adding more tables. Graph schemas can be isomorphic with that approach.

2. 