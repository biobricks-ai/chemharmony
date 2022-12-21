# chemharmony
Chemharmony harmonizes some simple chemical properties.

It reduces databases into three tables:
1. substances - sid (a uuid), data (a description of the substance)
2. properties - pid (a uuid), data (a description of the property)
3. activities - aid (biobricks-refid), sid, pid, qualifier, units, value

```mermaid
classDiagram

    class Substances{
        - UUID sid
        - JSON data
    }

    class Properties{
        - UUID pid
        - JSON data
    }

    class Activities{
        - UUID aid 
        - UUID sid
        - UUID pid
        + String qualifier
        + String units
        + String value
    }

    Activities --> Substances
    Activities --> Properties

```

## TODO
1. [ ] toxvaldb
2. [ ] chembl
3. [ ] tox21

## FAQ
### This schema seems limited. What about capturing metabolism data? Or dose response data? 
This schema is focused on building QSAR models, but a more flexible graph based schema might be better. Ultimately, capturing data that relates compounds with each other or with other entities through reactions, metabolism, etc. could be captured by adding more tables. Graph schemas can be isomorphic with that approach.

2. 