# chemharmony - a large scale chemical activity store
Chemharmony harmonizes some simple chemical properties.

It reduces databases into four tables:

```mermaid
erDiagram

    SUBSTANCES {
        UUID sid
        JSON data
    }

    PROPERTIES {
        UUID pid
        JSON data
    }

    PROPERTY_CATEGORIES {
        UUID pid
        String category
        String reason
        Double strength
    }

    ACTIVITIES {
        UUID aid
        UUID sid
        UUID pid
        String inchi
        String value
        Double numvalue
    }

    SUBSTANCES ||--o{ ACTIVITIES : contains
    PROPERTIES ||--o{ ACTIVITIES : measures
    PROPERTIES ||--o{ PROPERTY_CATEGORIES : categorized_by

```
Each `activities` is a measurement of a `properties` on a `substances` resulting in a `value` or `numvalue`.

Property_categories are the result of a GPT4 classification of the Properties `data` field, each classification assigns a property to one or more `category` with an associated `reason` and `strength`. A strength of 1 is the weakest and 10 the strongest. 

## FAQ
### This schema seems limited. What about capturing metabolism data? Or dose response data? 
This schema is focused on building QSAR models, but a more flexible graph based schema might be better. Ultimately, capturing data that relates compounds with each other or with other entities through reactions, metabolism, etc. could be captured by adding more tables. Graph schemas can be isomorphic with that approach.
