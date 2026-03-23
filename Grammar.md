# Grammar for Domain and Solution Specifications

This document explains the JSON-based grammar used to describe:

1. A **domain schema**, consisting of entities, attributes, and relationships.
2. A **solution**, consisting of collections, indexes, and query plans.

The grammar is:

```
<domain> ::= { "entities": { <entity>* }, "relationships": { <rel>* } }
<entity> ::= "<code>": { "name": <str>, "card": <num>, "attr": { <attr>* } }
<attr>   ::= "<name>": { "card": <num>, "type": <type>, "len": <num>,
                        "pk": <bool>, "fk": <bool> }
<rel>    ::= "<name>": { "from": <entity>, "to": <entity>,
                        "avgCard": <num>, "maxCard": <num>,
                        "fromAtt": <attr>, "toAtt": <attr> }

<solution> ::= { "collections": { <collection>* },
                 "indexes": { <index-def>* },
                 "query_plans": { <qp>* } }
<collection> ::= "<cname>": { <entity-root>: <children>|null }
<children>   ::= { <entity-child>: <children>|null }
<index-def>  ::= "<cname>": { "IX": [ <idx>* ]}
<idx>        ::= [ <ref>+ ]
<ref>        ::= [ "<entity>", "<attr>" ]
<qp>         ::= "<id>": { "freq": <num>, "type": "r"|"w",
                           "aps": [ <ap>+ ] }
<ap>         ::= { "c": "<cname>", "r": <str>|null,
                   "sp": [ <pred>* ], "jp": [ <ref>,<ref>] }
<pred>       ::= [ "<entity>", "<attr>", <selectivity-num> ]
```

---

## 1. Domain Description

The **domain** captures the conceptual schema: entities, their attributes, and relationships.

### 1.1 `<domain>`

A domain is a JSON object with two fields:

- `"entities"`: a map of entity identifiers to entity definitions.
- `"relationships"`: a map of relationship names to relationship definitions.

### 1.2 `<entity>`

Each entity is defined as:

- `"<code>"`: a short identifier.
- `"name"`: human-readable entity name.
- `"card"`: estimated cardinality (number of records).
- `"attr"`: a map of attributes.

Example:

```json
"Cu": {
  "name": "Customer",
  "card": 100000,
  "attr": { ... }
}
```

### 1.3 `<attr>`

Each attribute contains:

- `"card"`: distinct cardinality.
- `"type"`: data type.
- `"len"`: length/size.
- `"pk"`: primary-key flag.
- `"fk"`: foreign-key flag.

Example:

```json
"idC": { "card": 100000, "type": "int", "len": 4, "pk": true, "fk": false }
```

### 1.4 `<rel>`

A relationship connects two entities:

- `"from"`: the source entity.
- `"to"`: the target entity.
- `"avgCard"` / `"maxCard"`: average and maximum relationship cardinality.
- `"fromAtt"` / `"toAtt"`: attributes realizing the relationship (e.g., FK–PK).

Example:
```json
 "requests": { "from": "Cu", "to": "Or", "avgCard": 34.36, "maxCard": 61.0, "fromAtt": "idC", "toAtt": "idC" },
```
   

---

## 2. Solution Description

### 2.1 `<solution>`

A solution contains:

- `"collections"`: how entities are embedded or grouped.
- `"indexes"`: index definitions.
- `"query_plans"`: plans describing how queries access the data.

---

## 2.2 Collections (`<collection>`, `<children>`)

Collections describe document-style nested structures.

Example:

```json
"C_orders": {
  "Or": {
    "It": null
  }
}
```

### Hierarchy rules

- A collection has exactly one root entity.
- Child entities may recursively embed their own children.

---

## 2.3 Indexes (`<index-def>`, `<idx>`, `<ref>`)

Indexes are grouped by collection:

- `"IX"`: list of index definitions.
- `<idx>`: ordered list of `<ref>` pairs.
- `<ref>`: identifies an attribute as `[ "<entity>", "<attr>" ]`.

Example:

```json
"C_orders": {
  "IX": [
    [ ["Or", "idO"]],
    [ ["Or", "date"], ["Or", "idC"] ]
  ]
}
```

---

## 2.4 Query Plans (`<qp>`)

Each query plan includes:

- `"freq"`: estimated query frequency.
- `"type"`: `"r"` (read) or `"w"` (write).
- `"aps"`: list of access paths.

### 2.4.1 `<ap>` — Access Path

An access path contains:

- `"c"`: collection name.
- `"sp"`: selection predicates.
- `"jp"`: a join predicate linking two attribute references. The internal-side reference can include an optional provenance relation as a 3rd field, e.g. `["It","idI","gets"]`, to disambiguate duplicate entities inside the same collection; planners may branch and emit multiple alternative read plans in this case.

### 2.4.2 `<pred>` — Selection Predicate

A predicate is:

```
[ "<entity>", "<attr>", <selectivity-num> ]
```

Where `selectivity-num` is the number of expected instances.

Example:

```json
["Order", "date", 3]  
```

---

## 3. Summary

This grammar provides a unified structure for:

- Expressing a conceptual domain with entities, attributes, and relationships.
- Describing a physical solution including collections and indexes.
- Modeling query behavior with precise access paths, predicates, and join conditions.
