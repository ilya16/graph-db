# Specifications

## Stores

* `Node` - 13 bytes

| `in_use` | `label_id` | `first_rel_id` | `first_prop_id` |
|:------:|:---------:|:----------:|:----------:|
| 1 byte | 4 bytes   | 4 bytes    |  4 bytes   |

* `Relationship` - 33 bytes

|  `in_use` |  `start_node` | `end_node` | `label_id` | `start_prev_id` | `start_next_id` | `end_prev_id` | `end_next_id` | `first_prop_id` |
|:------:|:-------------:|:------------:|:-----------:|:-----------------:|:-------------------:|:-----------------:|:------------------:|:-------------:|
| 1 byte |     4 bytes    |    4 bytes   |    4 bytes  |       4 bytes     | 4 bytes             | 4 bytes           | 4 bytes            | 4 bytes       |

* `Label` - 5 bytes

|  `in_use` |   `dynamic_id` |
|:------:|:---------------:|
| 1 byte |      4 bytes    |

* `Property` - 13 bytes

|  `in_use` |  `key_id`  | `value_id` | `next_prop_id` |
|:------:|:------:|:-----------:|:--------------:|
| 1 byte | 4 bytes | 4 bytes     |    4 bytes     |

* `Dynamic Data` - 32 bytes

|  `real data length` | `data` | `next_chunk_id` |
|:------:|:------:|:-----------:|
| 1 byte | 27 bytes |   4 bytes   |

## Types

* `Int` - 4 bytes
* `Bool` - 1 byte
* `Str` - depends on the length

## Graph Engine API

Following graph operations are supported:

* Creation of graph
* Creation of nodes, relationships with label and properties
* Match (selection) of nodes, relationships, labels and all graph objects together
* Deletion of nodes and relationships
* Addition of new properties to nodes adn relationships

## Query Language

Graph Database *not4oundQL* supports the following queries:

```
create graph: label
create node: label
create node: label key:value
create node: label key:value key:value key:value
create relationship: label from label1 to label2
create relationship: label from label1 to label2 key:value
create relationship: label from id:0 to id:1 key:value
match node: label
match node: id:0
match relationship: label
match relationship: id:1
match node: key=value
match node: key<value
match relationship: key>=value
match graph: label
delete node: id:0
delete relationship: id:0
```

