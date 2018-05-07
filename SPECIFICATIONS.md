# Specifications

## Stores

* `Node` - 13 bytes

| `in_use` | `label_id` | `first_rel_id` | `first_prop_id` |
|:------:|:---------:|:----------:|:----------:|
| 1 byte | 4 bytes   | 4 bytes    |  4 byte    |

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

|  `number of bytes taken by data` | `data` | `pointer to id of next_chunk` |
|:------:|:------:|:-----------:|
| 1 byte | 27 bytes |   4 bytes   |

## Types

* `Int` - 4 bytes
* `Bool` - 1 byte
* `Str` - depends on the length