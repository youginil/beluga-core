# beluga-core

## Beluga Parsing

| Bytes             | Description                                         |
| ----------------- | --------------------------------------------------- |
| 2                 | `spec` the beluga file format version, current is 1 |
| 4                 | `metadata_length`                                   |
| `metadata_length` | `Metadata` JSON string                              |
| -24 + 8           | entry/resource root node offset                     |
| -16 + 4           | entry/resource root node size                       |
| -12 + 8           | token root node offset                              |
| -4 + 4            | token root node size                                |

### Metadata

| Name        | Type   | Description        |
| ----------- | ------ | ------------------ |
| version     | string | dictionary version |
| entry_num   | u64    | entry number       |
| author      | string | author name        |
| email       | string | email              |
| create_time | string | create time        |
| comment     | string | other information  |

### Parsing Node

> Node is compressed by Deflate algorithm

| Bytes | Description                  |
| ----- | ---------------------------- |
| 1     | `is_leaf_node`               |
| 4     | `entry_num` loop for entries |

### Parsing Entry/Resource

| Bytes               | Description                                |
| ------------------- | ------------------------------------------ |
| 4                   | `key_length`                               |
| `key_length`        | `key` is utf8 string                       |
| 4 `is_leaf == true` | `value_length`                             |
| `value_length`      | string for entry, binary data for resource |

## Raw

| Name   | Type    |
| ------ | ------- |
| id     | INTEGER |
| name   | TEXT    |
| text   | TEXT    |
| binary | BLOB    |

## References

#### Mdict

- https://github.com/csarron/mdict-analysis
- https://github.com/zhansliu/writemdict/blob/master/fileformat.md
- https://github.com/fengdh/mdict-js
- https://github.com/ilius/pyglossary

#### Fulltext Index

- https://github.com/stanfordnlp/CoreNLP
- https://github.com/hankcs/HanLP
- https://github.com/nltk/nltk
- https://github.com/RaRe-Technologies/gensim
- https://github.com/explosion/spaCy
- https://github.com/keras-team/keras
- https://github.com/thunlp/THULAC
- https://github.com/yanyiwu/cppjieba
- @ https://github.com/fxsjy/jieba
- https://github.com/HIT-SCIR/ltp
- https://github.com/NLPchina/ansj_seg

#### Dictionary Online

- https://www.thesaurus.com/
- https://www.dictionary.com/
- https://dictionary.cambridge.org/
- https://www.collinsdictionary.com/
- https://www.macmillandictionary.com/
- https://www.ldoceonline.com/
- https://www.oxfordlearnersdictionaries.com/

## TODO

- Checksum
- Remove special chars and set @@@LINK=
- HTML validation, fix, prettify. `tidy-html5`, `cheerio`
- Convert resource id to `<a href="audio://a/b/c.mp3">`, `<img data-src="a/b/c.jpg">`
- Capture. `puppeteer`
- Performance monitor
- Tire tree
- "assist in" Merge(Result<assist>, Result<in>)
